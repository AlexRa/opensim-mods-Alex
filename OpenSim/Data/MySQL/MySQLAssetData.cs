/*
 * Copyright (c) Contributors, http://opensimulator.org/
 * See CONTRIBUTORS.TXT for a full list of copyright holders.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the OpenSimulator Project nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE DEVELOPERS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

using System;
using System.Data;
using System.Reflection;
using System.Collections.Generic;
using log4net;
using MySql.Data.MySqlClient;
using OpenMetaverse;
using OpenSim.Framework;

namespace OpenSim.Data.MySQL
{
    /*  The KeepAlive mod by AlexRa, 25-Apr-2010:  Instead of connecting/disconnecting from the Db on each request,
     *  there is now an option to keep both the connection and the prepared SQL statements.  The comments in the code
     *  suggested that there should be a retry on the first failure, but no such thing was implemented, apparently.  
     *  Not sure how useful this is, but I've added the "retry" logic now, only in the situation when the connection
     *  was already present (KeepAlive) before the failed request. When this happens, we just re-open the connection
     *  and try one more time. Obviously there is no point in the re-trying if the connection has just been opened.
     * 
     *  As to UpdateAccessTime() (which is commented out now)
     */
    /// <summary>
    /// A MySQL Interface for the Asset Server
    /// </summary>
    public class MySQLAssetData : AssetDataBase
    {
        /// <summary>Whether to use KeepAlive if nothing is specified in the connection string or config.
        /// Should be FALSE for compatibility, now TRUE to make testing easier.
        /// </summary>
        const bool DEFAULT_KEEP_ALIVE = true;

        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private string m_connectionString;
        private object m_dbLock = new object();

        #region The Keep-Alive functionality

        private MySqlConnection m_conn;

        private MySqlCommand m_cmd_get;
        private MySqlCommand m_cmd_put;
        private MySqlCommand m_cmd_meta;
        private MySqlCommand m_cmd_exists;

        private void clear_cmd(ref MySqlCommand cmd)
        {
            if (cmd != null)
            {
                try
                {
                    cmd.Dispose();
                }
                finally
                {
                    cmd = null;
                }
            }
        }

        public void Disconnect()
        {
            clear_cmd(ref m_cmd_get);
            clear_cmd(ref m_cmd_meta);
            clear_cmd(ref m_cmd_exists);
            clear_cmd(ref m_cmd_put);

            if (m_conn != null)
            {
                try
                {
                    m_conn.Dispose();
                }
                finally
                {
                    m_conn = null;
                }
            }
        }

        public void Connect()
        {
            if (m_conn == null)
                m_conn = new MySqlConnection(m_connectionString);
            if (m_conn.State != ConnectionState.Open)
                m_conn.Open();
        }

        private bool m_KeepAlive = true;

        public bool KeepAlive
        {
            get
            {
                return m_KeepAlive;
            }
            set
            {
                m_KeepAlive = value;
                if (!value)
                    Disconnect();
            }
        }

        private MySqlCommand GetCmd(ref MySqlCommand cmd, string sql)
        {
            Connect();
            if (cmd == null)
                cmd = new MySqlCommand(sql, m_conn);
            if (!cmd.IsPrepared)
                cmd.Prepare();
            return cmd;
        }

        private void set_param(MySqlCommand cmd, string sName, object val)
        {
            if (cmd.Parameters.Contains(sName))
                cmd.Parameters[sName].Value = val;
            else
                cmd.Parameters.AddWithValue(sName, val);
        }

        private MySqlCommand GetCmd(ref MySqlCommand cmd, string sql, string uid)
        {
            GetCmd(ref cmd, sql);
            if (!String.IsNullOrEmpty(uid))
                set_param(cmd, "?id", uid);
            return cmd;
        }

        private MySqlCommand GetCmd(ref MySqlCommand cmd, string sql, UUID uid)
        {
            return GetCmd(ref cmd, sql, uid.ToString());
        }

        /// <summary>Couldn't figure out how to pass Config info correctly into Initialize(),
        /// so as a (temporary?) workaround the KeepAlive parameter is added to the connection
        /// string. This function checks if KeepAlive is present, returns its value and removes it
        /// from the conn string.
        /// 
        /// This is not a good solution, as it may affect other places where the conn string is used.
        /// </summary>
        /// <param name="conn"></param>
        /// <returns></returns>
        private bool extract_keep_alive(ref string connect, bool bDefault)
        {
            int ndx = connect.IndexOf("KeepAlive=", 0, StringComparison.InvariantCultureIgnoreCase);
            if (ndx < 0)
                return bDefault;

            string[] pars = connect.Split(';');
            connect = "";
            foreach (string s in pars)
            {
                if (s.StartsWith("KeepAlive=", StringComparison.InvariantCultureIgnoreCase))
                {
                    bDefault = Convert.ToBoolean(s.Substring(10));
                }
                else
                {
                    connect += s + ";";
                }
            }
            return bDefault;
        }

        #endregion

        #region IPlugin Members

        public override string Version { get { return "1.0.0.0"; } }

        /// <summary>
        /// <para>Initialises Asset interface</para>
        /// <para>
        /// <list type="bullet">
        /// <item>(already done elsewhere?) Loads and initialises the MySQL storage plugin.</item>
        /// <item>(doesn't!) Warns and uses the obsolete mysql_connection.ini if connect string is empty.</item>
        /// <item>Check for migration</item>
        /// </list>
        /// </para>
        /// </summary>
        /// <param name="connect">connect string</param>
        public override void Initialise(string connect)
        {
            KeepAlive = extract_keep_alive(ref connect, DEFAULT_KEEP_ALIVE);
            
            m_connectionString = connect;

            // This actually does the roll forward assembly stuff
            Assembly assem = GetType().Assembly;

            using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
            {
                dbcon.Open();
                Migration m = new Migration(dbcon, assem, "AssetStore");
                m.Update();
            }
        }

        public override void Initialise()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// In KeepAlive mode, dispose of any prepared statements and disconnect from the database
        /// </summary>
        public override void Dispose() 
        {
            Disconnect();
        }

        /// <summary>
        /// The name of this DB provider
        /// </summary>
        override public string Name
        {
            get { return "MySQL Asset storage engine"; }
        }

        #endregion

        #region IAssetDataPlugin Members

        /// <summary>GetAsset without any error catching, used twice in the code below
        /// 
        /// </summary>
        private AssetBase GetAsset_inner(UUID assetID)
        {
            MySqlCommand cmd = GetCmd(ref m_cmd_get, "SELECT name, description, assetType, local, temporary, data FROM assets WHERE id=?id", assetID.ToString());
            using (MySqlDataReader dbReader = cmd.ExecuteReader(CommandBehavior.SingleRow))
            {
                if (!dbReader.Read())
                    return null;

                AssetBase asset = new AssetBase(assetID, (string)dbReader[0], (sbyte)dbReader[2], UUID.Zero.ToString());
                asset.Description = (string)dbReader[1];

                // ?? Weirdness: why not just Convert.ToBoolean() as below?
                string local = dbReader[3].ToString();
                asset.Local = (local.Equals("1") || local.Equals("true", StringComparison.InvariantCultureIgnoreCase));
                asset.Temporary = Convert.ToBoolean(dbReader[4]);
                asset.Data = (byte[])dbReader[5];

                return asset;
            }
        }

        /// <summary>
        /// Fetch Asset <paramref name="assetID"/> from database
        /// </summary>
        /// <param name="assetID">Asset UUID to fetch</param>
        /// <returns>Return the asset or Null</returns>
        /// <remarks>On failure : log an exception and attempt to reconnect to database, returns Null if still fails</remarks>
        override public AssetBase GetAsset(UUID assetID)
        {
            lock (m_dbLock)
            {
                try
                {
                    if ((m_conn != null) && (m_conn.State == ConnectionState.Open))     // conn already open, may be old, so go for retry if fails
                    {
                        try
                        {
                            return GetAsset_inner(assetID);
                        }
                        catch (Exception e)
                        {
                            m_log.WarnFormat("[ASSETS DB]: MySql failure fetching asset {0}: {1}\n  Trying to reconnect...", assetID, e.Message);
                            // fall through to retry
                        }
                    }
                    Disconnect();

                    try
                    {
                        return GetAsset_inner(assetID);
                    }
                    catch (Exception e)
                    {
                        m_log.Error("[ASSETS DB]: MySql failure fetching asset " + assetID + ": " + e.Message);
                    }
                }
                finally
                {
                    if (!KeepAlive)
                        Disconnect();
                }
            }
            return null;
        }

        private void StoreAsset_inner(AssetBase asset, string assetName, string assetDescr)
        {
            string sql = "replace INTO assets(id, name, description, assetType, local, temporary, create_time, access_time, data) " +
                    "VALUES(?id, ?name, ?description, ?assetType, ?local, ?temporary, ?create_time, ?access_time, ?data)";

            MySqlCommand cmd = GetCmd(ref m_cmd_put, sql, asset.ID);

            int now = (int)Utils.DateTimeToUnixTime(DateTime.UtcNow);
            set_param(cmd, "?name", assetName);
            set_param(cmd, "?description", assetDescr);
            set_param(cmd, "?assetType", asset.Type);
            set_param(cmd, "?local", asset.Local);
            set_param(cmd, "?temporary", asset.Temporary);
            set_param(cmd, "?create_time", now);
            set_param(cmd, "?access_time", now);
            set_param(cmd, "?data", asset.Data);
            cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Create an asset in database, or update it if existing.
        /// </summary>
        /// <param name="asset">Asset UUID to create</param>
        /// <remarks>On failure : Throw an exception and attempt to reconnect to database</remarks>
        override public void StoreAsset(AssetBase asset)
        {
            string assetName = asset.Name;
            if (asset.Name.Length > 64)
            {
                assetName = asset.Name.Substring(0, 64);
                m_log.Warn("[ASSET DB]: Name field truncated from " + asset.Name.Length + " to " + assetName.Length + " characters on add");
            }

            string assetDescription = asset.Description;
            if (asset.Description.Length > 64)
            {
                assetDescription = asset.Description.Substring(0, 64);
                m_log.Warn("[ASSET DB]: Description field truncated from " + asset.Description.Length + " to " + assetDescription.Length + " characters on add");
            }

            lock (m_dbLock)
            {
                if (m_conn != null)     // conn already open, may be old, so go for retry if fails
                {
                    try
                    {
                        StoreAsset_inner(asset, assetName, assetDescription);
                        return;
                    }
                    catch (Exception e)
                    {
                        m_log.WarnFormat("[ASSETS DB]: MySql failure storing asset {0}: {1}\n  Trying to reconnect...", asset.ID, e.Message);
                        Disconnect();
                        // fall through to the next try
                    }
                }

                try
                {
                    StoreAsset_inner(asset, assetName, assetDescription);
                }
                catch (Exception e)
                {
                    m_log.Error("[ASSETS DB]: MySql failure storing asset " + asset.ID + ": " + e.Message);
                }
                finally
                {
                    if (!KeepAlive)
                        Disconnect();
                }
            }
        }

        private void UpdateAccessTime(AssetBase asset)
        {
            // Writing to the database every time Get() is called on an asset is killing us. Seriously. -jph
            return;

/*
            lock (m_dbLock)
            {
                using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
                {
                    dbcon.Open();
                    MySqlCommand cmd =
                        new MySqlCommand("update assets set access_time=?access_time where id=?id",
                                         dbcon);

                    // need to ensure we dispose
                    try
                    {
                        using (cmd)
                        {
                            // create unix epoch time
                            int now = (int)Utils.DateTimeToUnixTime(DateTime.UtcNow);
                            cmd.Parameters.AddWithValue("?id", asset.ID);
                            cmd.Parameters.AddWithValue("?access_time", now);
                            cmd.ExecuteNonQuery();
                            cmd.Dispose();
                        }
                    }
                    catch (Exception e)
                    {
                        m_log.ErrorFormat(
                            "[ASSETS DB]: " +
                            "MySql failure updating access_time for asset {0} with name {1}" + Environment.NewLine + e.ToString()
                            + Environment.NewLine + "Attempting reconnection", asset.FullID, asset.Name);
                    }
                }
            }
*/
        }


        private bool ExistsAsset_inner(UUID uuid)
        {
            MySqlCommand cmd = GetCmd(ref m_cmd_exists, "SELECT id FROM assets WHERE id=?id", uuid.ToString());
            using (MySqlDataReader dbReader = cmd.ExecuteReader(CommandBehavior.SingleRow))
            {
                return dbReader.Read();  
            }
        }

        /// <summary>
        /// check if the asset UUID exist in database
        /// </summary>
        /// <param name="uuid">The asset UUID</param>
        /// <returns>true if exist.</returns>
        override public bool ExistsAsset(UUID uuid)
        {
            lock (m_dbLock)
            {
                if (m_conn != null)     // conn already open, may be old, so go for retry if fails
                {
                    try
                    {
                        return ExistsAsset_inner(uuid);
                    }
                    catch (Exception e)
                    {
                        m_log.WarnFormat("[ASSETS DB]: MySql failure checking asset {0}: {1}\n  Trying to reconnect...", uuid.ToString(), e.Message);
                        Disconnect();
                        // fall through to the next try
                    }
                }

                try
                {
                    return ExistsAsset_inner(uuid);
                }
                catch (Exception e)
                {
                    m_log.Error("[ASSETS DB]: MySql failure storing asset " + uuid.ToString() + ": " + e.Message);
                    return false;
                }
                finally
                {
                    if (!KeepAlive)
                        Disconnect();
                }
            }
        }

        private MySqlDataReader FetchAssetMetadataSet_reader(int start, int count)
        {
            List<AssetMetadata> retList = new List<AssetMetadata>(count);
            MySqlCommand cmd = GetCmd(ref m_cmd_meta, "SELECT name,description,assetType,temporary,id FROM assets LIMIT ?start, ?count", null);
            cmd.Parameters.AddWithValue("?start", start);
            cmd.Parameters.AddWithValue("?count", count);
            return cmd.ExecuteReader();
        }

            /// <summary>
        /// Returns a list of AssetMetadata objects. The list is a subset of
        /// the entire data set offset by <paramref name="start" /> containing
        /// <paramref name="count" /> elements.
        /// </summary>
        /// <param name="start">The number of results to discard from the total data set.</param>
        /// <param name="count">The number of rows the returned list should contain.</param>
        /// <returns>A list of AssetMetadata objects.</returns>
        public override List<AssetMetadata> FetchAssetMetadataSet(int start, int count)
        {
            List<AssetMetadata> retList = new List<AssetMetadata>(count);
            MySqlDataReader dbReader = null; 
            lock (m_dbLock)
            {
                try
                {
                    if (m_conn != null)     // conn already open, may be old, so go for retry if fails
                    {
                        try
                        {
                            dbReader = FetchAssetMetadataSet_reader(start, count);
                        }
                        catch (Exception e)
                        {
                            m_log.WarnFormat("[ASSETS DB]: MySql failure reading asset set: {1}\n  Trying to reconnect...", e.Message);
                            Disconnect();
                            // fall through to the next try
                        }
                    }

                    if( dbReader == null )
                        dbReader = FetchAssetMetadataSet_reader(start, count);

                    while (dbReader.Read())
                    {
                        AssetMetadata metadata = new AssetMetadata();
                        metadata.Name = (string)dbReader["name"];
                        metadata.Description = (string)dbReader["description"];
                        metadata.Type = (sbyte)dbReader["assetType"];
                        metadata.Temporary = Convert.ToBoolean(dbReader["temporary"]); // Not sure if this is correct.
                        metadata.FullID = new UUID((string)dbReader["id"]);

                        // Current SHA1s are not stored/computed.
                        metadata.SHA1 = new byte[] { };

                        retList.Add(metadata);
                    }

                }
                catch (Exception e)
                {
                    m_log.Error("[ASSETS DB]: MySql failure fetching asset set" + Environment.NewLine + e.ToString());
                    Disconnect();
                }
                finally
                {
                    if (dbReader != null)
                    {
                        dbReader.Dispose();
                        dbReader = null;
                    }
                    if (!KeepAlive)
                        Disconnect();
                }
                return retList;
            }
        }

        #endregion
    }
}
