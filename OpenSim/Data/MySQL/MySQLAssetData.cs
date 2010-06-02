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
using System.Data.Common;
using System.Reflection;
using System.Collections.Generic;
using log4net;
using MySql.Data.MySqlClient;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Data;

namespace OpenSim.Data.MySQL
{
    /// <summary>How the code tracks the asset acces times. This is probably not needed
    /// for the real use, but is convenient for performance testing. 
    /// 
    /// </summary>
    public enum AccessTrackMode
    {
        None = 0,           // access times not tracked (fastest, but not what we want)
        UpdateFromCode = 1, // use a separate UPDATE from the code (slow)
        FastUpdate = 2,     // delayed update on MySql side (supposed to be the best?)
        FastLog = 3         // same as above, use simpler logging method
    };


    /// <summary>
    /// A MySQL Interface for the Asset Server
    /// </summary>
    public class MySQLAssetData : AssetDataBase
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private string m_connectionString;
        private object m_dbLock = new object();

        private AccessTrackMode m_track_mode = AccessTrackMode.None;
        private int m_track_delay = 10;

        public AccessTrackMode AccessTrackMode { get { return m_track_mode; } set { m_track_mode = value; } }

        /// <summary>The minimum access tracking interval, in sec: if an asset was last accessed
        /// less than AccessTrackDelay seconds ago, don't update the access time again.
        /// </summary>
        public int AccessTrackDelay { get { return m_track_delay; } set { m_track_delay = value; } }

        #region IPlugin Members

        public override string Version { get { return "1.0.0.0"; } }

        /// <summary>
        /// <para>Initialises Asset interface</para>
        /// <para>
        /// <list type="bullet">
        /// <item>Loads and initialises the MySQL storage plugin.</item>
        /// <item>Warns and uses the obsolete mysql_connection.ini if connect string is empty.</item>
        /// <item>Check for migration</item>
        /// </list>
        /// </para>
        /// </summary>
        /// <param name="connect">connect string</param>
        public override void Initialise(string connect)
        {
            m_connectionString = connect;

            // This actually does the roll forward assembly stuff
            Assembly assem = GetType().Assembly;

            using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
            {
                // NOTE: here we could use either Migration or MySqlMigration class, doesn't matter unless we have scripts!
                dbcon.Open();
                Migration m = new MySqlMigration(dbcon, assem, "AssetStore");
                m.Update();

                // We also have to take care about the stored procedure used for server-side access time tracking.
                // The problem is that the DB can be copied from another DB (without actually running the migrations).
                // That leaves any SPs behind, but does update the "AssetStore" record in [migrations].  So let's
                // put any procs/funcs into a separate migration script "AssetStoreProcs".

                using(DbCommand cmd = dbcon.CreateCommand() )
                {
                    cmd.CommandText = "update migrations set version = f_asset_ver() where name='AssetStoreProcs'";
                    try
                    {
                        // We expect this to fail if the version function is not defined (which is normal)
                        cmd.ExecuteNonQuery();
                    }
                    catch
                    {
                        // Without the version func, make sure the entire Procs migration will run. 
                        cmd.CommandText = "delete from migrations where name='AssetStoreProcs'";
                        try
                        {
                            cmd.ExecuteNonQuery();
                        }
                        catch(Exception e)
                        {
                            // there is no good reason for it to fail, but still proceed as if nothing has happened (?) 
                            m_log.Error("[MySQL ASSETS]:", e); 
                        }
                    }
                }

                // *Must* use MySqlMigration here to define procs/funcs!
                m = new MySqlMigration(dbcon, assem, "AssetStoreProcs");
                m.Update();
            }
        }

        public override void Initialise()
        {
            throw new NotImplementedException();
        }

        public override void Dispose() 
        {
            Flush();
        }

        /// <summary>
        /// The name of this DB provider
        /// </summary>
        override public string Name
        {
            get { return "MySQL Asset storage engine"; }
        }

        #endregion

        /// <summary>Do whatever is necessary to commit to the DB whatever might be cached, or do
        /// some housekeeping in the DB or whatever.  It is called when the server is being stopped,
        /// but could also be called occasionally (by timer or whatever) if it makes sense for a
        /// specific store.
        /// 
        /// Here this is used to run the stored proc which updates the asset access times.
        /// 
        /// </summary>
        public void Flush()
        {
            if (m_track_mode < AccessTrackMode.FastUpdate)
                return;

            using (DbConnection dbcon = new MySqlConnection(m_connectionString))
            {
                dbcon.Open();
                using (DbCommand cmd = dbcon.CreateCommand())
                {
                    cmd.CommandText = (m_track_mode == AccessTrackMode.FastUpdate) ?
                        "call asset_flush_log(1);" : "call asset_flush_log1();";
                    try
                    {
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception e)
                    {
                        m_log.Error("Error updating asset access times", e);
                    }
                }
            }
        }

        #region IAssetDataPlugin Members

        /// <summary>
        /// Fetch Asset <paramref name="assetID"/> from database
        /// </summary>
        /// <param name="assetID">Asset UUID to fetch</param>
        /// <returns>Return the asset</returns>
        /// <remarks>On failure : throw an exception and attempt to reconnect to database</remarks>
        override public AssetBase GetAsset(UUID assetID)
        {
            AssetBase asset = null;
            lock (m_dbLock)
            {
                using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
                {
                    dbcon.Open();

                    using (MySqlCommand cmd = dbcon.CreateCommand())
                    {
                        string s = "";
                        switch (m_track_mode)
                        {
                            case AccessTrackMode.UpdateFromCode:
                                s = ", access_time, UNIX_TIMESTAMP() this_time";
                                break;
                            case AccessTrackMode.FastUpdate:
                                s = String.Format(", asset_accessed(id, access_time, {0})", m_track_delay);
                                break;
                            case AccessTrackMode.FastLog:
                                s = String.Format(", asset_accessed1(id, access_time, {0})", m_track_delay);
                                break;
                        }

                        cmd.CommandText = String.Format("SELECT name, description, assetType, local, temporary, " +
                            "asset_flags, CreatorID, data {0} FROM assets WHERE id=?id", s);
                        cmd.Parameters.AddWithValue("?id", assetID.ToString());

                        int last_access = 0, this_access = 0;
                        bool bNeedUpdate = false;
                        try
                        {
                            using (DbDataReader dbReader = cmd.ExecuteReader(CommandBehavior.SingleRow))
                            {
                                if (dbReader.Read())
                                {
                                    asset = new AssetBase(assetID, (string)dbReader["name"], (sbyte)dbReader["assetType"], dbReader["CreatorID"].ToString());
                                    asset.Data = (byte[])dbReader["data"];
                                    asset.Description = (string)dbReader["description"];

                                    string local = dbReader["local"].ToString();
                                    if (local.Equals("1") || local.Equals("true", StringComparison.InvariantCultureIgnoreCase))
                                        asset.Local = true;
                                    else
                                        asset.Local = false;

                                    asset.Temporary = Convert.ToBoolean(dbReader["temporary"]);
                                    asset.Flags = (AssetFlags)Convert.ToInt32(dbReader["asset_flags"]);

                                    if (m_track_mode == AccessTrackMode.UpdateFromCode)
                                    {
                                        last_access = Convert.ToInt32(dbReader["access_time"]);
                                        this_access = Convert.ToInt32(dbReader["this_time"]); 
                                        // NOTE: weird to pull current timestamp from DB, but should be eventually removed anyway
                                        bNeedUpdate = (this_access - last_access) > m_track_delay;
                                    }
                                }
                            }

                            if (bNeedUpdate)
                            {
                                cmd.CommandText = "UPDATE assets SET access_time = UNIX_TIMESTAMP() WHERE id=?id";
                                // (Param ?id is already there)
                                cmd.ExecuteNonQuery();
                            }
                        }
                        catch (Exception e)
                        {
                            m_log.Error("[ASSETS DB]: MySql failure fetching asset " + assetID + ": " + e.Message);
                        }
                    }
                }
            }
            return asset;
        }

        /// <summary>
        /// Create an asset in database, or update it if existing.
        /// </summary>
        /// <param name="asset">Asset UUID to create</param>
        /// <remarks>On failure : Throw an exception and attempt to reconnect to database</remarks>
        override public void StoreAsset(AssetBase asset)
        {
            lock (m_dbLock)
            {
                using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
                {
                    dbcon.Open();

                    MySqlCommand cmd =
                        new MySqlCommand(
                            "replace INTO assets(id, name, description, assetType, local, temporary, create_time, access_time, asset_flags, CreatorID, data)" +
                            "VALUES(?id, ?name, ?description, ?assetType, ?local, ?temporary, ?create_time, ?access_time, ?asset_flags, ?CreatorID, ?data)",
                            dbcon);

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

                    // need to ensure we dispose
                    try
                    {
                        using (cmd)
                        {
                            // create unix epoch time
                            int now = (int)Utils.DateTimeToUnixTime(DateTime.UtcNow);
                            cmd.Parameters.AddWithValue("?id", asset.ID);
                            cmd.Parameters.AddWithValue("?name", assetName);
                            cmd.Parameters.AddWithValue("?description", assetDescription);
                            cmd.Parameters.AddWithValue("?assetType", asset.Type);
                            cmd.Parameters.AddWithValue("?local", asset.Local);
                            cmd.Parameters.AddWithValue("?temporary", asset.Temporary);
                            cmd.Parameters.AddWithValue("?create_time", now);
                            cmd.Parameters.AddWithValue("?access_time", now);
                            cmd.Parameters.AddWithValue("?CreatorID", asset.Metadata.CreatorID);
                            cmd.Parameters.AddWithValue("?asset_flags", (int)asset.Flags);
                            cmd.Parameters.AddWithValue("?data", asset.Data);
                            cmd.ExecuteNonQuery();
                            cmd.Dispose();
                        }
                    }
                    catch (Exception e)
                    {
                        m_log.ErrorFormat("[ASSET DB]: MySQL failure creating asset {0} with name \"{1}\". Error: {2}",
                            asset.FullID, asset.Name, e.Message);
                    }
                }
            }
        }

        private void UpdateAccessTime(AssetBase asset)
        {
            // Writing to the database every time Get() is called on an asset is killing us. Seriously. -jph
            return;

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

        }

        /// <summary>
        /// check if the asset UUID exist in database
        /// </summary>
        /// <param name="uuid">The asset UUID</param>
        /// <returns>true if exist.</returns>
        override public bool ExistsAsset(UUID uuid)
        {
            bool assetExists = false;

            lock (m_dbLock)
            {
                using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
                {
                    dbcon.Open();
                    using (MySqlCommand cmd = new MySqlCommand("SELECT id FROM assets WHERE id=?id", dbcon))
                    {
                        cmd.Parameters.AddWithValue("?id", uuid.ToString());

                        try
                        {
                            using (MySqlDataReader dbReader = cmd.ExecuteReader(CommandBehavior.SingleRow))
                            {
                                if (dbReader.Read())
                                    assetExists = true;
                            }
                        }
                        catch (Exception e)
                        {
                            m_log.ErrorFormat(
                                "[ASSETS DB]: MySql failure fetching asset {0}" + Environment.NewLine + e.ToString(), uuid);
                        }
                    }
                }
            }

            return assetExists;
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

            lock (m_dbLock)
            {
                using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
                {
                    dbcon.Open();
                    MySqlCommand cmd = new MySqlCommand("SELECT name,description,assetType,temporary,id,asset_flags,CreatorID FROM assets LIMIT ?start, ?count", dbcon);
                    cmd.Parameters.AddWithValue("?start", start);
                    cmd.Parameters.AddWithValue("?count", count);

                    try
                    {
                        using (MySqlDataReader dbReader = cmd.ExecuteReader())
                        {
                            while (dbReader.Read())
                            {
                                AssetMetadata metadata = new AssetMetadata();
                                metadata.Name = (string)dbReader["name"];
                                metadata.Description = (string)dbReader["description"];
                                metadata.Type = (sbyte)dbReader["assetType"];
                                metadata.Temporary = Convert.ToBoolean(dbReader["temporary"]); // Not sure if this is correct.
                                metadata.Flags = (AssetFlags)Convert.ToInt32(dbReader["asset_flags"]);
                                metadata.FullID = DBGuid.FromDB(dbReader["id"]);
                                metadata.CreatorID = dbReader["CreatorID"].ToString();

                                // Current SHA1s are not stored/computed.
                                metadata.SHA1 = new byte[] { };

                                retList.Add(metadata);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        m_log.Error("[ASSETS DB]: MySql failure fetching asset set" + Environment.NewLine + e.ToString());
                    }
                }
            }

            return retList;
        }

        public override bool Delete(string id)
        {
            lock (m_dbLock)
            {
                using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
                {
                    dbcon.Open();
                    MySqlCommand cmd = new MySqlCommand("delete from assets where id=?id", dbcon);
                    cmd.Parameters.AddWithValue("?id", id);
                    cmd.ExecuteNonQuery();

                    cmd.Dispose();
                }
            }

            return true;
        }

        #endregion
    }
}
