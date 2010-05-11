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

//#define DELAYED_ACCESS_UPDATE

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
    /// <summary>
    /// A MySQL Interface for the Asset Server
    /// </summary>
    public sealed class MySQLAssetData : AssetDataBase<MySqlConnection, MySqlDataSpecific>
    {
        // NOTE: all Cmds must be PUBLIC, as they are scanned from the base class using Reflection!
        public Cmd UpdateCmd;
        public Cmd MetaListCmd;
        
        int m_KeyCacheSize = 0;

#if DELAYED_ACCESS_UPDATE
        private BaseDataBase.Cmd UpdateAccessTimesCmd;
#endif

        public MySQLAssetData()
            : base()
        {
#if DELAYED_ACCESS_UPDATE
            UpdateAccessTimesCmd = new Cmd(this, "call flush_asset_log();");
            GetAssetCmd = new Cmd(this, "SELECT name, assetType, creatorid, description, local, temporary, data " +
                ", mark_asset_read(id, access_time, 10) is_updated " +
                " FROM assets WHERE id = @id", typeof(UUID));

#endif
            UpdateCmd = new Cmd(this, "replace INTO assets(id, name, description, assetType, local, temporary, data, CreatorID, create_time, access_time)" +
                            "VALUES(@id, @name, @description, @assetType, @local, @temporary, @data, @CreatorID, UNIX_TIMESTAMP(), UNIX_TIMESTAMP())",
                            typeof(UUID), typeof(string), typeof(string), typeof(sbyte), typeof(bool), typeof(bool), typeof(byte[]), typeof(UUID)
                            );
            MetaListCmd = new Cmd(this, "SELECT id,name,description,assetType,temporary,local,CreatorID FROM assets LIMIT @start, @count",
                            typeof(int), typeof(int));
        }

#if DELAYED_ACCESS_UPDATE
        public override void Flush()
        {
            if( m_got_asset_count > 0 )
            {
                try
                {
                    UpdateAccessTimesCmd.Exec();
                }
                catch (Exception e)
                {
                    m_log.WarnFormat("[{0}]: Failed to update access times: {1}", Name, e.Message);
                }
            }
            base.Flush();
        }
#endif

        public override void Initialise(string connect)
        {
            // Extract AssetKeyCache=size, if present, from the conn string
            int cache_size = m_KeyCacheSize;
            string s = ExtractConnParam(ref connect, "AssetKeyCache");
            if (s != "" )
            {
                int scale = 1;
                s = s.ToLower();
                if( s.EndsWith("k") ) //|| s.EndsWith("kb") )
                {
                    scale = 1024;
                    s = s.Substring(0, s.Length - 1);
                } else if( s.EndsWith("m") ) //|| s.EndsWith("mb") )
                {
                    scale = 1024 * 1024;
                    s = s.Substring(0, s.Length - 1);
                }

                cache_size = 0;
                if( int.TryParse(s, out cache_size) && cache_size * scale >= 1024 )
                    m_KeyCacheSize = cache_size * scale;
                else
                    m_log.Warn("Invalid AssetKeyCache size - parameter ignored");
            }

            base.Initialise(connect);

            if( m_KeyCacheSize > 0 )
            {
                ExecSQL(String.Format("SET GLOBAL asset_cache.key_buffer_size={0}; cache index assets in asset_cache;", m_KeyCacheSize));
            }
        }


        /// <summary>
        /// Create an asset in database, or update it if existing.
        /// </summary>
        /// <param name="asset">Asset UUID to create</param>
        /// <remarks>On failure : Throw an exception and attempt to reconnect to database</remarks>
        override public void StoreAsset(AssetBase asset)
        {
            string assetDescription, assetName; 
            TrimNameAndDescr(asset, out assetName, out assetDescription, 64);

            try
            {
                // id, name, description, assetType, local, temporary, data, creator
                UpdateCmd.Exec(asset.FullID, assetName, assetDescription, asset.Type, asset.Local, asset.Temporary, asset.Data, new UUID(asset.Metadata.CreatorID));
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[ASSET DB]: MySQL failure storing asset {0} with name \"{1}\". Error: {2}", asset.FullID, asset.Name, e.Message);

            }
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

            // id,name,description,assetType,temporary,local,CreatorID
            try
            {
                MetaListCmd.Query(
                    delegate(IDataReader reader)
                    {
                        AssetMetadata metadata = ReadMeta(reader);
                        retList.Add(metadata);
                        return true;
                    },
                    false, start, count);
            }
            catch (Exception e)
            {
                m_log.Error("[ASSETS DB]: MySql failure fetching asset set: " + e.ToString());
            }

            return retList;
        }
    }
}
