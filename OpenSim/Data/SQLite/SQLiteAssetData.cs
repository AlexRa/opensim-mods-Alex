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
using Mono.Data.Sqlite;
using OpenMetaverse;
using OpenSim.Framework;

namespace OpenSim.Data.SQLite
{
    /// <summary>
    /// An asset storage interface for the SQLite database system
    /// </summary>
    public class SQLiteAssetData : AssetDataBase<SqliteConnection, SQLiteDataSpecific> 
    {
        public Cmd InsertCmd;
        public Cmd UpdateCmd;
        public Cmd MetaListCmd;

        public SQLiteAssetData()
            : base()
        {
            InsertCmd = new Cmd(this, "insert into assets(ID, Name, Description, AssetType, Local, Temporary, CreatorID, Data) values(" +
                "@ID, @Name, @Description, @AssetType, @Local, @Temporary, @CreatorID, @Data)",
                typeof(UUID), typeof(string), typeof(string), typeof(sbyte), typeof(bool), typeof(bool), typeof(UUID), typeof(byte[])
                );

            UpdateCmd = new Cmd(this, "update assets set Name=@Name, Description=@Description, AssetType=@AssetType, Local=@Local, Temporary=@Temporary, CreatorID=@CreatorID, Data=@Data where ID=@ID",
                typeof(string), typeof(string), typeof(sbyte), typeof(bool), typeof(bool), typeof(UUID), typeof(byte[]), typeof(UUID)
                );

            MetaListCmd = new Cmd(this, "select ID, Name, Description, AssetType, Local, Temporary, CreatorID from assets limit @start, @count",
                typeof(int), typeof(int));
        }

        /// <summary>
        /// Name of this DB provider
        /// </summary>
        override public string Name
        {
            get { return "SQLite Asset storage engine"; }
        }


        /// <summary>
        /// <list type="bullet">
        /// <item>Initialises AssetData interface</item>
        /// <item>Loads and initialises a new SQLite connection and maintains it.</item>
        /// <item>use default URI if connect string is empty.</item>
        /// </list>
        /// </summary>
        /// <param name="dbconnect">connect string</param>
        override public void Initialise(string dbconnect)
        {
            if (String.IsNullOrEmpty(dbconnect))
            {
                dbconnect = "URI=file:Asset.db,version=3";
            }

            base.Initialise(dbconnect);
        }

        /// <summary>
        /// Initialise the AssetData interface using default URI
        /// </summary>
        override public void Initialise()
        {
            Initialise(null);
        }

        /// <summary>
        /// Create an asset
        /// </summary>
        /// <param name="asset">Asset Base</param>
        override public void StoreAsset(AssetBase asset)
        {
            //m_log.Info("[ASSET DB]: Creating Asset " + asset.FullID.ToString());
            if (ExistsAsset(asset.FullID))
            {
                // Name, Description, Type, Local, Temporary, CreatorID, Data, ID
                UpdateCmd.Exec(
                    asset.Name, asset.Description, asset.Type, DBMS.BoolToDb(asset.Local), 
                    DBMS.BoolToDb(asset.Temporary), DBMS.UuidToDb(asset.Metadata.CreatorID), asset.Data, DBMS.UuidToDb(asset.FullID)
                );
            }
            else
            {
                // ID, Name, Description, Type, Local, Temporary, CreatorID, Data
                InsertCmd.Exec(
                    DBMS.UuidToDb(asset.FullID), asset.Name, asset.Description, asset.Type, DBMS.BoolToDb(asset.Local),
                    DBMS.BoolToDb(asset.Temporary), DBMS.UuidToDb(asset.Metadata.CreatorID), asset.Data
                );
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

            MetaListCmd.Query(
                delegate(IDataReader row)
                {
                    AssetMetadata metadata = ReadMeta(row);
                    retList.Add(metadata);
                    return true;
                },
                false, start, count);

            return retList;
        }

#if SOME_TEST_CODE_HERE
        /// <summary>
        /// Some... logging functionnality
        /// </summary>
        /// <param name="asset"></param>
        private static void LogAssetLoad(AssetBase asset)
        {
            string temporary = asset.Temporary ? "Temporary" : "Stored";
            string local = asset.Local ? "Local" : "Remote";

            int assetLength = (asset.Data != null) ? asset.Data.Length : 0;

            m_log.Debug("[ASSET DB]: " +
                                     string.Format("Loaded {5} {4} Asset: [{0}][{3}] \"{1}\":{2} ({6} bytes)",
                                                   asset.FullID, asset.Name, asset.Description, asset.Type,
                                                   temporary, local, assetLength));
        }

#endif
    }
}
