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
using System.Data.SqlClient;
using System.Reflection;
using System.Collections.Generic;
using OpenMetaverse;
using log4net;
using OpenSim.Framework;
using OpenSim.Data;

namespace OpenSim.Data.MSSQL
{
    /// <summary>
    /// A MSSQL Interface for the Asset server
    /// </summary>
    public class MSSQLAssetData : AssetDataBase<MSSQLDataSpecific>
    {
        public Cmd StoreCmd;
        public Cmd MetaListCmd;

        private long m_ticksToEpoch = new System.DateTime(1970, 1, 1).Ticks;

        public MSSQLAssetData() : base()
        {
            StoreCmd = new Cmd(this, 
                 "IF EXISTS(SELECT * FROM [assets] WHERE id = @id) " +
                 "  UPDATE [assets] SET [name] = @name, [description] = @description, [assetType] = @assetType, " +
                 "  [local] = @local, [temporary] = @temporary, [create_time] = @create_time, [access_time] = @access_time, " +
                 "  [creatorid] = @creatorid, [asset_flags] = @asset_flags, [data] = @data WHERE id = @id" + 
                 " ELSE " +
                 "  INSERT INTO assets ([id], [name], [description], [assetType], [local], [temporary], [create_time], [access_time], [creatorid], [asset_flags], [data]) " +
                 "  VALUES (@id, @name, @description, @assetType, @local, @temporary, @create_time, @access_time, @creatorid, @asset_flags, @data)"
                 );

            MetaListCmd = new Cmd(this,
                @"WITH OrderedAssets AS
                (
                    SELECT [id], [name], [description], [assetType], [local], [temporary], [creatorid], [asset_flags],
                    [RowNumber] = ROW_NUMBER() OVER (ORDER BY [id])
                    FROM assets 
                ) 
                SELECT * 
                FROM OrderedAssets
                WHERE RowNumber BETWEEN @start AND @stop;",
                typeof(int), typeof(int)
                );
        }

        /// <summary>
        /// Create asset in m_database
        /// </summary>
        /// <param name="asset">the asset</param>
        override public void StoreAsset(AssetBase asset)
        {
            int now = (int)((System.DateTime.Now.Ticks - m_ticksToEpoch) / 10000000);

            string assetDescription, assetName;
            TrimNameAndDescr(asset, out assetName, out assetDescription, 64);

            try
            {
                // @id, @name, @descr, @assetType, @local, @temporary, @create_time, @access_time, @creatorid, @data
                StoreCmd.Exec(asset.FullID, assetName, assetDescription, (int)asset.Type, asset.Local,
                    asset.Temporary, now, now, asset.Metadata.CreatorID, (int)asset.Flags, asset.Data);
            }
            catch(Exception e)
            {
                m_log.ErrorFormat("[ASSET DB]: Error storing item {0}: {1}", asset.ID, e.Message);
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

            try
            {
                MetaListCmd.Query(
                    delegate(IDataReader reader)
                    {
                        AssetMetadata metadata = ReadMeta(reader);
                        retList.Add(metadata);
                        return true;
                    },
                    false, start, start + count - 1);
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[ASSET DB]: Error getting metadata list: {0}", e.Message);
            }
            return retList;
        }
    }
}
