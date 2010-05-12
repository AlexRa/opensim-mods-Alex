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
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using OpenMetaverse;
using OpenSim.Framework;
using System.Data;
using System.Data.Common;

namespace OpenSim.Data
{
    [Table("assets")]
    public class AssetDataBase<TConn, TDataSpec> : BaseDataBaseEx<TConn, TDataSpec>, IAssetDataPlugin
        where TConn : DbConnection, new()
        where TDataSpec : DataSpecificBase, new() 
    {
        public Cmd GetAssetCmd;
        public Cmd GetMetaCmd;
        public Cmd CheckCmd;
        public Cmd DeleteCmd;

        protected int m_got_asset_count = 0;
        protected int m_notfound_count = 0;
        protected int m_update_count = 0;

        public AssetDataBase() : base()
        {
            GetAssetCmd = new Cmd(this, "SELECT name, assetType, creatorid, description, local, temporary, asset_flags, data FROM assets WHERE id = @id", typeof(UUID));
            GetMetaCmd = new Cmd(this, "SELECT name, assetType, creatorid, description, local, temporary, asset_flags FROM assets WHERE id = @id", typeof(UUID));
            CheckCmd = new Cmd(this, "SELECT local FROM assets WHERE id = @id", typeof(UUID));
            DeleteCmd = new Cmd(this, "DELETE FROM assets WHERE id = @id", typeof(UUID));
            // NOTE:  no SQL for StoreAsset() here, because the UPDATE/INSERT implementation will be different!
        }

        protected override string GetMigrationStore()
        {
            return "AssetStore";
        }

        public override string Name
        {
            get { return DBMS.DBName() + " Asset storage engine"; }
        }

        // NOT defined here (too DBMS-specific):
        public virtual void StoreAsset(AssetBase asset)
        {
            throw new NotImplementedException();
        }

        public virtual List<AssetMetadata> FetchAssetMetadataSet(int start, int count)
        {
            throw new NotImplementedException();
        }

        public virtual AssetBase GetAsset(UUID assetID)
        {
            AssetBase asset = null;
            try
            {
                GetAssetCmd.Query(
                    delegate(IDataReader reader)
                    {
                        // columns: name, assetType, creatorid, description, local, temporary, data
                        asset = new AssetBase(
                            assetID,
                            reader["name"].ToString(),
                            Convert.ToSByte(reader["assetType"]),
                            DBMS.DbToUuid(reader["creatorid"])
                        );
                        // Region Main
                        asset.Description = reader["description"].ToString();
                        asset.Local = DBMS.DbToBool(reader["local"]);
                        asset.Temporary = DBMS.DbToBool(reader["temporary"]);
                        asset.Flags = (AssetFlags)Convert.ToInt32(reader["asset_flags"]);
                        asset.Data = (byte[])reader["data"];
                        m_got_asset_count++;
                        return false;
                    },
                    true, assetID
                );
            }
            catch (Exception e)
            {
                m_log.Error("[ASSETS DB]: failure fetching asset " + assetID + ": " + e.Message);
            }

            if (asset == null)
                m_notfound_count++;

            return asset;
        }

        protected AssetMetadata ReadMeta(IDataReader reader, UUID assetID)
        {
            AssetMetadata meta = new AssetMetadata();
            meta.FullID = assetID;
            meta.Name = reader["name"].ToString();
            meta.Type = Convert.ToSByte(reader["assetType"]);
            meta.CreatorID = DBMS.DbToUuidStr(reader["creatorid"]);
            meta.Description = reader["description"].ToString();
            meta.Local = DBMS.DbToBool(reader["local"]);
            meta.Temporary = DBMS.DbToBool(reader["temporary"]);
            meta.Flags = (AssetFlags)Convert.ToInt32(reader["asset_flags"]);

            // Current SHA1s are not stored/computed.
            meta.SHA1 = new byte[] { };
            return meta;
        }

        protected AssetMetadata ReadMeta(IDataReader reader)
        {
            return ReadMeta(reader, DBMS.DbToUuid(reader["id"]));
        }

        public virtual AssetMetadata GetMetadata(UUID assetID)
        {
            AssetMetadata meta = null;
            try
            {
                GetAssetCmd.Query(
                    delegate(IDataReader reader)
                    {
                        // columns: name, assetType, creatorid, description, local, temporary
                        meta = ReadMeta(reader, assetID);
                        return false;
                    },
                    true, assetID
                );
            }
            catch (Exception e)
            {
                m_log.Error("[ASSETS DB]: failure fetching metadata of asset " + assetID.ToString() + ": " + e.Message);
            }
            return meta;
        }

        public virtual bool DeleteAsset(UUID assetID)
        {
            try
            {
                DeleteCmd.Exec(assetID);
            }
            catch (Exception e)
            {
                m_log.Error("[ASSETS DB]: failure trying to delete asset " + assetID.ToString() + ": " + e.Message);
                return false;
            }
            return true;
        }

        public virtual bool ExistsAsset(UUID assetID)
        {
            bool exists = false;
            try
            {
                CheckCmd.Query(
                    delegate(IDataReader reader)
                    {
                        exists = true;
                        return false;
                    },
                    true, assetID
                );
            }
            catch (Exception e)
            {
                m_log.Error("[ASSETS DB]: failure checking for asset " + assetID.ToString() + ": " + e.Message);
                return false;
            }
            return exists;
        }

        protected void TrimNameAndDescr(AssetBase asset, out string assetName, out string assetDescr, int MaxLen)
        {
            assetName = asset.Name;
            if (assetName.Length > MaxLen)
            {
                assetName = asset.Name.Substring(0, MaxLen);
                m_log.Warn("[ASSET DB]: Name field truncated from " + asset.Name.Length + " to " + MaxLen + " characters on add");
            }

            assetDescr = asset.Description;
            if (asset.Description.Length > MaxLen)
            {
                assetDescr = assetDescr.Substring(0, MaxLen);
                m_log.Warn("[ASSET DB]: Description field truncated from " + asset.Description.Length + " to " + MaxLen + " characters on add");
            }
        }
    }
}
