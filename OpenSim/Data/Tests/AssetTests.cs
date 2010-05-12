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
using log4net.Config;
using NUnit.Framework;
using NUnit.Framework.Constraints;
using OpenMetaverse;
using OpenSim.Framework;
using System.Data.Common;
using log4net;

// DBMS-specific:
using MySql.Data.MySqlClient;
using OpenSim.Data.MySQL;

using System.Data.SqlClient;
using OpenSim.Data.MSSQL;

using Mono.Data.Sqlite;
using OpenSim.Data.SQLite;

namespace OpenSim.Data.Tests
{
    [TestFixture(typeof(MySqlConnection), typeof(MySQLAssetData), Description="Basic Asset store tests (MySQL)")]
    [TestFixture(typeof(SqlConnection), typeof(MSSQLAssetData), Description = "Basic Asset store tests (MS SQL Server)")]
    [TestFixture(typeof(SqliteConnection), typeof(SQLiteAssetData), Description = "Basic Asset store tests (SQLite)")]

    public class AssetTests<TConn, TAssetData> : BasicDataServiceTest<TConn, TAssetData>
        where TConn : DbConnection, new()
        where TAssetData : class, IAssetDataPlugin, new()
    {
        IAssetDataPlugin m_db;

        const bool COMPARE_CREATOR = true;

        public UUID uuid1 = UUID.Random();
        public UUID uuid2 = UUID.Random();
        public UUID uuid3 = UUID.Random();

        public UUID critter1 = COMPARE_CREATOR ? UUID.Random() : UUID.Zero;
        public UUID critter2 = COMPARE_CREATOR ? UUID.Random() : UUID.Zero;
        public UUID critter3 = COMPARE_CREATOR ? UUID.Random() : UUID.Zero;

        public byte[] data1 = new byte[100];

        PropertyScrambler<AssetBase> scrambler = new PropertyScrambler<AssetBase>()
                .DontScramble(x => x.ID)
                .DontScramble(x => x.Type)
                .DontScramble(x => x.FullID)
                .DontScramble(x => x.Metadata.ID)
                .DontScramble(x => x.Metadata.CreatorID)
                .DontScramble(x => x.Metadata.ContentType)
                .DontScramble(x => x.Metadata.FullID)
                .DontScramble(x => x.Data);

        protected override void InitService(object service)
        {
            m_db = (IAssetDataPlugin)service;
            m_db.Initialise(m_connStr);
        }

        [Test]
        public void T001_LoadEmpty()
        {
            Assert.That(m_db.ExistsAsset(uuid1), Is.False);
            Assert.That(m_db.ExistsAsset(uuid2), Is.False);
            Assert.That(m_db.ExistsAsset(uuid3), Is.False);
        }

        [Test]
        public void T010_StoreReadVerifyAssets()
        {
            AssetBase a1 = new AssetBase(uuid1, "asset one", (sbyte)AssetType.Texture, critter1.ToString());
            AssetBase a2 = new AssetBase(uuid2, "asset two", (sbyte)AssetType.Texture, critter2.ToString());
            AssetBase a3 = new AssetBase(uuid3, "asset three", (sbyte)AssetType.Texture, critter3.ToString());
            a1.Data = data1;
            a2.Data = data1;
            a3.Data = data1;

            scrambler.Scramble(a1);
            scrambler.Scramble(a2);
            scrambler.Scramble(a3);

            m_db.StoreAsset(a1);
            m_db.StoreAsset(a2);
            m_db.StoreAsset(a3);
            
            AssetBase a1a = m_db.GetAsset(uuid1);
            Assert.That(a1a, Constraints.PropertyCompareConstraint(a1));

            AssetBase a2a = m_db.GetAsset(uuid2);
            Assert.That(a2a, Constraints.PropertyCompareConstraint(a2));

            AssetBase a3a = m_db.GetAsset(uuid3);
            Assert.That(a3a, Constraints.PropertyCompareConstraint(a3));

            scrambler.Scramble(a1a);
            scrambler.Scramble(a2a);
            scrambler.Scramble(a3a);

            m_db.StoreAsset(a1a);
            m_db.StoreAsset(a2a);
            m_db.StoreAsset(a3a);

            AssetBase a1b = m_db.GetAsset(uuid1);
            Assert.That(a1b, Constraints.PropertyCompareConstraint(a1a));

            AssetBase a2b = m_db.GetAsset(uuid2);
            Assert.That(a2b, Constraints.PropertyCompareConstraint(a2a));

            AssetBase a3b = m_db.GetAsset(uuid3);
            Assert.That(a3b, Constraints.PropertyCompareConstraint(a3a));

            Assert.That(m_db.ExistsAsset(uuid1), Is.True);
            Assert.That(m_db.ExistsAsset(uuid2), Is.True);
            Assert.That(m_db.ExistsAsset(uuid3), Is.True);

            List<AssetMetadata> metadatas = m_db.FetchAssetMetadataSet(0, 1000);

            Assert.That(metadatas.Count >= 3, "FetchAssetMetadataSet() should have returned at least 3 assets!");

            // It is possible that the Asset table is filled with data, in which case we don't try to find "our"
            // assets there:
            if (metadatas.Count < 999)
            {
                AssetMetadata metadata = metadatas.Find(x => x.FullID == uuid1);
                Assert.That(metadata, Is.Not.Null, "Metadata not found!");
                Assert.That(metadata.Name, Is.EqualTo(a1b.Name));
                Assert.That(metadata.Description, Is.EqualTo(a1b.Description));
                Assert.That(metadata.Type, Is.EqualTo(a1b.Type));
                Assert.That(metadata.Temporary, Is.EqualTo(a1b.Temporary));
                Assert.That(metadata.FullID, Is.EqualTo(a1b.FullID));
            }

            // Finally dropping the assets:

            m_db.DeleteAsset(uuid1);
            Assert.That(m_db.ExistsAsset(uuid1), Is.False);
            Assert.That(m_db.ExistsAsset(uuid2), Is.True);
            Assert.That(m_db.ExistsAsset(uuid3), Is.True);

            m_db.DeleteAsset(uuid2);
            Assert.That(m_db.ExistsAsset(uuid2), Is.False);
            Assert.That(m_db.ExistsAsset(uuid3), Is.True);

            m_db.DeleteAsset(uuid3);
            Assert.That(m_db.ExistsAsset(uuid3), Is.False);

        }
    }
}