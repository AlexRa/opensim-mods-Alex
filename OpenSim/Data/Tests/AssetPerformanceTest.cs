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
using log4net;
using System.Data;
using System.Data.Common;
using System.Reflection;


// DBMS-specific:
using MySql.Data.MySqlClient;
using OpenSim.Data.MySQL;

using System.Data.SqlClient;
using OpenSim.Data.MSSQL;

using Mono.Data.Sqlite;
using OpenSim.Data.SQLite;

namespace OpenSim.Data.Tests
{

    public enum ReadLevel
    {
        CheckOnly = 1,
        Meta = 2,
        Full = 3
    };

    [TestFixture(typeof(MySqlConnection), typeof(MySQLAssetData), true, true, "Server=localhost;Port=3306;Database=opensim-perf;User ID=opensim-nunit;Password=opensim-nunit;",
        Description = "MySQL Large asset performance tests (separate DB)")]
    [TestFixture(typeof(MySqlConnection), typeof(MySQLAssetData), false, true,
        Description = "MySQL Asset performance tests (KeepAlive)")]
    [TestFixture(typeof(MySqlConnection), typeof(MySQLAssetData), false, false,
        Description = "MySQL Asset performance tests (Disconnect)")]
/*
 *  Oops!  Must be SUPER to set the cache!
    [TestFixture(typeof(MySqlConnection), typeof(MySQLAssetData), false, true,
        "Server=localhost;Port=3306;Database=opensim-nunit;User ID=opensim-nunit;Password=opensim-nunit;AssetKeyCache=1M;",
        Description = "MySQL Asset performance tests (cached keys)")]
*/
    [TestFixture(typeof(SqlConnection), typeof(MSSQLAssetData), false, true,
        Description = "Asset performance tests (MS SQL Server, KeepAlive)")]
    [TestFixture(typeof(SqlConnection), typeof(MSSQLAssetData), false, false,
        Description = "Asset performance tests (MS SQL Server, Disconnecting)")]

    [TestFixture(typeof(SqliteConnection), typeof(SQLiteAssetData), false, true,
        Description = "SQLite Asset performance tests (KeepAlive)")]
    [TestFixture(typeof(SqliteConnection), typeof(SQLiteAssetData), false, false,
        Description = "SQLite Asset performance tests (Disconnecting)")]

    class AssetPerformanceTest<TConn, TAssetData> : BasicDataServiceTest<TConn, TAssetData>
        where TConn : DbConnection, new()
        where TAssetData : class, IAssetDataPlugin, new()
    {
        TAssetData m_db;
        bool m_bigDB = false;
        bool m_KeepAlive = true;

        public AssetPerformanceTest(bool bigDB, bool keepAlive, string conn)
            : base(conn)
        {
            // If TRUE, tests will be done on a separate non-disposable database, with 100000+ records.
            // The tests will behave differently, as the subset of known IDs will have to be stored separately!
            m_bigDB = bigDB;
            m_KeepAlive = keepAlive;
        }

        public AssetPerformanceTest(bool bigDB, bool keepAlive)
            : this(bigDB, keepAlive, "")
        {
        }

        PropertyScrambler<AssetBase> scrambler = new PropertyScrambler<AssetBase>()
                .DontScramble(x => x.Data)
                .DontScramble(x => x.ID)
                .DontScramble(x => x.Type)
                .DontScramble(x => x.FullID)
                .DontScramble(x => x.Metadata.ID)
                .DontScramble(x => x.Metadata.CreatorID)
                .DontScramble(x => x.Metadata.ContentType)
                .DontScramble(x => x.Metadata.FullID);

        protected override void InitService(object service)
        {
            if (!String.IsNullOrEmpty(m_connStr) && !m_connStr.EndsWith(";"))
                m_connStr += ";";

            string cs = m_connStr + String.Format("KeepAlive={0}", m_KeepAlive ? 1 : 0);
            m_db = (TAssetData)service;
            m_db.Initialise(cs);
        }

        private byte[] MakeRandomBlob(Random rnd)
        {
            byte[] data = new byte[rnd.Next(100, 10000)];
            rnd.NextBytes(data);
            return data;
        }

//      [TestCase(1000, 1000)]
        [TestCase(10000, 10000), Explicit]
//      [TestCase(100000, 1000), 

        public void T010_StoreLotsOfAssets(int nAssetCount, int nKnownIDs)
        {
            if (m_bigDB == (nAssetCount == nKnownIDs))
                Assert.Ignore();

            // create an aux table where we store a small percentage of the created asset IDs. 
            // In the following random access tests we load and use these IDs.   
            if (m_bigDB)
            {
                try
                {
                    ExecuteSql("create table some_uuids(uid char(36));");
                }
                catch
                {
                    // OK to fail if the table exists, don't want DBMS-specific "CREATE IF NOT EXISTS" syntax 
                }
            }
            else
            {
                // Clean up the table, to have a good timing
                ExecuteSql("delete from assets;");
            }

            UUID critter = UUID.Random();
            int duration = 0;
            Random rnd = new Random();

            // Get a large number of randomized assets:
            int total_start_time = Environment.TickCount;
            AssetBase asset = new AssetBase(UUID.Random(), "random asset", (sbyte)AssetType.Texture, critter.ToString());
            for (int i = 0; i < nAssetCount; i++)
            {
                asset.FullID = UUID.Random();
                scrambler.Scramble(asset);
                asset.Data = MakeRandomBlob(rnd);

                if ((i % 1000) == 0)
                    Console.WriteLine("Written {0} assets out of {1}, time to go: {2} sec",
                        i, nAssetCount, (int)((float)duration / 1000.0 / (float)i * (float)(nAssetCount - i)));
                int start_time = Environment.TickCount;
                m_db.StoreAsset(asset);
                int end_time = Environment.TickCount;
                duration += (end_time - start_time);

                if (m_bigDB)
                {
                    // add some IDs to the aux table:
                    if (rnd.Next(nAssetCount) < nKnownIDs)
                        ExecuteSql(String.Format("insert into some_uuids values ('{0}');", asset.ID));
                }
            }

            Console.WriteLine("Writing {0} assets has taken {1} sec", nAssetCount, (float)(Environment.TickCount - total_start_time) / 1000.0);
        }

        private List<UUID> LoadKnownIDs(int nMax)
        {
            List<UUID> list = new List<UUID>();
            string sql = m_bigDB ?
                "select uid from some_uuids" :
                "select id from assets";
            if (nMax > 0)
                sql = sql + " limit " + nMax.ToString(); 

            ExecQuery(sql, false,
                delegate(IDataReader reader)
                {
                    object o = reader[0];
                    if( o is Guid )
                        list.Add(new UUID((Guid)o));
                    else if (o is byte[])
                        list.Add(new UUID((byte[])o, 0));
                    else 
                        list.Add(new UUID(o.ToString()));

                    return (nMax == 0) || (list.Count < nMax);  // continue?
                }
            );
            return list;
        }


        // Note: Get() and ExistsAsset() are very similar to test. A good implementation must be
        // much faster on the 'exists' check than on full read.
        [Explicit]
        [TestCase(10000, ReadLevel.CheckOnly, true, Description = "Repeatedly check for assets (known to be there)")]
        [TestCase(10000, ReadLevel.Meta, true, Description = "Repeatedly read metadata (known to be there)")]
        [TestCase(10000, ReadLevel.Full, true, Description = "Repeatedly read assets (known to be there)")]

        [TestCase(10000, ReadLevel.Full, false, Description = "Repeatedly read assets (missing)")]
        [TestCase(10000, ReadLevel.Meta, false, Description = "Repeatedly read metadata (missing)")]
        [TestCase(10000, ReadLevel.CheckOnly, false, Description = "Repeatedly check for assets (missing)")]

        public void T020_RandomAccessTest(int nChecks, ReadLevel Level, bool bKnownIDs)
        {
            Random rnd = new Random();
            int nFailed = 0;

            List<UUID> list = null;
            int nMax = 0;
            if (bKnownIDs)
            {
                list = LoadKnownIDs(0);
                nMax = list.Count;
                Console.WriteLine("Fetched IDs of {0} assets", nMax);
            }

            string s = (Level == ReadLevel.CheckOnly) ? "AssetExists" : 
                ((Level == ReadLevel.Full) ? "GetAsset" : "GetMetadata");
            Console.WriteLine("Starting random {0} test ({1} iterations)...", s, nChecks);

            int start_time = Environment.TickCount;
            for (int i = 0; i < nChecks; i++)
            {
                UUID uid = bKnownIDs ? list[rnd.Next(nMax)] : UUID.Random();
                try
                {
                    bool isThere = false;
                    switch (Level)
                    {
                        case ReadLevel.CheckOnly:
                            isThere = m_db.ExistsAsset(uid);
                            break;
                        case ReadLevel.Meta:
                            AssetMetadata meta = m_db.GetMetadata(uid);
                            isThere = (meta != null);
                            break;
                        case ReadLevel.Full:
                            AssetBase a = m_db.GetAsset(uid);
                            isThere = (a != null);
                            break;
                    }

                    if (bKnownIDs && !isThere)
                    {
                        Console.WriteLine("UID {0} not found in the asset table", uid.ToString());
                        nFailed++;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error after checking {0} assets: {1}", i, e.Message);
                    throw e;
                }

            }
            int end_time = Environment.TickCount;

            Console.WriteLine("Randomly checking existing assets {0} times took {1} sec", nChecks, (double)(end_time - start_time) / 1000.0);

            Assert.That(nFailed, Is.EqualTo(0), String.Format("{0} assets of {1} couldn't be checked", nFailed, nMax));
        }


        [Explicit]
        [TestCase(10000, true, Description = "Updating existing assets")]
        [TestCase(10000, false, Description = "Updating new assets")]

        public void T030_RandomUpdateTest(int nUpdates, bool bKnownIDs)
        {
            Random rnd = new Random();

            List<UUID> list = null;
            int nMax = 0;
            if (bKnownIDs)
            {
                list = LoadKnownIDs(0);
                nMax = list.Count;
                Console.WriteLine("Fetched metadata of {0} assets", nMax);
            }

            Console.WriteLine("Starting random update test ({0} iterations)...", nUpdates);
            int duration = 0;

            for (int i = 0; i < nUpdates; i++)
            {
                UUID uid = bKnownIDs ? list[rnd.Next(nMax)] : UUID.Random();
                AssetBase a = new AssetBase(uid, "Some name", (sbyte)AssetType.Texture, UUID.Zero.ToString());
                scrambler.Scramble(a);
                a.Data = MakeRandomBlob(rnd);
                try
                {
                    int start_time = Environment.TickCount;
                    m_db.StoreAsset(a);
                    int end_time = Environment.TickCount;
                    duration += end_time - start_time;
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error after updating {0} assets: {1}", i, e.Message);
                    throw e;
                }

            }

            Console.WriteLine("Randomly updating {0} assets {1} times took {2} ms", 
                bKnownIDs ? "existing" : "missing", nUpdates, duration);
        }
    }
}
