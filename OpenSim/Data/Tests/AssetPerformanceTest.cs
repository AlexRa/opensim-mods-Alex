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
    [TestFixture(typeof(MySqlConnection), typeof(MySQLAssetData), Description = "Asset performance tests (MySQL)")]
    [TestFixture(typeof(SqlConnection), typeof(MSSQLAssetData), Description = "Asset performance tests (MS SQL Server)")]
    [TestFixture(typeof(SqliteConnection), typeof(SQLiteAssetData), Description = "Asset performance tests (SQLite)")]

    class AssetPerformanceTest<TConn, TAssetData> : BasicDataServiceTest<TConn, TAssetData>
        where TConn : DbConnection, new()
        where TAssetData : AssetDataBase, new()
    {
        TAssetData m_db;

        public enum ReadLevel
        {
            CheckOnly = 1,
            Meta = 2,
            Full = 3
        };

        PropertyScrambler<AssetBase> scrambler = new PropertyScrambler<AssetBase>()
                .DontScramble(x => x.Data)
                .DontScramble(x => x.ID)
                .DontScramble(x => x.FullID)
                .DontScramble(x => x.Metadata.ID)
                .DontScramble(x => x.Metadata.CreatorID)
                .DontScramble(x => x.Metadata.ContentType)
                .DontScramble(x => x.Metadata.FullID);

        protected override void InitService(object service)
        {
            m_db = (TAssetData)service;
            m_db.Initialise(m_connStr);
        }

        private byte[] MakeRandomBlob(Random rnd)
        {
            byte[] data = new byte[rnd.Next(100, 10000)];
            rnd.NextBytes(data);
            return data;
        }

        [TestCase(100000, 1000), Explicit]
        public void T010_StoreLotsOfAssets(int nAssetCount, int nKnownIDs)
        {
            // create an aux table where we store a small percentage of the created asset IDs. 
            // In the following random access tests we load and use these IDs.   
            try
            {
                ExecuteSql("create table some_uuids(uid char(36));");
            }
            catch
            {
                // OK to fail if the table exists, don't want DBMS-specific "CREATE IF NOT EXISTS" syntax 
            }

            UUID critter = UUID.Random();
            int duration = 0;
            Random rnd = new Random();

            // Get a large number of randomized assets:
            AssetBase asset = new AssetBase(UUID.Random(), "random asset", (sbyte)AssetType.Texture, critter.ToString());
            for (int i = 0; i < nAssetCount; i++)
            {
                asset.ID = UUID.Random().ToString();
                scrambler.Scramble(asset);
                asset.Data = MakeRandomBlob(rnd);

                if ((i % 1000) == 0)
                    Console.WriteLine("Written {0} assets out of {1}, time to go: {2} sec",
                        i, nAssetCount, (int)((float)duration / 1000.0 / (float)i * (float)(nAssetCount - i)));
                int start_time = Environment.TickCount;
                m_db.StoreAsset(asset);
                int end_time = Environment.TickCount;
                duration += (end_time - start_time);

                // add some IDs to the aux table:
                if( rnd.Next(nAssetCount) < nKnownIDs )
                    ExecuteSql(String.Format("insert into some_uuids values ('{0}');", asset.ID));

            }

            Console.WriteLine("Writing {0} assets has taken {1} sec", nAssetCount, (float)duration / 1000.0);
        }

        private List<UUID> LoadKnownIDs(int nMax)
        {
            List<UUID> list = new List<UUID>();
            ExecQuery("select uid from some_uuids", false,
                delegate(IDataReader reader)
                {
                    list.Add(new UUID(reader.GetString(0)));
                    return (nMax == 0) || (list.Count < nMax);  // continue?
                }
            );
            return list;
        }


        // Note: Get() and ExistsAsset() are very similar to test. A good implementation must be
        // much faster on the 'exists' check than on full read.
        [Explicit, TestCase(1000, (int)ReadLevel.CheckOnly, true, Description = "Repeatedly check for assets (known to be there)")]
        //        [TestCase(1000, ReadLevel.Meta, true, Description = "Repeatedly read metadata (known to be there)")]
        [Explicit, TestCase(1000, (int)ReadLevel.Full, true, Description = "Repeatedly read assets (known to be there)")]

        [Explicit, TestCase(1000, (int)ReadLevel.Full, false, Description = "Repeatedly read assets (missing)")]
        //        [TestCase(1000, ReadLevel.Meta, false, Description = "Repeatedly read metadata (missing)")]
        [Explicit, TestCase(1000, (int)ReadLevel.CheckOnly, false, Description = "Repeatedly check for assets (missing)")]

        public void T020_RandomAccessTest(int nChecks, int lvl, bool bKnownIDs)
        {
            Random rnd = new Random();
            int nFailed = 0;
            ReadLevel Level = (ReadLevel)lvl;

            List<UUID> list = null;
            int nMax = 0;
            if (bKnownIDs)
            {
                list = LoadKnownIDs(0);
                nMax = list.Count;
                Console.WriteLine("Fetched metadata of {0} assets", nMax);
            }

            Console.WriteLine("Starting random access test ({0} iterations)...", nChecks);
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
                            // not impl in connectors?
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

            Console.WriteLine("Randomly checking existing assets {0} times took {1} ms", nChecks, end_time - start_time);

            Assert.That(nFailed == 0, String.Format("{0} assets of {1} couldn't be checked", nFailed, nMax));
        }


        [Explicit, TestCase(1000, true, Description = "Updating existing assets")]
        [Explicit, TestCase(1000, false, Description = "Updating new assets")]

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
