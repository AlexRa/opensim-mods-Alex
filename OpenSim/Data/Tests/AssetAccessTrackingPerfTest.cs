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

    [TestFixture(Description = "MySQL Asset access tracking")]
    public class MySqlAssetAccessTrackingTests: AssetTrackingPerfTest<MySqlConnection, MySQLAssetData>
    {
        [Test]
        public void T010_StoreAssets_1000()
        {
            StoreLotsOfAssets(1000);
        }

        [Test]
        public void T020_AccessTracking_None_500_of_1000()
        {
            m_db.AccessTrackMode = AccessTrackMode.None;
            RandomAccessTest(1000, 10000, 500, AccessTrackMode.None, true);
        }

        [Test]
        public void T030_AccessTracking_Smart_500_of_1000()
        {
            m_db.AccessTrackMode = AccessTrackMode.FastUpdate;
            m_db.AccessTrackDelay = 1;
            RandomAccessTest(1000, 10000, 500, AccessTrackMode.FastUpdate, true);
        }

        [Test]
        public void T040_AccessTracking_Smart_5sec()
        {
            m_db.AccessTrackMode = AccessTrackMode.FastUpdate;
            m_db.AccessTrackDelay = 5;
            RandomAccessTest(1000, 10000, 500, AccessTrackMode.FastUpdate, true);
        }

        [Test]
        public void T050_AccessTracking_Smart_50sec()
        {
            m_db.AccessTrackMode = AccessTrackMode.FastUpdate;
            m_db.AccessTrackDelay = 50;
            RandomAccessTest(1000, 10000, 500, AccessTrackMode.FastUpdate, true);
            RandomAccessTest(1000, 10000, 500, AccessTrackMode.FastUpdate, false);  // 2nd time faster
        }

        [Test]
        public void T055_AccessTracking_Logged()
        {
            m_db.AccessTrackMode = AccessTrackMode.FastLog;
            m_db.AccessTrackDelay = 1;
            RandomAccessTest(1000, 10000, 500, AccessTrackMode.FastLog, true);
        }

        [Test, Explicit("Very slow")]
        public void T060_AccessTracking_Update_3sec()
        {
            m_db.AccessTrackMode = AccessTrackMode.UpdateFromCode;
            m_db.AccessTrackDelay = 3;
            RandomAccessTest(1000, 10000, 500, AccessTrackMode.UpdateFromCode, true);
        }

        [Test, Explicit("Very slow")]
        public void T070_AccessTracking_Update_50sec()
        {
            m_db.AccessTrackMode = AccessTrackMode.UpdateFromCode;
            m_db.AccessTrackDelay = 50;
            RandomAccessTest(1000, 10000, 500, AccessTrackMode.UpdateFromCode, true);
        }
    }


    public class AssetTrackingPerfTest<TConn, TAssetData> : BasicDataServiceTest<TConn, TAssetData>
        where TConn : DbConnection, new()
        where TAssetData : class, IAssetDataPlugin, new()
    {

        public TAssetData m_db;

        // If TRUE, tests will be done on a separate non-disposable database, with 100000+ records.
        // The tests will behave differently, as the subset of known IDs will have to be stored separately!
        bool m_CleanUp = false;

        bool m_KeepAlive = true;    // (not used at this time)

        public AssetTrackingPerfTest(string conn, bool keepAlive, bool withCleanUp)
            : base(conn)
        {
            m_CleanUp = withCleanUp;
            m_KeepAlive = keepAlive;
        }

        public AssetTrackingPerfTest()
            : this("", true, false)
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

            string cs = m_connStr; // + String.Format("KeepAlive={0}", m_KeepAlive ? 1 : 0); // not yet
            m_db = (TAssetData)service;
            m_db.Initialise(cs);
        }

        private byte[] MakeRandomBlob(Random rnd)
        {
            byte[] data = new byte[rnd.Next(100, 10000)];
            rnd.NextBytes(data);
            return data;
        }

        protected void ResetAccessLog(AccessTrackMode tracking)
        {
            string sLogTable = (tracking == AccessTrackMode.FastLog) ? "asset_access_log1" : "asset_access_log";
            ExecuteSql("delete from " + sLogTable);
        }

        /// <summary>Store a large number of randomized assets. We either add to existing
        /// assets, or clear the DB first.
        /// 
        /// </summary>
        /// <param name="nAssetCount"></param>
 
        public int StoreLotsOfAssets(int nAssetCount)
        {
            int nRows = 0;

            if( m_CleanUp )
            {
                // Clean up the table, to have a good timing
                ExecuteSql("delete from assets;");
            }
            else
            {
                ExecQuery("select count(*) from assets", true, 
                    delegate(IDataReader reader)
                    {
                        nRows = Convert.ToInt32(reader[0]);
                        return false;
                    }
                );
            }

            if (nRows >= nAssetCount)
            {
                m_log.InfoFormat("Already have {0} assets in the DB", nRows);
                return nRows;
            }

            string critter = UUID.Random().ToString();
            int duration = 0;
            Random rnd = new Random();
            int total_start_time = Environment.TickCount;

            AssetBase asset = new AssetBase(UUID.Random(), "random asset", (sbyte)AssetType.Texture, critter);
            int nDone = 0, nToAdd = nAssetCount - nRows;
            for (int i = nRows; i < nAssetCount; i++)
            {
                asset.FullID = UUID.Random();
                scrambler.Scramble(asset);
                asset.Data = MakeRandomBlob(rnd);

                int start_time = Environment.TickCount;
                m_db.StoreAsset(asset);
                int end_time = Environment.TickCount;
                duration += (end_time - start_time);
                nDone++;

                if ((nDone > 0) && ((nDone % 1000) == 0))
                {
                    int start_log = Environment.TickCount;
                    Console.WriteLine("Written {0} assets out of {1}, time to go: {2} sec",
                        nDone, nToAdd, (int)((float)duration / 1000.0 / (float)nDone * (float)(nToAdd - nDone)));
                    total_start_time += (Environment.TickCount - start_log);
                }
            }
            int total_end_time = Environment.TickCount;

            Console.WriteLine("Writing {0} assets has taken {1} sec ({2} sec for Store() only)", 
                nAssetCount, (float)(Environment.TickCount - total_start_time) / 1000.0, (float)duration / 1000.0);

            return nAssetCount;
        }

        /// <summary>Load a subset of "good" IDs of the assets we are goind to fetch. 
        /// </summary>
        /// <param name="nMax"></param>
        /// <returns></returns>
        private List<UUID> LoadKnownIDs(int nMax)
        {
            List<UUID> list = new List<UUID>();

            // Note asset names are randomized, so we order by name and, supposedly,
            // return a random subset (rather than, say, the first N in the order they were added).
            // Probably doesn't any difference anyway, though.
            string sql = "select id from assets order by name";
            if (nMax > 0)
                sql += " limit " + nMax.ToString(); 

            ExecQuery(sql, false,
                delegate(IDataReader reader)
                {
                    list.Add(DBGuid.FromDB(reader[0]));
                    return true;
                }
            );
            return list;
        }

        /// <summary>Set the same access time on all assets, return it so we can check later.
        /// </summary>
        /// <returns></returns>
        private int ResetAccessTimes()
        {
            int time = 0;
            // (the access time is set at T-100sec to make sure our updates will work
            ExecuteSql("update assets set access_time = UNIX_TIMESTAMP() - 100");
            ExecQuery("select access_time from assets limit 1", true,
                delegate(IDataReader reader)
                {
                    time = Convert.ToInt32(reader[0]);
                    return false;
                }
            );

            return time;
        }

        private Dictionary<int, int> GetAccessTimes()
        {
            Dictionary<int, int> times = new Dictionary<int, int>();
            ExecQuery("select access_time, count(*) from assets group by access_time", false,
                delegate(IDataReader reader)
                {
                    int time = Convert.ToInt32(reader[0]);
                    int count = Convert.ToInt32(reader[1]);
                    times[time] = count;
                    return true;
                }
                );
            return times;
        }

        private int CountAccessLog(AccessTrackMode tracking)
        {
            int nCount = 0;
            string s = (tracking == AccessTrackMode.FastUpdate) ? "asset_access_log" : "asset_access_log1";
            ExecQuery("select count(*) from " + s, true,
                delegate(IDataReader reader) { nCount = Convert.ToInt32(reader[0]); return false; });
            return nCount;
        }

        protected void RandomAccessTest(int nAssets, int nChecks, int nKnownIDs, AccessTrackMode tracking, bool align_times)
        {
            Random rnd = new Random();
            int nFailed = 0;
            Dictionary<UUID, bool> done_list = new Dictionary<UUID,bool>(nKnownIDs);

            int nTotalAssets = StoreLotsOfAssets(nAssets);

            if (align_times && (tracking >= AccessTrackMode.FastUpdate))
                ResetAccessLog(tracking);

            List<UUID> list = LoadKnownIDs(nKnownIDs);
            int nMax = list.Count;
            m_log.InfoFormat("Fetched IDs of {0} assets", nMax);

            int initial_timestamp = ResetAccessTimes();

            string s;
            switch (tracking)
            {
                case AccessTrackMode.UpdateFromCode:
                    s = "separate UPDATE";
                    break;
                case AccessTrackMode.FastUpdate:
                    s = "SMART LOG";
                    break;
                case AccessTrackMode.FastLog:
                    s = "FAST LOG";
                    break;
                default:
                    s = "NO TRACKING";
                    break;
            }
            m_log.InfoFormat("Starting access tracking test ({0}, {1} iterations)...", s, nChecks);

            int start_time = Environment.TickCount;
            for (int i = 0; i < nChecks; i++)
            {
                UUID uid = list[rnd.Next(nMax)];
                done_list[uid] = true;
                try
                {
                    AssetBase a = m_db.GetAsset(uid);
                    if (a == null)
                    {
                        m_log.WarnFormat("UID {0} not found in the asset table", uid.ToString());
                        nFailed++;
                    }
                }
                catch (Exception e)
                {
                    m_log.ErrorFormat("Error on ID {0} after checking {1} assets: {2}", uid.ToString(), i, e.Message);
                    throw e;
                }

            }
            int end_time = Environment.TickCount;

            m_log.InfoFormat("Result for {0} test: {1} sec", nChecks, (double)(end_time - start_time) / 1000.0);

            Assert.That(nFailed == 0, String.Format("{0} assets of {1} couldn't be checked", nFailed, nMax));

            if (tracking == AccessTrackMode.None)
                return;

            if (tracking >= AccessTrackMode.FastUpdate)
            {
                int nLogged = CountAccessLog(tracking);
                Assert.That(nLogged > 0, "None of the asset accesses are logged!");

                int nExpectedLogSize = (tracking == AccessTrackMode.FastUpdate) ? done_list.Count : nChecks;
                Assert.That(nLogged == nExpectedLogSize, "Not all accessed assets are logged!");

                (m_db as MySQLAssetData).Flush();

                nLogged = CountAccessLog(tracking);

                Assert.That(nLogged == 0, "The asset log must be empty now!");
            }

            Dictionary<int, int> times = GetAccessTimes();

            Assert.That(times.ContainsKey(initial_timestamp), "Error: all assets seem to be touched!");

            int nUntouched = times[initial_timestamp];
            int nTouched = nTotalAssets - nUntouched;

            Assert.That(nTouched > 0, "No assets have been touched!");
            Assert.That(nTouched == done_list.Count, "Not all accessed assets are touched!");
        }

    }
}
