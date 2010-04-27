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
using NUnit.Framework.SyntaxHelpers;
using OpenMetaverse;
using OpenSim.Framework;
using log4net;

namespace OpenSim.Data.Tests
{
    public class BasicAssetTest
    {

        private const int NUM_ASSETS = 1000;
        private const int NUM_READS = 30000;
        private const int NUM_UPDATES = 1000;

        public IAssetDataPlugin db;
        public UUID uuid1;
        public UUID uuid2;
        public UUID uuid3;
        public byte[] asset1;
        PropertyScrambler<AssetBase> scrambler;

        public void SuperInit()
        {
            OpenSim.Tests.Common.TestLogging.LogToConsole();

            uuid1 = UUID.Random();
            uuid2 = UUID.Random();
            uuid3 = UUID.Random();
            asset1 = new byte[100];
            asset1.Initialize();

            scrambler = new PropertyScrambler<AssetBase>()
                .DontScramble(x => x.ID)
                .DontScramble(x => x.FullID)
                .DontScramble(x => x.Metadata.ID)
                .DontScramble(x => x.Metadata.Type)
                .DontScramble(x => x.Metadata.CreatorID)
                .DontScramble(x => x.Metadata.ContentType)
                .DontScramble(x => x.Metadata.FullID);
        }

        [Test]
        public void T001_LoadEmpty()
        {
            Assert.That(db.ExistsAsset(uuid1), Is.False);
            Assert.That(db.ExistsAsset(uuid2), Is.False);
            Assert.That(db.ExistsAsset(uuid3), Is.False);
        }

        [Test]
        public void T010_StoreSimpleAsset()
        {
            AssetBase a1 = new AssetBase(uuid1, "asset one", (sbyte)AssetType.Texture, UUID.Zero.ToString());
            AssetBase a2 = new AssetBase(uuid2, "asset two", (sbyte)AssetType.Texture, UUID.Zero.ToString());
            AssetBase a3 = new AssetBase(uuid3, "asset three", (sbyte)AssetType.Texture, UUID.Zero.ToString());
            a1.Data = asset1;
            a2.Data = asset1;
            a3.Data = asset1;

            scrambler.Scramble(a1);
            scrambler.Scramble(a2);
            scrambler.Scramble(a3);

            db.StoreAsset(a1);
            db.StoreAsset(a2);
            db.StoreAsset(a3);
            
            AssetBase a1a = db.GetAsset(uuid1);
            Assert.That(a1a, Constraints.PropertyCompareConstraint(a1));

            AssetBase a2a = db.GetAsset(uuid2);
            Assert.That(a2a, Constraints.PropertyCompareConstraint(a2));

            AssetBase a3a = db.GetAsset(uuid3);
            Assert.That(a3a, Constraints.PropertyCompareConstraint(a3));

            scrambler.Scramble(a1a);
            scrambler.Scramble(a2a);
            scrambler.Scramble(a3a);

            db.StoreAsset(a1a);
            db.StoreAsset(a2a);
            db.StoreAsset(a3a);

            AssetBase a1b = db.GetAsset(uuid1);
            Assert.That(a1b, Constraints.PropertyCompareConstraint(a1a));

            AssetBase a2b = db.GetAsset(uuid2);
            Assert.That(a2b, Constraints.PropertyCompareConstraint(a2a));

            AssetBase a3b = db.GetAsset(uuid3);
            Assert.That(a3b, Constraints.PropertyCompareConstraint(a3a));

            Assert.That(db.ExistsAsset(uuid1), Is.True);
            Assert.That(db.ExistsAsset(uuid2), Is.True);
            Assert.That(db.ExistsAsset(uuid3), Is.True);

            List<AssetMetadata> metadatas = db.FetchAssetMetadataSet(0, 1000);

            AssetMetadata metadata = metadatas.Find(x => x.FullID == uuid1);
            Assert.That(metadata.Name, Is.EqualTo(a1b.Name));
            Assert.That(metadata.Description, Is.EqualTo(a1b.Description));
            Assert.That(metadata.Type, Is.EqualTo(a1b.Type));
            Assert.That(metadata.Temporary, Is.EqualTo(a1b.Temporary));
            Assert.That(metadata.FullID, Is.EqualTo(a1b.FullID));
        }

        [Test]
        public void T020_Store1000Assets()
        {

            AssetBase[] assets = new AssetBase[NUM_ASSETS];
            
            // Get a large number of randomized assets:
            for (int i = 0; i < assets.Length; i++)
            {
                UUID uid = UUID.Random();
                AssetBase a = new AssetBase(uid, "asset one", (sbyte)AssetType.Texture, UUID.Zero.ToString());
                a.Data = asset1;
                scrambler.Scramble(a);
                assets[i] = a;
            }

            int start_time = Environment.TickCount;
            for (int i = 0; i < assets.Length; i++)
                db.StoreAsset(assets[i]);
            int end_time = Environment.TickCount;

            Console.WriteLine("Writing {0} assets takes {1} ms", assets.Length, end_time - start_time);
        }

        [Test]
        public void T030_RandomFetchTest()
        {
            Random rnd = new Random();
            int nFailed = 0;
            AssetBase a;

            List<AssetMetadata> list = db.FetchAssetMetadataSet(0, NUM_ASSETS);
            int nMax = list.Count;
            Console.WriteLine("Fetched metadata of {0} assets", nMax);

            Console.WriteLine("Starting random access test ({0} iterations)...", NUM_READS);
            int start_time = Environment.TickCount;
            for (int i = 0; i < NUM_READS; i++)
            {
                int n = rnd.Next(nMax);
                try
                {
                    a = db.GetAsset(list[n].FullID);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error after getting {0} assets: {1}", i, e.Message);
                    throw e;
                }

                if (a == null)
                {
                    Console.WriteLine("Couldn't fetch #{0}, ndx={1}: {2}", i, n, list[n].FullID);
                    nFailed++;
                }
            }
            int end_time = Environment.TickCount;

            Console.WriteLine("Randomly accessing assets {0} times took {1} ms", NUM_READS, end_time - start_time);
            
            Assert.That(nFailed == 0, String.Format("{0} assets of {1} couldn't be fetched", nFailed, nMax));
        }

        [Test]
        public void T040_RandomUpdateTest()
        {
            Random rnd = new Random();

            List<AssetMetadata> list = db.FetchAssetMetadataSet(0, NUM_ASSETS);
            int nMax = list.Count;
            Console.WriteLine("Fetched metadata of {0} assets", nMax);

            Console.WriteLine("Starting random update test ({0} iterations)...", NUM_UPDATES);
            int start_time = Environment.TickCount;

            for (int i = 0; i < NUM_UPDATES; i++)
            {
                int n = rnd.Next(nMax);
                AssetMetadata m = list[n];
                AssetBase a = new AssetBase(m.ID, m.Name, (sbyte)AssetType.Texture, UUID.Zero.ToString());
                a.Data = asset1;
                scrambler.Scramble(a);
                try
                {
                    db.StoreAsset(a);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error after updating {0} assets: {1}", i, e.Message);
                    throw e;
                }

            }
            int end_time = Environment.TickCount;

            Console.WriteLine("Randomly updating assets {0} times took {1} ms", NUM_UPDATES, end_time - start_time);
        }

        [Test]
        public void T050_RandomChecksMissing()
        {
            Random rnd = new Random();
            int nFailed = 0;

            Console.WriteLine("Starting random existance check (not in DB) ({0} iterations)...", NUM_READS);
            int start_time = Environment.TickCount;
            for (int i = 0; i < NUM_READS; i++)
            {
                bool bFound;
                UUID uid = UUID.Random();
                try
                {
                    bFound = db.ExistsAsset(uid);
                    if (bFound)
                    {
                        Console.WriteLine("Random UID {0} found in the asset table?", uid.ToString());
                        nFailed++;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error after checking {0} missing assets: {1}", i, e.Message);
                    throw e;
                }

            }
            int end_time = Environment.TickCount;

            Console.WriteLine("Randomly checking non-existing assets {0} times took {1} ms", NUM_READS, end_time - start_time);

            Assert.That(nFailed == 0, "{0} random assets found in table (although they shouldn't be there)", nFailed);
        }

        [Test]
        public void T050_RandomChecksMissingTest()
        {
            Random rnd = new Random();
            int nFailed = 0;

            List<AssetMetadata> list = db.FetchAssetMetadataSet(0, NUM_ASSETS);
            int nMax = list.Count;
            Console.WriteLine("Fetched metadata of {0} assets", nMax);

            Console.WriteLine("Starting random access test ({0} iterations)...", NUM_READS);
            int start_time = Environment.TickCount;
            for (int i = 0; i < NUM_READS; i++)
            {
                int n = rnd.Next(nMax);
                UUID uid = list[n].FullID;
                try
                {
                    if ( !db.ExistsAsset(uid) )
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

            Console.WriteLine("Randomly checking existing assets {0} times took {1} ms", NUM_READS, end_time - start_time);

            Assert.That(nFailed == 0, String.Format("{0} assets of {1} couldn't be checked", nFailed, nMax));
        }
    }
}
