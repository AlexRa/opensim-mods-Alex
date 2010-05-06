using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;
using NUnit.Framework;
using System.Reflection;

using MySql.Data.MySqlClient;
using OpenSim.Data.MySQL;
using System.Data.SqlClient;
using Mono.Data.Sqlite;

namespace OpenSim.Data.Tests
{
    [TestFixture(typeof(MySqlConnection), typeof(MySqlMigration), "MySQL")]
    [TestFixture(typeof(SqlConnection), typeof(Migration), "MSSQL")]
    [TestFixture(typeof(SqliteConnection), typeof(Migration), "SQLite")]

    public class MigrationTests<TConn, TMigr> : BasicDataServiceTest<TConn, object> 
        where TConn: DbConnection, new()
        where TMigr : Migration, new()
    {
        public string m_DbName;

        public MigrationTests(string sDbName, string sConnString) : base(sConnString)
        {
            m_DbName = sDbName;
        }

        public MigrationTests(string sDbName)
            : this(sDbName, null)
        {
        }

        protected void ResetStoreToVersion(String store, int ver)
        {
            string sql = String.Format("delete from migrations where name = '{0}'", store);
            ExecuteSql(sql);

            if (ver > 0)
            {
                sql = String.Format("insert into migrations values('{0}', {1})", store, ver);
                ExecuteSql(sql);
            }
        }

        protected void DropTarget()
        {
            ExecuteSql("drop table if exists `version_test`");
        }

        protected void CreateTarget()
        {
            ExecuteSql(
                @"create table if not exists version_test
                (
	                id char(36) not null primary key,
	                magic int,
	                v1 int,
	                v2 int,
	                v3 int,
	                v4 int,
	                v5 int,
	                v6 int
                );"
                );
        }

        protected void ResetTarget(int nCurrValue, int nNextVal)
        {
            CreateTarget();
            ExecuteSql("delete from version_test;");
            ExecuteSql(String.Format("insert into version_test values('test', {0}, {1}, {1}, {1}, {1}, {1}, {1});",
                nNextVal, nCurrValue));
        }

        protected int GetVersion(string store)
        {
            int ver = -1;
            ExecQuery(String.Format("select version from migrations where name = '{0}';", store), true,
                delegate(IDataReader reader)
                {
                    ver = reader.GetInt32(0);
                    return false;
                }
                );
            return ver;
        }

        protected int[] GetValues()
        {
            int[] v = new int[6];
            ExecQuery("select v1, v2, v3, v4, v5, v6 from version_test", true,
                delegate(IDataReader rdr)
                {
                    v[0] = rdr.GetInt32(0);
                    v[1] = rdr.GetInt32(1);
                    v[2] = rdr.GetInt32(2);
                    v[3] = rdr.GetInt32(3);
                    v[4] = rdr.GetInt32(4);
                    v[5] = rdr.GetInt32(5);
                    return false;
                }
            );
            return v;
        }

        private void StartFrom(string store, int nVer)
        {
            if (nVer == 0)
                DropTarget();
            else
                ResetTarget(0, 1);

            ResetStoreToVersion(store, nVer);    // should have only V4..V6 updated
        }


        private void RunMigrations(string store)
        {
            Assembly assem = GetType().Assembly;
            using (DbConnection dbcon = Connect())
            {
                Migration m = new TMigr();
                m.Initialize(dbcon, assem, store, "");
                m.Update();
            }
        }

        private void CheckResults(string store, int nTargetVer, params int[] cols)
        {
            Assert.That((GetVersion(store) == nTargetVer), "Version number is not correctly updated");
            int[] vals = GetValues();
            for (int i = 0; i < vals.Length; i++)
            {
                int v = (i < cols.Length) ? cols[i] : 0;
                Assert.That(v == vals[i], "Target table is not correctly updated (column V{0})", i+1);
            }
        }

        /*  Note: all tests below work upon the same table [version_test], however each time it is  
         * updated as a part of a different "store". The migration files in the "Resources/" directory
         * are named accordingly.
         * 
         */

        [TestCase("OldStyle", 0, new int[] {1, 1, 1}, Description="Old-style migrations (from scratch)")]
        [TestCase("OldStyle", 1, new int[] {0, 1, 1}, Description = "Old-style migrations (partial)")]
        [TestCase("OldStyle", 3, new int[] {0, 0, 0}, Description = "Old-style migrations (all skipped)")]
        [TestCase("TestStore", 0, new int[]{1, 1, 1, 1, 1, 1}, Description = "New-style migrations (from scratch)")]
        [TestCase("TestStore", 3, new int[]{0, 0, 0, 1, 1, 1}, Description = "New-style migrations, (partial)")]
        [TestCase("MixedTest", 0, new int[] { 1, 1, 1, 1, 333}, Description = "Mixed-style migrations (from scratch)")]
        [TestCase("MixedTest", 2, new int[] { 0, 0, 1, 1, 333}, Description = "Mixed-style migrations (mix)")]
        [TestCase("MixedTest", 4, new int[] { 0, 0, 0, 0, 333}, Description = "Mixed-style migrations (only extra file)")]
        [TestCase("MixedTest", 5, new int[] { 0, 0, 0, 0, 0}, Description = "Mixed-style migrations (all skipped)")]
        public void T010_RegularTests(string store, int nStartVer, int[] cols)
        {
            int nFinalVer = cols.Length;
            StartFrom(store, nStartVer);
            RunMigrations(store);
            CheckResults(store, nFinalVer, cols);
        }

        [Test, Description("'Numbered' naming of migration scripts")]
        public void T020_NumberedNames()
        {
            // We have 3 scripts ending in 'migrations', 'migrations.2' and 'migrations.3'.
            // If there are "numbered" migration files, only the one with the highest number
            // should be executed
            string store = "MigrNumbered";     // MySQL and MSSQL will have different syntax

            ResetStoreToVersion(store, 0);
            ResetTarget(0, 0);                  // the script doesn't create the table, so create it here
            RunMigrations(store);
            CheckResults(store, 3, 1, 2, 3);
        }

        [Test, Description("Using :GO to define procs")]
        public void T030_DefiningProcs()
        {
            //  The script contains several stored funcs, separated by :GO 
            //  The syntax id DBMS-specific, so we have separate scripts for each supported DB
            string store = "MigrProcs_" + m_DbName;     // MySQL and MSSQL will have different syntax

            ResetStoreToVersion(store, 0);
            ResetTarget(0, 0);
            RunMigrations(store);

            // if we get here without exception, we are probably OK, but just to make sure
            // the script finally updates our test table with the values from those functions
            CheckResults(store, 3, 1, 2, 3);
        }
    }
}
