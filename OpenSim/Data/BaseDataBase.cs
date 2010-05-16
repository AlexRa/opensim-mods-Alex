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
using System.Data;
using System.Data.Common;
using System.Reflection;
using OpenSim.Framework;
using OpenMetaverse;
using log4net;
using System.Text.RegularExpressions;



namespace OpenSim.Data
{

    public class TableAttribute : Attribute
    {
        public string TableName;

        public TableAttribute(string table)
        {
            TableName = table;
        }
    }

    /// <summary>This is the base class for all DB connectors. You derive from it classes
    /// such as AssetDataBase which implement specific interfaces in as much as possible
    /// DBMS-independent way. From that, we further derive DBMS-specific versions where
    /// the rest of the stuff is implemented (only then the generic TConn is resolved).
    /// </summary>
    /// <typeparam name="TDataSpec">The DBMS-specific class to be used by this connector</typeparam>
    /// 
    public class BaseDataBaseEx<TDataSpec> : BaseDataBase
        where TDataSpec : DataSpecificBase, new()   
    {
        public BaseDataBaseEx() :
            base(new TDataSpec())
        {
        }
    }

    public partial class BaseDataBase : IDataPlugin
    {
        protected static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        protected string m_connStr = "";
        protected bool m_keepAlive = true;
        protected DbConnection m_conn = null;
        protected List<Cmd> m_cmds = new List<Cmd>();
        protected object m_dbLock = new object();
        protected readonly DataSpecificBase m_DBMS = null;

        public DataSpecificBase DBMS { get { return m_DBMS; } }

        /* One DataPlugin class can work with several tables. We scan table info on demand from individual commands
         * and do it only once.  We have to scan table structure for two reasons: 
         * 
         * (1) It is necessary right now to infer the parameter types, which in turn is required for Prepare 
         *     (at least on some databases - MS SQL)
         * 
         * (2) It can be also used somehow to bind query results (something like in MSSQLGenericTableHandler)
         *   
         *  NOTE that we scan table *columns*, not query parameters, but the assumption is that in all queries 
         *  the parameter names match their respective columns. So when a Cmd needs to find out the in-DB parameter
         *  type, we just look up a column with the same name.
         *  
         * We keep the scanned table structure in a raw form, as a DataTable returned from reader.GetSchemaTable().
         * This way, getting the schema data is DBMS-independent, but interpreting it is not.
         */
        protected Dictionary<string, DataTable> m_knownTables = new Dictionary<string, DataTable>();


        public delegate bool ProcessRow(IDataReader reader);

        protected BaseDataBase(DataSpecificBase dbms)
        {
            m_DBMS = dbms;
        }

        protected virtual string GetMigrationStore()
        {
            throw new NotImplementedException();
        }

        public bool KeepAlive
        {
            get { return m_keepAlive; }
            set
            {
                m_keepAlive = value;
                if (!value)
                    Disconnect();
            }
        }

        protected virtual void Connect()
        {
            if( m_conn == null )
                m_conn = m_DBMS.GetNewConnection(this, m_connStr);

            if (m_conn.State != ConnectionState.Open)
            {
                m_conn.Open();
            }
        }

        public void Disconnect()
        {
            foreach(Cmd cmd in m_cmds)
                cmd.Disconnect();

            if( m_conn != null )
            {
                m_conn.Dispose();
                m_conn = null;
            }
        }

        // For running misc one-time commands:
        protected void ExecSQL(string sql)
        {
            Connect();
            try
            {
                using (DbCommand cmd = m_conn.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                }
                if (!m_keepAlive)
                    Disconnect();
            }
            catch (Exception e)
            {
                Disconnect();
                throw e;
            }
        }

        protected DataTable GetTableSchema(string sTable)
        {
            DataTable schema;
            if (m_knownTables.ContainsKey(sTable) && ((schema = m_knownTables[sTable]) != null))
                return schema;

            if (m_conn == null)
                throw new Exception("Internal error: GetTableSchema() must only be called when the connection is already open");

            using (DbCommand cmd = m_conn.CreateCommand())
            {
                cmd.CommandText = "SELECT * FROM " + sTable;
                using (DbDataReader rdr = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
                {
                    schema = rdr.GetSchemaTable();
                }
            }
            m_knownTables[sTable] = schema;

            return schema;
        }



        #region IDataPlugin Members

        public virtual void Flush()
        {
            // Do nothing here
        }

        public virtual string Version
        {
            get
            {
                Version ver = GetType().Assembly.GetName().Version; 
                return String.Format("{0}.{1}.{2}.{3}", ver.Major, ver.Minor, ver.Build, ver.Revision);
            }
        }

        public virtual string Name
        {
            get { return GetType().Name; }
        }

        public virtual void Initialise()
        {
            m_log.Info("[{0}]: " + Name + " cannot be default-initialized!");
            throw new PluginNotInitialisedException(Name);
        }

        public string ExtractConnParam(ref string conn, string name)
        {
            // quick pre-check:
            if (conn.IndexOf(name, StringComparison.InvariantCultureIgnoreCase) < 0)
                return "";

            string prefix = name + "=";
            string val = "";
            string[] a = conn.Split(';');
            conn = "";
            foreach (string s in a)
            {
                if (s.StartsWith(prefix, StringComparison.InvariantCultureIgnoreCase))
                    val = s.Substring(prefix.Length);
                else
                    conn += s + ";";
            }
            return val;
        }

        public bool TryConnParamToBool(string val, ref bool res)
        {
            if (val == "1" || val == "true" || val == "yes")
            {
                res = true;
                return true;
            }

            if (val == "0" || val == "false" || val == "no")
            {
                res = false;
                return true;
            }

            return false;
        }

        public virtual void Initialise(string connect)
        {

            // Extract KeepAlive={true|1|yes|false|0|no}, if present, from the conn string
            string ka = ExtractConnParam(ref connect, "KeepAlive");
            if (ka != "")
                TryConnParamToBool(ka, ref m_keepAlive); 

            m_connStr = connect;
            using (DbConnection connection = m_DBMS.GetNewConnection(this, m_connStr))
            {
                connection.Open();
                Assembly assem = GetType().Assembly;
                Migration migration = m_DBMS.GetMigration();
                migration.Initialize(connection, assem, GetMigrationStore(), "");
                migration.Update();
            }

            LocateTableAttributes();
        }

        /// <summary>The entire data plugin class or a specific Cmd field inside it can be prefixed
        /// with a Table(...) attribute, which names the table(s) this plugin works with. The table
        /// names are used to obtain the schemas (most importantly column info).
        /// 
        /// This method extracts the class-level and Cmd-level table name attributes and stores them 
        /// into the Cmds (class-level table name(s) only work as a default).
        /// 
        /// 
        /// </summary>
        protected void LocateTableAttributes()
        {
            Attribute class_attr = Attribute.GetCustomAttribute(GetType(), typeof(TableAttribute), true);
            FieldInfo[] fields = GetType().GetFields(BindingFlags.Instance | BindingFlags.Public);
            foreach (FieldInfo f in fields)
            {
                if (f.FieldType != typeof(Cmd)) continue;

                Attribute fld_attr = Attribute.GetCustomAttribute(f, typeof(TableAttribute), true);
                if ( fld_attr == null )
                    fld_attr = class_attr;

                if (fld_attr == null)
                    continue;

                ((Cmd)(f.GetValue(this))).TableName = ((TableAttribute)fld_attr).TableName; 
            }
        }


        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            Flush();
            Disconnect();
        }

        #endregion
    }
}
