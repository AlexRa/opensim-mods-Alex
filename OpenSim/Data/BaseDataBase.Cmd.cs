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
    public partial class BaseDataBase
    {

        public class Cmd : IDisposable
        {
            BaseDataBase m_owner;
            string m_sql;
            DbCommand m_cmd = null;
            string[] m_parms = null;    // Parameter names, extracted from the SQL
            Type[] m_partypes;
            string m_table;
            static private readonly Regex rex_params = new Regex(@"@(\w+)");        //(@"^[^@]+(?:@(\w+)[^@]*)*$"); 

            public Cmd(BaseDataBase owner, string sql, params Type[] types)
            {
                m_owner = owner;
                m_owner.m_cmds.Add(this);
                m_sql = sql;
                m_partypes = types;
                ExtractParams();
            }

            public string TableName
            {
                get { return m_table; }
                set { m_table = value; }
            }

            /// <summary>Extract names of input parameters from the SQL. On this level,
            /// we use '@name' syntax for the params, regardless of the DB. Later a DataSpecificBase.Prepare()
            /// will be able to substitute the '@name' with whatever syntax supported by the specific DB
            /// NOTE: the regexp used in the code does NOT check if the '@' is inside a string literal! 
            /// Anything that looks like a parameter gets extracted! This shouldn't be a problem with the 
            /// kind of SQL we expect to use (SELECT/UPDATE/INSERT with parameters rather than literal values)
            /// </summary>
            private void ExtractParams()
            {
                MatchCollection ms = rex_params.Matches(m_sql);     // grab any '@id' params from the SQL
                List<string> lst = new List<string>(ms.Count);

                foreach (Match m in ms)
                {
                    string sName = m.Groups[1].Value;
                    if (!lst.Contains(sName))
                        lst.Add(sName);
                }

                int n = lst.Count;
                if (m_partypes.Length != n)
                    throw new Exception(String.Format("Invalid SQL Cmd: {0} types for {1} params", m_partypes.Length, lst.Count));

                m_parms = new string[n];
                for (int i = 0; i < n; i++)
                    m_parms[i] = lst[i];
            }

            public void Connect()
            {
                if (m_cmd != null)
                    return;

                m_owner.Connect();

                DataTable schema = null;
                try
                {
                    if (!String.IsNullOrEmpty(m_table))
                        schema = m_owner.GetTableSchema(m_table);
                }
                catch (Exception e)
                {
                    Disconnect();
                    throw e;
                }

                m_cmd = m_owner.m_conn.CreateCommand();
                m_cmd.CommandText = m_sql;

                DBMS.Prepare(m_cmd, m_parms, m_partypes, schema);
            }

            public void Disconnect()
            {
                if (m_cmd == null)
                    return;
                m_cmd.Dispose();
                m_cmd = null;
            }

            private void setParams(object[] parms)
            {
                int nPars = m_cmd.Parameters.Count;
                if ((parms != null ? parms.Length : 0) != nPars)
                    throw new Exception(String.Format("This query must be called with {0} parameters", nPars));

                for (int i = 0; i < nPars; i++)
                    DBMS.SetParamValue(m_cmd.Parameters[i], parms[i]); 
            }

            private bool TryReconnect(Exception e, object[] parms)
            {
                if (!DBMS.NeedReconnect(m_cmd, e))
                    return false;

                m_log.WarnFormat("[{0}] Trying to reconnect after error: {1}", m_owner.Name, e.Message);
                m_owner.Disconnect();   // close connection and all commands,
                Connect();          // reopen conn and this cmd. This resets params,
                setParams(parms);   // so they need be re-applied
                return true;
            }

            public int Query(ProcessRow action, bool bSingleRow, params object[] parms)
            {
                int nRecs = 0;
                bool was_open = (m_owner.m_conn != null);   // existing conn -> try to reconnect on 1st failure
                CommandBehavior cb = bSingleRow ? CommandBehavior.SingleRow : CommandBehavior.Default;
                DbDataReader rdr = null;

                lock (m_owner.m_dbLock)
                {
                    Connect();
                    try
                    {
                        try
                        {
                            setParams(parms);
                            try
                            {
                                rdr = m_cmd.ExecuteReader(cb);
                            }
                            catch (Exception e)
                            {
                                // might be an expired connection? 
                                if (was_open && TryReconnect(e, parms))
                                {
                                    // retry once more
                                    rdr = m_cmd.ExecuteReader(cb);
                                }
                                else
                                    throw e;
                            }

                            while (rdr.Read())
                            {
                                nRecs++;
                                if (!action(rdr))
                                    break;
                            }

                            if (!m_owner.m_keepAlive)
                                m_owner.Disconnect();
                        }
                        finally
                        {
                            if (rdr != null)
                                rdr.Dispose();
                        }
                    }
                    catch (Exception e)
                    {
                        // Even in KeepAlive mode, we disconnect on any error, will try to re-connect next time
                        m_owner.Disconnect();
                        throw e;
                    }
                }
                return nRecs;
            }

            public void Exec(params object[] parms)
            {
                lock (m_owner.m_dbLock)
                {
                    bool was_open = (m_owner.m_conn != null);   // existing conn -> try to reconnect on 1st failure
                    Connect();
                    try
                    {
                        try
                        {
                            setParams(parms);   // so they need be re-applied
                            m_cmd.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {
                            // might be an expired connection? 
                            if (was_open && TryReconnect(e, parms))
                                m_cmd.ExecuteNonQuery();
                            else
                                throw e;
                        }

                        if (!m_owner.m_keepAlive)
                            m_owner.Disconnect();

                    }
                    catch (Exception e)
                    {
                        m_owner.Disconnect();
                        throw e;
                    }
                }
            }

            public void Exec()
            {
                Exec(null);
            }

            public void Dispose()
            {
                Disconnect();
            }
        }
    }
}
