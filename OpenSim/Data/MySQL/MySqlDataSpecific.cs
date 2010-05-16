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
using MySql.Data.MySqlClient;
using MySql.Data.Types;
using OpenMetaverse;

namespace OpenSim.Data.MySQL
{
    public class MySqlDataSpecific : DataSpecificBase
    {
        // TODO: redefine any data conversions that need be MySql-specific 
        // (ok as is so far)

        public override string DBName()
        {
            return "MySQL";
        }

        public override DbConnection GetNewConnection(BaseDataBase owner, string conn_str)
        {
            // Unless explicitly set in the conn string, we want the Prepare() method enabled!
            if (owner.KeepAlive && (conn_str.IndexOf("IgnorePrepare", StringComparison.InvariantCultureIgnoreCase) < 0) )
            {
                if (!conn_str.EndsWith(";"))
                    conn_str += ";";
                conn_str += "IgnorePrepare=false";
            }

            return new MySqlConnection(conn_str);
        }

        protected override DbParameter MakeParam(DbCommand cmd, string sName, Type type, DataRow sch_row)
        {
            MySqlParameter par = new MySqlParameter();
            par.ParameterName = "@" + sName;
            par.Direction = ParameterDirection.Input;

            // We'll have the 'sch_row' only when the parameter name matches one of the table columns.
            // Otherwise (e.g. in a "limit @start, @count" clause) we have to use the type type 
            if (sch_row != null)
            {
                par.MySqlDbType = (MySqlDbType)Convert.ToInt32(sch_row["ProviderType"]);
                par.Size = Convert.ToInt32(sch_row["ColumnSize"]);

                int x = Convert.ToInt32(sch_row["NumericPrecision"]);
                if (x >= 0 && x <= 38)
                    par.Precision = (byte)x;

                x = Convert.ToInt32(sch_row["NumericScale"]);
                if (x >= 0 && x <= 38)
                    par.Scale = Convert.ToByte(sch_row["NumericScale"]);
            }
            else
            {
                if (type != null)   // no idea what the parameter is - really should never happen!
                    par.MySqlDbType = MySqlDbtypeFromType(type);
                else
                    par.DbType = DbType.Object;
            }

            return par;
        }

        // NOTE: MySql driver doesn't correctly map MySqlDbType to DbType, so we must have the override:

        public override void SetParamValue(DbParameter par, object value)
        {
            if (value is UUID)
            {
                switch (((MySqlParameter)par).MySqlDbType)
                {
                    case MySqlDbType.Guid:
                        par.Value = ((UUID)value).Guid;
                        return;
                    case MySqlDbType.Binary:
                        par.Value = ((UUID)value).GetBytes();
                        return;
                    case MySqlDbType.String:
                    case MySqlDbType.VarString:
                    case MySqlDbType.VarChar:
                        par.Value = ((UUID)value).ToString();
                        return;
                    default:
                        par.Value = value;
                        return;
                }
            }
            par.Value = value;
        }
        internal MySqlDbType MySqlDbtypeFromType(Type type)
        {
            if (type == typeof(string))
            {
                return MySqlDbType.VarChar;
            }
            if (type == typeof(double))
            {
                return MySqlDbType.Float;
            }
            if (type == typeof(Single))
            {
                return MySqlDbType.Float;
            }
            if (type == typeof(int))
            {
                return MySqlDbType.Int32;
            }
            if (type == typeof(bool))
            {
                return MySqlDbType.Bit;
            }
            if (type == typeof(UUID))
            {
                return MySqlDbType.Guid;
            }
            if (type == typeof(sbyte))
            {
                return MySqlDbType.Int16;
            }
            if (type == typeof(Byte[]))
            {
                return MySqlDbType.Blob;
            }
            if (type == typeof(uint) || type == typeof(ushort))
            {
                return MySqlDbType.Int32;
            }
            if (type == typeof(ulong))
            {
                return MySqlDbType.Int64;
            }
            return MySqlDbType.VarChar;
        }

        /// <summary>This overridable returns a Migration object. For MySql that would be
        /// a derived class, for other DBs this one would do.
        /// </summary>
        public override Migration GetMigration()
        {
            return new MySqlMigration();
        }
    }
}
