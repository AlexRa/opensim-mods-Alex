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

namespace OpenSim.Data
{

    /// <summary>This is a base class for methods that may be specific to a DBMS (such as encoding of 
    /// certain data types), but not to a particular data store. So far all the methods here are static.
    /// The intended use for this class is to make a DBMS-specific derived class from it, then
    /// include it as a generic parameter to a particular data connector.</summary>
    /// <example>
    /// <code>
    /// // Common for all MySql data stores:
    /// class MySqlDataSpecific:  DataSpecificBase { ... }
    /// 
    /// // A specific store (no longer generic)
    /// class MySqlAssetData :  : AssetDataBase&lt;MySqlConnection, MySqlDataSpecific&gt;
    /// {
    ///  ...
    /// } 
    /// </code>
    /// </example>
    public class DataSpecificBase
    {

        public DataSpecificBase()   // must have a default ctr to satisfy 'new()' constraint in BaseDataBaseEx
        {
        }

        public virtual string DBName()
        {
            throw new NotImplementedException();
        }

        /// <summary>Check (probably in a DB-specific way) if an exception that occured while trying to execute
        /// a query has been caused by a timed out connection, so that re-connecting and running the query
        /// again may fix the problem.  Overriding this is not critical: the implementation below always says 
        /// retry, but even so the logic in BaseDataBase.Cmd will try reconnecting only once,
        /// and only if the connection was open prior to the current statement (i.e. in KeepAlive mode).
        /// </summary>
        /// <param name="cmd">The command that has just failed (can get DbConnection from it, if needed)</param>
        /// <param name="e">The exception reported by the database</param>
        /// <returns></returns>
        public virtual bool NeedReconnect(DbCommand cmd, Exception e)
        {
            return true;
        }

        /// <summary>This overridable returns a Migration object. For MySql that would be
        /// a derived class, for other DBs this one would do.
        /// </summary>
        public virtual Migration GetMigration()
        {
            return new Migration();
        }

        protected virtual DbParameter MakeParam(DbCommand cmd, string sName, Type type, DataRow sch_row)
        {
            DbParameter par = cmd.CreateParameter();
            par.ParameterName = sName;
            par.DbType = DbType.Object;
            par.Direction = ParameterDirection.Input;
            return par;
        }

        /// <summary>We need to guarantee that when a command is "prepared" in whatever way
        /// supported by the database, at least the Parameters list is filled up with all the
        /// parameters declared in the SQL, in the order of their appearance. 
        /// 
        /// </summary>
        /// <param name="cmd"></param>
        /// <returns></returns>

        // It seems likely that adding/removing Parameters items after a DbCommand has been
        // prepared might un-prepare it, whereas just assigning Value to existing parameters
        // should be Ok.  Again, this is a guess that should be checked.
        // 
        // This method also converts, if necessary, the parameter prefix from '@' in all queries
        // to whatever character recognized by the specific database (':', '@' or '?').
        // (NOTE: although MySQL, MS SQL and SQLite normally use different parameter prefix,
        // they ALL recognize '@'!)
        // 
        // NOTE: at least MS SQL requires specific lengths for all string/binary parameters,
        // so we'll need some way (preferably DBMS-independent) to fetch the column data
        // from the table before 

        public virtual bool Prepare(DbCommand cmd, string[] param_names, Type[] types, DataTable schema)
        {
            int n = param_names.Length; 
            DataRow[] rows = new DataRow[n];

            // Map column information from the schema table to the matching parameter names: 
            if (schema != null)
            {
                foreach (DataRow row in schema.Rows)
                {
                    if( row["ColumnName"] == null ) continue;
                    string sName = row["ColumnName"].ToString();
                    int ndx = Array.FindIndex(param_names, s => s.Equals(sName, StringComparison.InvariantCultureIgnoreCase));
                    if (ndx >= 0)
                        rows[ndx] = row;
                }
            }

            bool has_types = (types != null) && (types.Length > 0);
            cmd.Parameters.Clear();
            for(int i = 0; i < n; i++)
            {
                Type t = has_types ? types[i] : null;
                DbParameter par = MakeParam(cmd, param_names[i], t, rows[i]);
                cmd.Parameters.Add(par);
            }
            cmd.Prepare();
            
            return true;
        }

        public virtual void SetParamValue(DbParameter par, object value)
        {
            if (value is UUID)
            {
                switch (par.DbType)
                {
                    case DbType.Guid:
                        par.Value = ((UUID)value).Guid;
                        return;
                    case DbType.Binary:
                        par.Value = ((UUID)value).GetBytes();
                        return;
                    case DbType.String:
                    case DbType.AnsiString:
                    case DbType.AnsiStringFixedLength:
                    case DbType.StringFixedLength:
                        par.Value = ((UUID)value).ToString();
                        return;
                    default:
                        par.Value = value;
                        return;
                }
            }

            // Ideally, this should never happen,  but if we do get the CreatorID passed
            // here as a string, this is needed:
            if (value is string && par.DbType == DbType.Guid)
            {
                par.Value = new Guid((string)value);
                return;
            }

            if (value is bool)
            {
                switch (par.DbType)
                {
                    case DbType.UInt16:
                    case DbType.UInt32:
                    case DbType.UInt64:
                    case DbType.Int16:
                    case DbType.Int32:
                    case DbType.Int64:
                    case DbType.SByte:
                    case DbType.Byte:
                        par.Value = (bool)value ? 1 : 0;
                        return;
                    default:
                        par.Value = value;
                        return;
                }
            }

            par.Value = value;
        }


        // We want to have some virtuals to convert between UUID/string in memory and whatever way the UUIDs are
        // stored in the specific database. The functions below provide a reasonable default implementation, but
        // some of the derived DBMS-specific classes may want to override them.

        /// <summary>Convert a string representing an UUID to whatever format is actually used in the database 
        /// to keep UUIDs.  The result of this conversion is what you should supply as Cmd parameters.
        /// </summary>
        /// <param name="uuid">A string that must represent an UUID</param>
        /// <returns></returns>
        public virtual object UuidToDb(string uuid)
        {
            return uuid;
        }

        /// <summary>Convert an UUID to whatever format is actually used in the database 
        /// to keep UUIDs.  The result of this conversion is what you should supply as Cmd parameters.
        /// </summary>
        /// <param name="uuid">The UUID to be converted</param>
        /// <returns></returns>
        public virtual object UuidToDb(UUID uuid)
        {
            return uuid.ToString();
        }

        /// <summary>Convert a value returned from the database to an UUID. This implementation
        /// can convert values represented as 'string', 'Guid' or 'byte[16]'.  All other types
        /// throw an exception.  This implementation should work in most situations, but you
        /// might want to override it for efficiency reasons (when you know for sure what format
        /// you'll be getting from the DB)
        /// </summary>
        /// <param name="uuid">A value fetched from the DB, presumably representing an UUID</param>
        /// <returns></returns>
        public virtual UUID DbToUuid(object uuid)
        {
            if (uuid == null)
                return UUID.Zero;
            if (uuid is string)
                return new UUID((string)uuid);
            if (uuid is Guid)
                return new UUID((Guid)uuid);
            if ((uuid is byte[]) && ((byte[])uuid).Length == 16)
                return new UUID((byte[])uuid, 0);
            throw new Exception("Can''t convert data to UUID format: " + uuid.GetType().FullName);
        }

        /// <summary>Convert a value returned from the database to a *string representation* of an UUID.
        /// See DbToUuid for more details.  Note that this function goes through some extra conversions
        /// even if the data from the db is already a string.  
        /// </summary>
        /// <param name="uuid">A value fetched from the DB, presumably representing an UUID</param>
        /// <returns></returns>
        public virtual string DbToUuidStr(object uuid)
        {
            return DbToUuid(uuid).ToString();

            /*
                        if (uuid == null)
                            return UUID.Zero;
                        else
                            return uuid.ToString();
             */
        }

        public virtual object BoolToDb(bool b)
        {
            return b ? 1 : 0;
        }

        public virtual bool DbToBool(object o)
        {
            if (o is string)
            {
                string s = ((string)o).ToLower();
                return (s == "true" || s == "yes" || s == "1");
            }
            return Convert.ToInt32(o) != 0;
        }

    }

}
