using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;
using System.Data;
using System.Data.SqlClient;
using OpenMetaverse;

namespace OpenSim.Data.MSSQL
{
    public class MSSQLDataSpecific : DataSpecificBase
    {
        // TODO: redefine any data conversions that need be MS SQL-specific 
        // (code from MSSQLManager would go here)

        public override string DBName()
        {
            return "MS SQL";
        }

        public override DbConnection GetNewConnection(BaseDataBase owner, string conn_str)
        {
            return new SqlConnection(conn_str);
        }

        protected override DbParameter MakeParam(DbCommand cmd, string sName, Type type, DataRow sch_row)
        {
            SqlParameter par = new SqlParameter();
            par.ParameterName = sName;
            par.Direction = ParameterDirection.Input;

            // We'll have the 'sch_row' only when the parameter name matches one of the table columns.
            // Otherwise (e.g. in a "limit @start, @count" clause) we have to use the type type 
            if (sch_row != null)
            {
                par.SqlDbType = (SqlDbType)Convert.ToInt32(sch_row["ProviderType"]);
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
                    par.SqlDbType = DbtypeFromType(type);
                else
                    par.DbType = DbType.Object;
            }

            return par;
        }

        /// <summary>
        /// Type conversion to a SQLDbType functions
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        internal SqlDbType DbtypeFromType(Type type)
        {
            if (type == typeof(string))
            {
                return SqlDbType.VarChar;
            }
            if (type == typeof(double))
            {
                return SqlDbType.Float;
            }
            if (type == typeof(Single))
            {
                return SqlDbType.Float;
            }
            if (type == typeof(int))
            {
                return SqlDbType.Int;
            }
            if (type == typeof(bool))
            {
                return SqlDbType.Bit;
            }
            if (type == typeof(UUID))
            {
                return SqlDbType.UniqueIdentifier;
            }
            if (type == typeof(sbyte))
            {
                return SqlDbType.Int;
            }
            if (type == typeof(Byte[]))
            {
                return SqlDbType.Image;
            }
            if (type == typeof(uint) || type == typeof(ushort))
            {
                return SqlDbType.Int;
            }
            if (type == typeof(ulong))
            {
                return SqlDbType.BigInt;
            }
            return SqlDbType.VarChar;
        }

        public override object UuidToDb(string uuid)
        {
            return new Guid(uuid);
        }

        /// <summary>Convert an UUID to whatever format is actually used in the database 
        /// to keep UUIDs.  The result of this conversion is what you should supply as Cmd parameters.
        /// </summary>
        /// <param name="uuid">The UUID to be converted</param>
        /// <returns></returns>
        public override object UuidToDb(UUID uuid)
        {
            return uuid.Guid;
        }

        public override object BoolToDb(bool b)
        {
            return b ? 1 : 0;
        }

        public override bool DbToBool(object o)
        {
            return Convert.ToInt32(o) != 0;
        }

    }
}