using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DapperCleanData.Connection
{
    public class ConnectionFactory
    {
        private static string _connectionString;
        public static void SetConnectionString(string connectionString)
        {
            Dapper.SqlMapper.AddTypeMap(typeof(string), System.Data.DbType.AnsiString);
            _connectionString = connectionString;
        }

        public static DbConnection GetOpenConnection()
        {
            var connection = new SqlConnection(_connectionString);
            connection.Open();

            return connection;
        }
    }
}
