using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastMember;
using ImportData.PetroLogistics.Models;
using Newtonsoft.Json;

namespace ImportData.PetroLogistics
{
    public class Importer
    {
        private readonly string _connectionString;
        private List<ReportParams> Reports;

        public Importer(string connectionString, string user, string pass, string key, string hash, string domain, string path, string queryString)
        {
            Reports = new List<ReportParams>();
            _connectionString = connectionString;

            List<string> queries = queryString.Split('|').ToList();

            foreach (string query in queries)
            {
                var r = new ReportParams();

                r.API_HTTP_USER = user;
                r.API_HTTP_PASS = pass;
                r.API_KEY = key;
                r.API_HASH = hash;
                r.DOMAIN = domain;
                r.PATH = path;
                r.REPORT = ReportFormat.json;
                r.QUERY = query;

                Reports.Add(r);
            }
        }

        public void Import()
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                var commandText = "Select top 0 * into #temp_Movement from Movement";

                using (SqlCommand command = connection.CreateCommand())
                {
                    connection.Open();
                    command.CommandText = commandText;
                    command.ExecuteNonQuery();

                    var queries = string.Empty;
                    foreach (ReportParams r in Reports)
                    {
                        if (string.IsNullOrEmpty(r.QUERY))
                        {
                            WriteToLog($"No Query is defined");
                            continue;
                        }
                        WriteToLog($"Query is {r.QUERY}. Getting records");

                        queries += $"'{r.QUERY}',";
                        var content = string.Empty;
                        
                        try
                        {
                            content = new FetchReport(r).getreport();
                        }
                        catch (Exception e)
                        {
                            WriteToLog($"{e.Message} {e.InnerException}");
                            throw;
                        }

                        var response = JsonConvert.DeserializeObject<Response>(content);

                        var toAdd = response.envelope.movements.ToList();
                        toAdd.ForEach(m => m.query = r.QUERY);

                        WriteToLog($"{toAdd.Count} records found");

                        using (var bcp = new SqlBulkCopy(connection))
                        {
                            bcp.BulkCopyTimeout = 0;
                            using (var reader = ObjectReader.Create(toAdd))
                            {
                                bcp.DestinationTableName = "#temp_Movement";
                                bcp.ColumnMappings.Add("query", "query");
                                bcp.ColumnMappings.Add("tankers_name", "tankers_name");
                                bcp.ColumnMappings.Add("loadport_date", "loadport_date");
                                bcp.ColumnMappings.Add("load_port", "load_port");
                                bcp.ColumnMappings.Add("load_country", "load_country");
                                bcp.ColumnMappings.Add("ports_report_group", "ports_report_group");
                                bcp.ColumnMappings.Add("load_port_area", "load_port_area");
                                bcp.ColumnMappings.Add("qty_tonnes", "qty_tonnes");
                                bcp.ColumnMappings.Add("qty_bbl", "qty_bbl");
                                bcp.ColumnMappings.Add("c_f", "c_f");
                                bcp.ColumnMappings.Add("cargo_type", "cargo_type");
                                bcp.ColumnMappings.Add("cargo_grade", "cargo_grade");
                                bcp.ColumnMappings.Add("pc", "pc");
                                bcp.ColumnMappings.Add("dischargeport_date", "dischargeport_date");
                                bcp.ColumnMappings.Add("discharge_port", "discharge_port");
                                bcp.ColumnMappings.Add("second_discharge_port", "second_discharge_port");
                                bcp.ColumnMappings.Add("via", "via");
                                bcp.ColumnMappings.Add("discharge_country", "discharge_country");
                                bcp.ColumnMappings.Add("discharge_area", "discharge_area");
                                bcp.ColumnMappings.Add("supplier_name", "supplier_name");
                                bcp.ColumnMappings.Add("middle_man", "middle_man");
                                bcp.ColumnMappings.Add("company_name", "company_name");
                                bcp.ColumnMappings.Add("supplier_note", "supplier_note");
                                bcp.ColumnMappings.Add("customer_note", "customer_note");
                                bcp.ColumnMappings.Add("note", "note");
                                bcp.ColumnMappings.Add("cargo_id", "cargo_id");
                                bcp.ColumnMappings.Add("client_cargo_status", "client_cargo_status");
                                bcp.WriteToServer(reader);
                            }
                        }

                        if (content.Contains("(407) Proxy Authentication") ||
                            content.Contains("(401) Unauthorized"))
                        {
                            WriteToLog(content);
                            throw new Exception(content);
                        }
                    }
                    // merge here
                    command.CommandText = @"
                        Merge	Movement t
                        Using	#temp_Movement s
		                        On	s.cargo_id = t.cargo_id
		                        And	ISNULL(s.supplier_name,'') = ISNULL(t.supplier_name,'')
		                        And	ISNULL(s.middle_man,'') = ISNULL(t.middle_man,'')
		                        And	ISNULL(s.company_name,'') = ISNULL(t.company_name,'')
                        When Matched
                        And	 (
		                        s.tankers_name<> t.tankers_name
		                        or s.loadport_date<> t.loadport_date
		                        or s.load_port<> t.load_port
		                        or s.load_country<> t.load_country
		                        or s.ports_report_group<> t.ports_report_group
		                        or s.load_port_area<> t.load_port_area
		                        or s.qty_tonnes<> t.qty_tonnes
		                        or s.qty_bbl<> t.qty_bbl
		                        or s.c_f<> t.c_f
		                        or s.cargo_type<> t.cargo_type
		                        or s.cargo_grade<> t.cargo_grade
		                        or s.pc<> t.pc
		                        or s.dischargeport_date<> t.dischargeport_date
		                        or s.discharge_port<> t.discharge_port
		                        or s.second_discharge_port<> t.second_discharge_port
		                        or s.via<> t.via
		                        or s.discharge_country<> t.discharge_country
		                        or s.discharge_area<> t.discharge_area
		                        or s.supplier_note<> t.supplier_note
		                        or s.customer_note<> t.customer_note
		                        or s.note<> t.note
		                        or s.client_cargo_status<> t.client_cargo_status
                        )
                        Then
                        Update
                        Set t.tankers_name= s.tankers_name
                        , t.loadport_date= s.loadport_date
                        , t.load_port= s.load_port
                        , t.load_country= s.load_country
                        , t.ports_report_group= s.ports_report_group
                        , t.load_port_area= s.load_port_area
                        , t.qty_tonnes= s.qty_tonnes
                        , t.qty_bbl= s.qty_bbl
                        , t.c_f= s.c_f
                        , t.cargo_type= s.cargo_type
                        , t.cargo_grade= s.cargo_grade
                        , t.pc= s.pc
                        , t.dischargeport_date= s.dischargeport_date
                        , t.discharge_port= s.discharge_port
                        , t.second_discharge_port= s.second_discharge_port
                        , t.via= s.via
                        , t.discharge_country= s.discharge_country
                        , t.discharge_area= s.discharge_area
                        , t.supplier_note= s.supplier_note
                        , t.customer_note= s.customer_note
                        , t.note= s.note
                        , t.client_cargo_status= s.client_cargo_status
                        When Not Matched By Target
                        Then
                        Insert (
	                        cargo_id
	                        , tankers_name
                            , query
	                        , loadport_date
	                        , load_port
	                        , load_country
	                        , ports_report_group
	                        , load_port_area
	                        , qty_tonnes
	                        , qty_bbl
	                        , c_f
	                        , cargo_type
	                        , cargo_grade
	                        , pc
	                        , dischargeport_date
	                        , discharge_port
	                        , second_discharge_port
	                        , via
	                        , discharge_country
	                        , discharge_area
	                        , supplier_name
	                        , middle_man
	                        , company_name
	                        , supplier_note
	                        , customer_note
	                        , note
	                        , client_cargo_status
                        )
                        Values (
	                        cargo_id
	                        , s.tankers_name
                            , s.query
	                        , s.loadport_date
	                        , s.load_port
	                        , s.load_country
	                        , s.ports_report_group
	                        , s.load_port_area
	                        , s.qty_tonnes
	                        , s.qty_bbl
	                        , s.c_f
	                        , s.cargo_type
	                        , s.cargo_grade
	                        , s.pc
	                        , s.dischargeport_date
	                        , s.discharge_port
	                        , s.second_discharge_port
	                        , s.via
	                        , s.discharge_country
	                        , s.discharge_area
	                        , s.supplier_name
	                        , s.middle_man
	                        , s.company_name
	                        , s.supplier_note
	                        , s.customer_note
	                        , s.note
	                        , s.client_cargo_status
                        )
                        When Not Matched By Source
                        And t.query in (@queries)
                        Then Delete
                        OUTPUT
                        $action,
                        inserted.cargo_id,
                        deleted.cargo_id;
                        DROP TABLE #temp_Movement";

                    command.Parameters.Add("@queries", SqlDbType.VarChar);
                    command.Parameters["@queries"].Value = queries.TrimEnd(',');

                    SqlDataReader sqlDataReader = command.ExecuteReader();

                    int inserted = 0,
                        updated = 0,
                        deleted = 0;
                    while (sqlDataReader.Read())
                    {
                        if (sqlDataReader.GetString(0).ToLower() == "insert") inserted++;
                        if (sqlDataReader.GetString(0).ToLower() == "update") updated++;
                        if (sqlDataReader.GetString(0).ToLower() == "delete") deleted++;
                    }
                    connection.Close();

                    WriteToLog($"Inserted: {inserted}");
                    WriteToLog($"Updated: {updated}");
                    WriteToLog($"Deleted: {deleted}");
                }
            }
            WriteToLog($"Complete.");
        }

        private void SaveData(string content)
        {
            var response = JsonConvert.DeserializeObject<Response>(content);

            var toAdd = response.envelope.movements.ToList();

            using (var connection = new SqlConnection(_connectionString))
            {
                var commandText = "Select top 0 * into #temp_Movement from Movement";

                using (SqlCommand command = connection.CreateCommand())
                {
                    connection.Open();
                    command.CommandText = commandText;
                    command.ExecuteNonQuery();


                    using (var bcp = new SqlBulkCopy(connection))
                    {
                        bcp.BulkCopyTimeout = 0;
                        using (var reader = ObjectReader.Create(toAdd))
                        {
                            bcp.DestinationTableName = "#temp_Movement";
                            bcp.ColumnMappings.Add("tankers_name", "tankers_name");
                            bcp.ColumnMappings.Add("loadport_date", "loadport_date");
                            bcp.ColumnMappings.Add("load_port", "load_port");
                            bcp.ColumnMappings.Add("load_country", "load_country");
                            bcp.ColumnMappings.Add("ports_report_group", "ports_report_group");
                            bcp.ColumnMappings.Add("load_port_area", "load_port_area");
                            bcp.ColumnMappings.Add("qty_tonnes", "qty_tonnes");
                            bcp.ColumnMappings.Add("qty_bbl", "qty_bbl");
                            bcp.ColumnMappings.Add("c_f", "c_f");
                            bcp.ColumnMappings.Add("cargo_type", "cargo_type");
                            bcp.ColumnMappings.Add("cargo_grade", "cargo_grade");
                            bcp.ColumnMappings.Add("pc", "pc");
                            bcp.ColumnMappings.Add("dischargeport_date", "dischargeport_date");
                            bcp.ColumnMappings.Add("discharge_port", "discharge_port");
                            bcp.ColumnMappings.Add("second_discharge_port", "second_discharge_port");
                            bcp.ColumnMappings.Add("via", "via");
                            bcp.ColumnMappings.Add("discharge_country", "discharge_country");
                            bcp.ColumnMappings.Add("discharge_area", "discharge_area");
                            bcp.ColumnMappings.Add("supplier_name", "supplier_name");
                            bcp.ColumnMappings.Add("middle_man", "middle_man");
                            bcp.ColumnMappings.Add("company_name", "company_name");
                            bcp.ColumnMappings.Add("supplier_note", "supplier_note");
                            bcp.ColumnMappings.Add("customer_note", "customer_note");
                            bcp.ColumnMappings.Add("note", "note");
                            bcp.ColumnMappings.Add("cargo_id", "cargo_id");
                            bcp.ColumnMappings.Add("client_cargo_status", "client_cargo_status");
                            bcp.WriteToServer(reader);
                        }
                    }

                    command.CommandText = @"
                        Merge	Movement t
                        Using	#temp_Movement s
		                        On	s.cargo_id = t.cargo_id
		                        And	s.supplier_name = t.supplier_name
		                        And	s.middle_man = t.middle_man
		                        And	s.company_name = t.company_name
                        When Matched
                        And	 (
		                        s.tankers_name<> t.tankers_name
		                        or s.loadport_date<> t.loadport_date
		                        or s.load_port<> t.load_port
		                        or s.load_country<> t.load_country
		                        or s.ports_report_group<> t.ports_report_group
		                        or s.load_port_area<> t.load_port_area
		                        or s.qty_tonnes<> t.qty_tonnes
		                        or s.qty_bbl<> t.qty_bbl
		                        or s.c_f<> t.c_f
		                        or s.cargo_type<> t.cargo_type
		                        or s.cargo_grade<> t.cargo_grade
		                        or s.pc<> t.pc
		                        or s.dischargeport_date<> t.dischargeport_date
		                        or s.discharge_port<> t.discharge_port
		                        or s.second_discharge_port<> t.second_discharge_port
		                        or s.via<> t.via
		                        or s.discharge_country<> t.discharge_country
		                        or s.discharge_area<> t.discharge_area
		                        or s.supplier_note<> t.supplier_note
		                        or s.customer_note<> t.customer_note
		                        or s.note<> t.note
		                        or s.client_cargo_status<> t.client_cargo_status
                        )
                        Then
                        Update
                        Set t.tankers_name= s.tankers_name
                        , t.loadport_date= s.loadport_date
                        , t.load_port= s.load_port
                        , t.load_country= s.load_country
                        , t.ports_report_group= s.ports_report_group
                        , t.load_port_area= s.load_port_area
                        , t.qty_tonnes= s.qty_tonnes
                        , t.qty_bbl= s.qty_bbl
                        , t.c_f= s.c_f
                        , t.cargo_type= s.cargo_type
                        , t.cargo_grade= s.cargo_grade
                        , t.pc= s.pc
                        , t.dischargeport_date= s.dischargeport_date
                        , t.discharge_port= s.discharge_port
                        , t.second_discharge_port= s.second_discharge_port
                        , t.via= s.via
                        , t.discharge_country= s.discharge_country
                        , t.discharge_area= s.discharge_area
                        , t.supplier_note= s.supplier_note
                        , t.customer_note= s.customer_note
                        , t.note= s.note
                        , t.client_cargo_status= s.client_cargo_status
                        When Not Matched By Target
                        Then
                        Insert (
	                        cargo_id
	                        , tankers_name
	                        , loadport_date
	                        , load_port
	                        , load_country
	                        , ports_report_group
	                        , load_port_area
	                        , qty_tonnes
	                        , qty_bbl
	                        , c_f
	                        , cargo_type
	                        , cargo_grade
	                        , pc
	                        , dischargeport_date
	                        , discharge_port
	                        , second_discharge_port
	                        , via
	                        , discharge_country
	                        , discharge_area
	                        , supplier_name
	                        , middle_man
	                        , company_name
	                        , supplier_note
	                        , customer_note
	                        , note
	                        , client_cargo_status
                        )
                        Values (
	                        cargo_id
	                        , s.tankers_name
	                        , s.loadport_date
	                        , s.load_port
	                        , s.load_country
	                        , s.ports_report_group
	                        , s.load_port_area
	                        , s.qty_tonnes
	                        , s.qty_bbl
	                        , s.c_f
	                        , s.cargo_type
	                        , s.cargo_grade
	                        , s.pc
	                        , s.dischargeport_date
	                        , s.discharge_port
	                        , s.second_discharge_port
	                        , s.via
	                        , s.discharge_country
	                        , s.discharge_area
	                        , s.supplier_name
	                        , s.middle_man
	                        , s.company_name
	                        , s.supplier_note
	                        , s.customer_note
	                        , s.note
	                        , s.client_cargo_status
                        )
                        When Not Matched By Source
                        Then Delete
                        OUTPUT
                        $action,
                        inserted.cargo_id,
                        deleted.cargo_id;
                        DROP TABLE #temp_Movement";
                    SqlDataReader sqlDataReader = command.ExecuteReader();
                    int inserted = 0,
                        updated = 0,
                        deleted = 0;
                    while (sqlDataReader.Read())
                    {
                        if (sqlDataReader.GetString(0).ToLower() == "insert") inserted++;
                        if (sqlDataReader.GetString(0).ToLower() == "update") updated++;
                        if (sqlDataReader.GetString(0).ToLower() == "delete") deleted++;
                    }
                    connection.Close();

                    WriteToLog($"Inserted: {inserted}");
                    WriteToLog($"Updated: {inserted}");
                    WriteToLog($"Deleted: {inserted}");
                }
            }
        }

        public void WriteToLog(string msg)
        {
            Debug.WriteLine(msg);
            //using (var connection = new SqlConnection(_connectionString))
            //{
            //    var sql = @"INSERT INTO ImportLog (Source,Message) Values (@source,@message)";
            //    var command = new SqlCommand(sql, connection);
            //    command.Parameters.Add("@source", SqlDbType.VarChar);
            //    command.Parameters.Add("@message", SqlDbType.VarChar);

            //    command.Parameters["@source"].Value = "PetroLogistics";
            //    command.Parameters["@message"].Value = msg;

            //    try
            //    {
            //        connection.Open();
            //        Int32 rowsAffected = command.ExecuteNonQuery();
            //        Debug.WriteLine("Rows Affected@ {0}", rowsAffected);
            //    }
            //    catch (Exception ex)
            //    {
            //        throw ex;
            //    }
            //}
        }
    }
}
