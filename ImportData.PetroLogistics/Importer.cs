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
                                bcp.ColumnMappings.Add("tanker_name", "tanker_name");
                                bcp.ColumnMappings.Add("tanker_imo", "tanker_imo");
                                bcp.ColumnMappings.Add("tanker_dwt", "tanker_dwt");
                                bcp.ColumnMappings.Add("tanker_flag", "tanker_flag");
                                bcp.ColumnMappings.Add("tanker_owner", "tanker_owner");
                                bcp.ColumnMappings.Add("load_terminal", "load_terminal");
                                bcp.ColumnMappings.Add("grade_confidence", "grade_confidence");
                                bcp.ColumnMappings.Add("company_confidence", "company_confidence");
                                bcp.ColumnMappings.Add("quality_category", "quality_category");
                                bcp.ColumnMappings.Add("transit_time", "transit_time");
                                bcp.ColumnMappings.Add("load_port_date", "load_port_date");
                                bcp.ColumnMappings.Add("load_port", "load_port");
                                bcp.ColumnMappings.Add("load_country", "load_country");
                                bcp.ColumnMappings.Add("report_group", "report_group");
                                bcp.ColumnMappings.Add("load_port_area", "load_port_area");
                                bcp.ColumnMappings.Add("qty_tonnes", "qty_tonnes");
                                bcp.ColumnMappings.Add("qty_barrels", "qty_barrels");
                                bcp.ColumnMappings.Add("c_f", "c_f");
                                bcp.ColumnMappings.Add("cargo_type", "cargo_type");
                                bcp.ColumnMappings.Add("cargo_grade", "cargo_grade");
                                bcp.ColumnMappings.Add("pc", "pc");
                                bcp.ColumnMappings.Add("discharge_port_date", "discharge_port_date");
                                bcp.ColumnMappings.Add("discharge_port", "discharge_port");
                                bcp.ColumnMappings.Add("second_discharge_port", "second_discharge_port");
                                bcp.ColumnMappings.Add("via", "via");
                                bcp.ColumnMappings.Add("discharge_country", "discharge_country");
                                bcp.ColumnMappings.Add("discharge_area", "discharge_area");
                                bcp.ColumnMappings.Add("supplier", "supplier");
                                bcp.ColumnMappings.Add("middle_man", "middle_man");
                                bcp.ColumnMappings.Add("customer", "customer");
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
		                        And	ISNULL(s.supplier,'') = ISNULL(t.supplier,'')
		                        And	ISNULL(s.middle_man,'') = ISNULL(t.middle_man,'')
		                        And	ISNULL(s.customer,'') = ISNULL(t.customer,'')
                        When Matched
                        And	 (
		                        s.tanker_name<> ISNULL(t.tanker_name,'')
		                        or s.load_port<> ISNULL(t.load_port,'')
		                        or s.tanker_imo<> ISNULL(t.tanker_imo,'')
		                        or s.tanker_dwt<> ISNULL(t.tanker_dwt,'')
		                        or s.tanker_flag<> ISNULL(t.tanker_flag,'')
		                        or s.tanker_owner<> ISNULL(t.tanker_owner,'')
		                        or s.load_terminal<> ISNULL(t.load_terminal,'')
		                        or s.grade_confidence<> ISNULL(t.grade_confidence,'')
		                        or s.company_confidence<> ISNULL(t.company_confidence,'')
		                        or s.quality_category<> ISNULL(t.quality_category,'')
		                        or s.transit_time<> ISNULL(t.transit_time,'')
		                        or s.load_country<> ISNULL(t.load_country,'')
		                        or s.report_group<> ISNULL(t.report_group,'')
		                        or s.load_port_area<> ISNULL(t.load_port_area,'')
                                or s.load_port_date <> ISNULL(t.load_port_date,'')
		                        or s.qty_tonnes<> ISNULL(t.qty_tonnes,'')
		                        or s.qty_barrels<> ISNULL(t.qty_barrels,'')
		                        or s.c_f<> ISNULL(t.c_f,'')
		                        or s.cargo_type<> ISNULL(t.cargo_type,'')
		                        or s.cargo_grade<> ISNULL(t.cargo_grade,'')
		                        or s.pc<> ISNULL(t.pc,'')
		                        or s.discharge_port_date<> ISNULL(t.discharge_port_date,'')
		                        or s.discharge_port<> ISNULL(t.discharge_port,'')
		                        or s.second_discharge_port<> ISNULL(t.second_discharge_port,'')
		                        or s.via<> ISNULL(t.via,'')
		                        or s.discharge_country<> ISNULL(t.discharge_country,'')
		                        or s.discharge_area<> ISNULL(t.discharge_area,'')
		                        or s.supplier_note<> ISNULL(t.supplier_note,'')
		                        or s.customer_note<> ISNULL(t.customer_note,'')
		                        or s.note<> ISNULL(t.note,'')
		                        or s.client_cargo_status<> ISNULL(t.client_cargo_status,'')
                        )
                        Then
                        Update
                        Set t.tanker_name= s.tanker_name
                        , t.load_port= s.load_port
                        , t.tanker_imo= S.tanker_imo
		                , t.tanker_dwt= S.tanker_dwt
		                , t.tanker_flag= S.tanker_flag
		                , t.tanker_owner= S.tanker_owner
		                , t.load_terminal= S.load_terminal
		                , t.grade_confidence= S.grade_confidence
		                , t.company_confidence= S.company_confidence
		                , t.quality_category= S.quality_category
		                , t.transit_time= S.transit_time
                        , t.load_country= s.load_country
                        , t.report_group= s.report_group
                        , t.load_port_area= s.load_port_area
                        , t.qty_tonnes= s.qty_tonnes
                        , t.qty_barrels= s.qty_barrels
                        , t.c_f= s.c_f
                        , t.cargo_type= s.cargo_type
                        , t.cargo_grade= s.cargo_grade
                        , t.pc= s.pc
                        , t.load_port_date = s.load_port_date
                        , t.discharge_port_date= s.discharge_port_date
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
	                        , tanker_name
                            , query
	                        , load_port
                            , tanker_imo
		                    , tanker_dwt
		                    , tanker_flag
		                    , tanker_owner
		                    , load_terminal
		                    , grade_confidence
		                    , company_confidence
		                    , quality_category
		                    , transit_time
	                        , load_country
	                        , report_group
	                        , load_port_area
                            , load_port_date
	                        , qty_tonnes
	                        , qty_barrels
	                        , c_f
	                        , cargo_type
	                        , cargo_grade
	                        , pc
	                        , discharge_port_date
	                        , discharge_port
	                        , second_discharge_port
	                        , via
	                        , discharge_country
	                        , discharge_area
	                        , supplier
	                        , middle_man
	                        , customer
	                        , supplier_note
	                        , customer_note
	                        , note
	                        , client_cargo_status
                        )
                        Values (
	                        cargo_id
	                        , s.tanker_name
                            , s.query
	                        , s.load_port
                            , S.tanker_imo
		                    , S.tanker_dwt
		                    , S.tanker_flag
		                    , S.tanker_owner
		                    , S.load_terminal
		                    , S.grade_confidence
		                    , S.company_confidence
		                    , S.quality_category
		                    , S.transit_time
	                        , s.load_country
	                        , s.report_group
	                        , s.load_port_area
                            , s.load_port_date
	                        , s.qty_tonnes
	                        , s.qty_barrels
	                        , s.c_f
	                        , s.cargo_type
	                        , s.cargo_grade
	                        , s.pc
	                        , s.discharge_port_date
	                        , s.discharge_port
	                        , s.second_discharge_port
	                        , s.via
	                        , s.discharge_country
	                        , s.discharge_area
	                        , s.supplier
	                        , s.middle_man
	                        , s.customer
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
