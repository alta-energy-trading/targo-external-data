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
                var commandText = "Delete from Movement";

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
                                bcp.DestinationTableName = "Movement";
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

                    connection.Close();
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
