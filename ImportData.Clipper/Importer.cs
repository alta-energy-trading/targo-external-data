using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using Dapper;
using FastMember;
using ImportData.Clipper.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ImportData.Clipper
{
    public class Importer
    {
        private static string _baseUrl;
        private static string _user;
        private static string _pass;
        private static string _currentFeed;
        private readonly string _connectionString;

        public static string CurrentFeed => _currentFeed;

        public Importer(string connectionString, string url, string user, string pass)
        {
            _baseUrl = url;
            _user = user;
            _pass = pass;
            _connectionString = connectionString;
            ConnectionFactory.SetConnectionString(connectionString);
        }

        public void Import(string types,int baseline)
        {
            int countFailedLogin = 0;
            foreach (string type in types.Split(',').ToList())
            {
                WriteToLog($"Type is {type}.");
                TaskResult apiTaskResult = new TaskResult(1, 0, 0);
                while ((apiTaskResult.AddCount != 0 || apiTaskResult.RemoveCount != 0) &&
                       countFailedLogin < 10) // Until all records are downloaded
                {
                    var content = string.Empty;

                    using (var handler = BuildHttpRequestHandler(_user, _pass))
                    using (var client = new HttpClient(handler))
                    {
                        string encoded =
                            Convert.ToBase64String(Encoding.GetEncoding("ISO-8859-1").GetBytes(_user + ":" + _pass));
                        client.DefaultRequestHeaders.Add("Authorization", "Basic " + encoded);

                        _currentFeed = GetCurrentFeedType(type);
                        var maxDateNum = apiTaskResult.DataSetMaxDateNum == 0 ? GetMaxDatenum(CurrentFeed) : apiTaskResult.DataSetMaxDateNum;

                        var url = baseline == 1
                            ? BuildBaseLineDataUri(maxDateNum, 0, type)
                            : BuildAppendDataUri(maxDateNum, type);

                        try
                        {
                            WriteToLog($"Getting records where Datenum > {maxDateNum}");
                            content = client.GetStringAsync(url).Result;
                        }
                        catch (AggregateException ex)
                        {
                            AggregateException aggregateException = ex.Flatten();
                            for (int i = 0; i < aggregateException.InnerExceptions.Count; ++i)
                            {
                                var canceledException =
                                    aggregateException.InnerExceptions[i];

                                if (canceledException.Message.Contains("407") ||
                                    canceledException.Message.Contains("401"))
                                {
                                    WriteToLog($"Failed Login");
                                    content = "Failed Login";
                                }
                                else throw new Exception(canceledException.Message);
                            }
                        }
                        catch (Exception e)
                        {
                            if (e.Message.Substring(0, 8).Contains("Unexpect") || e.Message.Contains("401"))
                            {
                                WriteToLog($"Failed Login");
                                content = "Failed Login";
                            }
                            else throw new Exception(e.Message);
                        }
                    }
                    if (content == "Failed Login")
                    {
                        countFailedLogin++;
                        System.Threading.Thread.Sleep(20000);
                        continue;
                    }

                    countFailedLogin = 0;
                    apiTaskResult = SaveData(content, GetCurrentFeedType(type));
                }
                if (countFailedLogin == 10) throw new Exception("Failed to login to Web API");
            }
            WriteToLog($"Complete.");
        }

        private TaskResult SaveData(string content, string type)
        {
            var response = JsonConvert.DeserializeObject<Response>(content, new JsonSerializerSettings
            {
                ContractResolver = new CustomDataContractResolver(type)
            });

            var toAdd = response.Records.Where(r => r.StatNum == 0).ToList();
            var toRemove = response.Records.Where(r => r.StatNum == 1).Select(r => r.RowNum).ToList();

            DeleteList(toRemove);

            WriteToLog($"Inserting {toAdd.Count} rows");
            using (var bcp = new SqlBulkCopy(_connectionString))
            {
                bcp.BulkCopyTimeout = 0;
                using (var reader = ObjectReader.Create(toAdd))
                {
                    bcp.DestinationTableName = "ClipperStaging";
                    bcp.ColumnMappings.Add("DateNum", "DateNum");
                    bcp.ColumnMappings.Add("RowNum", "RowNum");
                    bcp.ColumnMappings.Add("StatNum", "StatNum");
                    bcp.ColumnMappings.Add("Type", "Type");
                    bcp.ColumnMappings.Add("LoadArea", "LoadArea");
                    bcp.ColumnMappings.Add("OffTakeArea", "OffTakeArea");
                    bcp.ColumnMappings.Add("OffTakePoint", "OffTakePoint");
                    bcp.ColumnMappings.Add("Api", "Api");
                    bcp.ColumnMappings.Add("Bbls", "Bbls");
                    bcp.ColumnMappings.Add("BblsNominal", "BblsNominal");
                    bcp.ColumnMappings.Add("Bill", "Bill");
                    bcp.ColumnMappings.Add("BillDate", "BillDate");
                    bcp.ColumnMappings.Add("BillDescription", "BillDescription");
                    bcp.ColumnMappings.Add("Cas", "Cas");
                    bcp.ColumnMappings.Add("charter_grade", "charter_grade");
                    bcp.ColumnMappings.Add("charter_load_area", "charter_load_area");
                    bcp.ColumnMappings.Add("charter_offtake_area", "charter_offtake_area");
                    bcp.ColumnMappings.Add("charterer", "charterer");
                    bcp.ColumnMappings.Add("CdReport", "CdReport");
                    bcp.ColumnMappings.Add("Consignee", "Consignee");
                    bcp.ColumnMappings.Add("declaredDest", "declaredDest");
                    bcp.ColumnMappings.Add("draught", "draught");
                    bcp.ColumnMappings.Add("fix_date", "fix_date");
                    bcp.ColumnMappings.Add("Grade", "Grade");
                    bcp.ColumnMappings.Add("GradeApi", "GradeApi");
                    bcp.ColumnMappings.Add("GradeCountry", "GradeCountry");
                    bcp.ColumnMappings.Add("GradeRegion", "GradeRegion");
                    bcp.ColumnMappings.Add("GradeSubtype", "GradeSubtype");
                    bcp.ColumnMappings.Add("GradeType", "GradeType");
                    bcp.ColumnMappings.Add("GradeSulfur", "GradeSulfur");
                    bcp.ColumnMappings.Add("Imo", "Imo");
                    bcp.ColumnMappings.Add("Lat", "Lat");
                    bcp.ColumnMappings.Add("Laycan", "Laycan");
                    bcp.ColumnMappings.Add("Lightering_vessel", "Lightering_vessel");
                    bcp.ColumnMappings.Add("LoadAreaDescr", "LoadAreaDescr");
                    bcp.ColumnMappings.Add("LoadCountry", "LoadCountry");
                    bcp.ColumnMappings.Add("LoadDate", "LoadDate");
                    bcp.ColumnMappings.Add("LoadOwner", "LoadOwner");
                    bcp.ColumnMappings.Add("LoadPoint", "LoadPoint");
                    bcp.ColumnMappings.Add("LoadPort", "LoadPort");
                    bcp.ColumnMappings.Add("LoadRegion", "LoadRegion");
                    bcp.ColumnMappings.Add("LoadStsVessel", "LoadStsVessel");
                    bcp.ColumnMappings.Add("Load_sts_imo", "Load_sts_imo");
                    bcp.ColumnMappings.Add("Lon", "Lon");
                    bcp.ColumnMappings.Add("LoadTradingWeek", "LoadTradingWeek");
                    bcp.ColumnMappings.Add("LoadTradingYear", "LoadTradingYear");
                    bcp.ColumnMappings.Add("LzTanker", "LzTanker");
                    bcp.ColumnMappings.Add("mt", "mt");
                    bcp.ColumnMappings.Add("mtNominal", "mtNominal");
                    bcp.ColumnMappings.Add("Notification", "Notification");
                    bcp.ColumnMappings.Add("OffTakeAreaDescription", "OffTakeAreaDescription");
                    bcp.ColumnMappings.Add("OffTakeCountry", "OffTakeCountry");
                    bcp.ColumnMappings.Add("OffTakeDate", "OffTakeDate");
                    bcp.ColumnMappings.Add("OffTakeOwner", "OffTakeOwner");
                    bcp.ColumnMappings.Add("OffTakePort", "OffTakePort");
                    bcp.ColumnMappings.Add("OffTakeRegion", "OffTakeRegion");
                    bcp.ColumnMappings.Add("OffTakeState", "OffTakeState");
                    bcp.ColumnMappings.Add("OffTakeStsVessel", "OffTakeStsVessel");
                    bcp.ColumnMappings.Add("OffTake_sts_imo", "OffTake_sts_imo");
                    bcp.ColumnMappings.Add("OfftakeTradingWeek", "OfftakeTradingWeek");
                    bcp.ColumnMappings.Add("OfftakeTradingYear", "OfftakeTradingYear");
                    bcp.ColumnMappings.Add("OpecNopec", "OpecNopec");
                    bcp.ColumnMappings.Add("Probability", "Probability");
                    bcp.ColumnMappings.Add("ProbabilityGroup", "ProbabilityGroup");
                    bcp.ColumnMappings.Add("Processed", "Processed");
                    bcp.ColumnMappings.Add("Projection", "Projection");
                    bcp.ColumnMappings.Add("Route", "Route");
                    bcp.ColumnMappings.Add("Shipper", "Shipper");
                    bcp.ColumnMappings.Add("ShipToShipped", "ShipToShipped");
                    bcp.ColumnMappings.Add("Source", "Source");
                    bcp.ColumnMappings.Add("Speed", "Speed");
                    bcp.ColumnMappings.Add("Sulfur", "Sulfur");
                    bcp.ColumnMappings.Add("Vessel", "Vessel");
                    bcp.ColumnMappings.Add("VesselClass", "VesselClass");
                    bcp.ColumnMappings.Add("VesselClassDescription", "VesselClassDescription");
                    bcp.ColumnMappings.Add("VesselFlag", "VesselFlag");
                    bcp.ColumnMappings.Add("VesselType", "VesselType");
                    bcp.ColumnMappings.Add("WeightMt", "WeightMt");
                    bcp.WriteToServer(reader);
                }
                return new TaskResult(toAdd.Count, toRemove.Count, response.Records.Count > 0 ? response.Records.OrderByDescending(o => o.DateNum).First().DateNum : 0);
            }
        }

        public static void DeleteList(List<int> idList)
        {
            WriteToLog($"Deleting {idList.Count()} rows.");
            foreach (var batch in idList.Batch(500))
            {
                string joined = string.Join(",", batch);
                using (var connection = ConnectionFactory.GetOpenConnection())
                {
                    var sql = $"DELETE FROM [ClipperStaging] WHERE Rownum in ({joined})";
                    connection.Execute(sql);
                }
            }
        }

        public static void WriteToLog(string msg)
        {
            Debug.WriteLine(msg);
            using (var connection = ConnectionFactory.GetOpenConnection())
            {
               // var sql = @"INSERT INTO ImportLog (Source,Message) Values (@source,@message)";
              //  connection.Execute(sql, new {source = "Clipper", message = msg});
            }
        }

        private string GetCurrentFeedType(string type)
        {
            return $"measuresGlobal{type.Replace("global_", "")}Entity";
        }

        internal int GetMaxDatenum(string clipperFeedName)
        {
            using (var connection = ConnectionFactory.GetOpenConnection())
            {
                var result = connection.Query<int?>(@"Select max(dateNum) from ClipperStaging where Type = @Type",
                                 new { Type = clipperFeedName })?.FirstOrDefault() ?? 0;
                return result;
            }
        }

        /// <summary>
        /// Parse the json content string as a Response object and displays the list of grade contained in the response.
        /// </summary>
        /// <param name="content"></param>
        private static Response GetPOCOs(string content)
        {
            return JsonConvert.DeserializeObject<Response>(content, Helpers.JsonSerializerSettings);
        }

        /// <summary>
        /// Parse the content string as a JObject and displays the list of grades contained in the response.
        /// </summary>
        /// <param name="content"></param>
        private static void ListGradeDynamic(string content)
        {
            var json = JObject.Parse(content);

            foreach (var record in json["record"])
            {
                var grade =
                    (JProperty)
                    record.FirstOrDefault(recordProperty => (recordProperty as JProperty)?.Name == "grade");

                Console.WriteLine(grade);
            }
        }

        /// <summary>
        /// Show the name of the properties and their JSON type of the JSON content
        /// </summary>
        /// <param name="content"></param>
        public void ShowJsonStructure(string content) => InternalShowJsonContent(JObject.Parse(content));

        private static void InternalShowJsonContent(JToken json, int nesting = 0)
        {
            foreach (var child in json)
            {
                if (child is JObject || child is JArray)
                {
                    WriteToLog(new string('\t', nesting) + $"({child.Type}):");
                    InternalShowJsonContent(child, nesting + 1);
                }

                if (!(child is JProperty)) continue;

                var prop = child as JProperty;

                WriteToLog(new string('\t', nesting) + $"({prop.Value.Type}): \"{prop.Name}\"");

                if (prop.Value.Type == JTokenType.Array || prop.Value.Type == JTokenType.Object)
                {
                    InternalShowJsonContent(prop.Value, nesting + 1);
                }
            }
        }

        private static HttpClientHandler BuildHttpRequestHandler(string username, string password) => new HttpClientHandler { Credentials = new NetworkCredential(username, password) };

        private static Uri BuildBaseLineDataUri(int datenum, int statnum, string type)
        {
            var query = $"datenum={datenum}&statnum={statnum}&type={type}";
            return new UriBuilder(_baseUrl) { Query = query }.Uri;
        }

        private static Uri BuildAppendDataUri(int datenum, string type)
        {
            var query = $"datenum={datenum}&type={type}";
            return new UriBuilder(_baseUrl) { Query = query }.Uri;
        }

        private static string GetAllProperties(object obj)
        {
            return string.Join(
                " ", obj.GetType()
                    .GetProperties()
                    .Select(prop => prop.GetValue(obj)));
        }
    }
}
