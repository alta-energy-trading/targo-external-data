using System;
using System.Collections.Generic;
using System.Data.Entity.SqlServer;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using ClipperDataReader.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

namespace ClipperDataReader
{
    public class DataReader
    {
        private static string _baseUrl;
        private static string _user;
        private static string _pass;
        private static string _currentFeed;
        private readonly DataService _data;

        public static string CurrentFeed => _currentFeed;

        public DataReader(string connectionString, string url, string user, string pass)
        {
            _baseUrl = url;
            _user = user;
            _pass = pass;
            _data = new DataService(connectionString);
        }

        private static readonly JsonSerializerSettings JsonSerializerSettings = new JsonSerializerSettings
        {
            ContractResolver = new DefaultContractResolver
            {
                NamingStrategy = new SnakeCaseNamingStrategy()
            }
        };

        public string GetData(int baseLine, string strType, int overrideMaxDateNum)
        {
            var content = string.Empty;

            using (var handler = BuildHttpRequestHandler(_user, _pass))
            using (var client = new HttpClient(handler))
            {
                string encoded =
                    Convert.ToBase64String(Encoding.GetEncoding("ISO-8859-1").GetBytes(_user + ":" + _pass));
                client.DefaultRequestHeaders.Add("Authorization", "Basic " + encoded);

                _currentFeed = GetCurrentFeedType(strType);
                var maxDateNum = overrideMaxDateNum == 0 ? _data.GetMaxDatenum(CurrentFeed) : overrideMaxDateNum;

                var url = baseLine == 1
                    ? BuildBaseLineDataUri(maxDateNum, 0, strType)
                    : BuildAppendDataUri(maxDateNum, strType);

                try
                {
                    content = client.GetStringAsync(url).Result;
                }
                catch (AggregateException ex)
                {
                    AggregateException aggregateException = ex.Flatten();
                    for (int i = 0; i < aggregateException.InnerExceptions.Count; ++i)
                    {
                        var canceledException =
                            aggregateException.InnerExceptions[i];

                        if (canceledException.Message.Contains("407") || canceledException.Message.Contains("401"))
                        {
                            content = "Failed Login";
                        }
                        else throw new Exception(canceledException.Message);
                    }
                }
                catch (Exception e)
                {
                    if (e.Message.Substring(0, 8).Contains("Unexpect") || e.Message.Contains("401"))
                    {
                        content = "Failed Login";
                    }
                    else throw new Exception(e.Message);
                }
            }
            return content;
        }

        public TaskResult SaveData(string content, string type)
        {
            return _data.AddClipperRecords(content, GetCurrentFeedType(type));
        }

        private string GetCurrentFeedType(string type)
        {
            return $"measuresGlobal{type.Replace("global_", "")}Entity";
        }

        /// <summary>
        /// Parse the json content string as a Response object and displays the list of grade contained in the response.
        /// </summary>
        /// <param name="content"></param>
        private static Response GetPOCOs(string content)
        {
            return JsonConvert.DeserializeObject<Response>(content, JsonSerializerSettings);
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
                    Debug.WriteLine(new string('\t', nesting) + $"({child.Type}):");
                    InternalShowJsonContent(child, nesting + 1);
                }

                if (!(child is JProperty)) continue;

                var prop = child as JProperty;

                Debug.WriteLine(new string('\t', nesting) + $"({prop.Value.Type}): \"{prop.Name}\"");

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
