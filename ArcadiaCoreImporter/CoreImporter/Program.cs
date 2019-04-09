using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Net.Http;
using Microsoft.AspNetCore.WebUtilities;
using System.Net.Http.Headers;
using System.Collections.Generic;
using Newtonsoft.Json;
using CommandLine;
using CoreImporter.IndustrialInfo;
using System.Linq;

namespace CoreImporter
{
    class Program
    {
        private static readonly HttpClient client = new HttpClient();
        private static readonly string baseUrl = "https://www.industrialinfo.com/dash/api/";
        private static readonly string loginTokenUrl = "https://www.industrialinfo.com/dash/api/createLoginToken.jsp";
        private static string currentTurnaroundsEndPoint = "currentTurnarounds.jsp";
        private static string latestTurnaroundUpdatesEndPoint = "latestTurnaroundUpdates.jsp";

        static void Main(string[] args)
        {  
            Parser.Default.ParseArguments<Options>(args)
                .WithParsed<Options>(opts => RunImportIndutrialInfo(opts));
        }

        private static void RunImportIndutrialInfo(Options opts)
        {            
            LoginToken token = GetAuthorizationToken(opts.UserName, opts.Password, loginTokenUrl);
            client.DefaultRequestHeaders.Add("Authorization", "Bearer " + token.access_token);
            
            var currentTurnaroundsUrl = $"{baseUrl}{currentTurnaroundsEndPoint}";
            SaveCurrentTurnArounds(ProcessCurrentTurnaroundData(token, currentTurnaroundsUrl).Result);

            var latestTurnaroundUpdatesUrl = $"{baseUrl}{latestTurnaroundUpdatesEndPoint}";
            SaveLatestTurnaroundUpdates(ProcessLatestTurnaroundUpdateData(token, latestTurnaroundUpdatesUrl).Result);

            Console.WriteLine($"Complete");
        }

        private static LoginToken GetAuthorizationToken(string user, string password, string url)
        {
            Console.WriteLine($"Getting login token from {url}");
            string token = string.Empty;

            Dictionary<string, string> paramsToAdd = new System.Collections.Generic.Dictionary<string, string> {
                {"loginuser",  user},
                {"loginpassword", password},
                {"productNumber", "OFFLINEEVENT_WEB"}
            };

            var stringTask = GetResponseBody(url, paramsToAdd).Result;

            var deserializedLogintoken = JsonConvert.DeserializeObject<LoginToken>(stringTask);

            return deserializedLogintoken;
        }


        private static async Task<string> GetResponseBody(string url, Dictionary<string, string> paramsToAdd)
        {
            var queryDictionary = Microsoft.AspNetCore.WebUtilities.QueryHelpers.AddQueryString(url, paramsToAdd);
            HttpResponseMessage response = await client.GetAsync(queryDictionary);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync();
        }
        
        // TODO make these methods more generic
        #region Process and Save Data
        private static async Task<ICollection<CurrentTurnaround>> ProcessCurrentTurnaroundData(LoginToken token, string url)
        {
            Console.WriteLine($"Getting current turnarounds from {url}");
            var stringTask = await client.GetStringAsync(url);

            var data = JsonConvert.DeserializeObject<CurrentTurnaroundData>(stringTask);

            return data.Records;
        }

        private static void SaveCurrentTurnArounds(ICollection<CurrentTurnaround> currentTurnarounds)
        {
            var now = DateTime.Now;
            using (var context = new CoreImporterDbContext())
            {
                Console.WriteLine($"Saving current turnarounds");
                // TODO merge
                currentTurnarounds.ToList().ForEach(e => e.LoadDate = now);
                context.CurrentTurnarounds.AddRange(currentTurnarounds);
                
                context.SaveChanges();
            }
        }

        private static async Task<ICollection<LatestTurnaroundUpdate>> ProcessLatestTurnaroundUpdateData(LoginToken token, string url)
        {
            Console.WriteLine($"Getting latest turnaround updates from {url}");
            var stringTask = await client.GetStringAsync(url);

            var data = JsonConvert.DeserializeObject<LatestTurnaroundUpdateData>(stringTask);

            return data.Records;
        }

        private static void SaveLatestTurnaroundUpdates(ICollection<LatestTurnaroundUpdate> latestTurnaroundUpdates)
        {
            var now = DateTime.Now;
            using (var context = new CoreImporterDbContext())
            {
                Console.WriteLine($"Saving latest turnaround updates");
                // TODO merge
                latestTurnaroundUpdates.ToList().ForEach(e => e.LoadDate = now);
                context.LatestTurnaroundUpdates.AddRange(latestTurnaroundUpdates);
                
                context.SaveChanges();
            }
        }
        #endregion
    }
}
