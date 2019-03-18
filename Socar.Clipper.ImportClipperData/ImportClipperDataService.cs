using System;
using System.Linq;
using ClipperDataReader;

namespace Socar.Clipper.ImportClipperData
{
    public static class ImportClipperDataService
    {
        public static void ImportData(string url, string targoDb, string username, string password, string types,
            int baseline)
        {
            DataReader dataReader = new DataReader(targoDb, url, username, password);

            int countFailedLogin = 0;
            foreach (string type in types.Split(',').ToList())
            {
                TaskResult apiTaskResult = new TaskResult(1,0,0);
                while ((apiTaskResult.AddCount != 0 || apiTaskResult.RemoveCount  != 0) && countFailedLogin < 10) // Until all records are downloaded
               {
                    string content = String.Empty;

                    if (apiTaskResult.RemoveCount == 10000) // If every record in this batch is a delete, use the max datenum as our maxdatenum
                        content = dataReader.GetData(baseline, type, apiTaskResult.DataSetMaxDateNum);
                    else
                        content = dataReader.GetData(baseline, type, 0);

                    if (content == "Failed Login")
                    {
                        countFailedLogin++;
                        System.Threading.Thread.Sleep(20000);
                        continue;
                    }

                    countFailedLogin = 0;
                    // dataReader.ShowJsonStructure(content);
                    apiTaskResult = dataReader.SaveData(content, type);
                }
                if (countFailedLogin == 10) throw new Exception("Failed to login to Web API");
            }
        }
    }
}
