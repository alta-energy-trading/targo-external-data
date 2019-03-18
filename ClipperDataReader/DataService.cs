using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Linq;
using ClipperDataReader.Models;
using Newtonsoft.Json;

namespace ClipperDataReader
{
    public class DataService
    {
        private readonly string _connectionString;

        public DataService(string connectionString)
        {
            _connectionString = connectionString;
        }

        internal int GetMaxDatenum(string clipperFeedName)
        {
            using (var db = new ClipperDataContext(_connectionString))
            {
                if (db.Records.Any(r => r.Type == clipperFeedName))
                    return db.Records.Where(r => r.Type == clipperFeedName).Select(d => d.DateNum).Max();
                return 0;
            }
        }

        internal TaskResult AddClipperRecords(string content, string clipperFeedName)
        {
            using (var db = new ClipperDataContext(_connectionString))
            { 
                var response = JsonConvert.DeserializeObject<Response>(content, new JsonSerializerSettings
                {
                    ContractResolver = new CustomDataContractResolver(clipperFeedName)
                });

                var toAdd = response.Records.Where(r => r.StatNum == 0).ToList();
                db.Records.AddRange(toAdd);

                var logAdds = (from x in toAdd
                    select new ImportLog
                    {
                        Source = "Clipper",
                        TableName = "ClipperStaging",
                        Action = "Insert",
                        IdRecord = x.DateNum,
                        ExtRef = x.RowNum
                    });

                db.Log.AddRange(logAdds);

                var toRemove = response.Records.Where(r => r.StatNum == 1).Select(r => r.RowNum).ToList();
                db.Records.RemoveRange(db.Records.Where(r => toRemove.Contains(r.RowNum)));

                var logDeletes = (from x in toAdd
                    select new ImportLog
                    {
                        Source = "Clipper",
                        TableName = "ClipperStaging",
                        Action = "Delete",
                        IdRecord = x.DateNum,
                        ExtRef = x.RowNum
                    });

                try
                {
                    db.SaveChanges();
                }
                catch (System.Data.Entity.Validation.DbEntityValidationException dbEx)
                {
                    Exception raise = dbEx;
                    foreach (var validationErrors in dbEx.EntityValidationErrors)
                    {
                        foreach (var validationError in validationErrors.ValidationErrors)
                        {
                            string message = string.Format("{0}:{1}", validationErrors.Entry.Entity.ToString(), validationError.ErrorMessage);
                            //raise a new exception inserting the current one as the InnerException
                            raise = new InvalidOperationException(message, raise);
                        }
                    }
                    throw raise;
                }
                catch (Exception ex)
                {
                    throw new Exception($"{ex.Message} Inner Exception: {ex.InnerException?.Message} Inner Inner Exception: {ex.InnerException?.InnerException?.Message} Inner Inner Inner Exception: {ex.InnerException?.InnerException?.InnerException?.Message} Stack Trace: {ex.StackTrace}");
                }

                return new TaskResult(toAdd.Count,toRemove.Count, toRemove.Count == 10000 ? response.Records.OrderByDescending(o => o.DateNum).First().DateNum : 0) ;
            }
        }
    }

    public class TaskResult
    {
        public TaskResult(int addCount, int removeCount, int dataSetMaxDateNum)
        {
            AddCount = addCount;
            RemoveCount = removeCount;
            DataSetMaxDateNum = dataSetMaxDateNum;
        }

        public int AddCount { get; set; }
        public int RemoveCount { get; set; }
        public int DataSetMaxDateNum { get; set; }
    }
}
