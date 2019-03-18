using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace ImportData.Clipper
{
    public static class Helpers
    {
        public static readonly JsonSerializerSettings JsonSerializerSettings = new JsonSerializerSettings
        {
            ContractResolver = new DefaultContractResolver
            {
                NamingStrategy = new SnakeCaseNamingStrategy()
            }
        };
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

    public class CustomDataContractResolver : DefaultContractResolver
    {
        private readonly string _clipperFeedName;
        public CustomDataContractResolver(string clipperFeedName)
        {
            _clipperFeedName = clipperFeedName;
        }

        protected override string ResolvePropertyName(string propertyName)
        {
            if (propertyName == "Pk") return _clipperFeedName + "Pk";
            return propertyName;
        }
    }
}
