using Newtonsoft.Json.Serialization;

namespace ClipperDataReader
{
    public class CustomDataContractResolver : DefaultContractResolver
    {
        private readonly string _clipperFeedName;
        public CustomDataContractResolver(string clipperFeedName)
        {
            _clipperFeedName = clipperFeedName;
        }

        protected override string ResolvePropertyName(string propertyName)
        {
            if(propertyName == "Pk") return _clipperFeedName + "Pk";
            return propertyName;
        }
    }
}
