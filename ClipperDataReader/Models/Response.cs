using Newtonsoft.Json;
using System.Collections.Generic;
using ClipperDataReader.Models;

namespace ClipperDataReader.Models
{ 
    public class Response
    {
        [JsonProperty("request_status")]
        public string RequestStatus { get; set; }
        [JsonProperty("record")]
        public ICollection<ClipperData> Records { get; set; }

        public Response()
        {
            Records = new List<ClipperData>();
        }
    }
}
