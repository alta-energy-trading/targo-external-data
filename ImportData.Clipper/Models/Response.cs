using Newtonsoft.Json;
using System.Collections.Generic;

namespace ImportData.Clipper.Models
{ 
    public class Response
    {
        [JsonProperty("request_status")]
        public string RequestStatus { get; set; }
        [JsonProperty("record")]
        public ICollection<ClipperStaging> Records { get; set; }

        public Response()
        {
            Records = new List<ClipperStaging>();
        }
    }
}
