using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace CoreImporter.IndustrialInfo
{
    // TODO make these two classes one generic class
    public class CurrentTurnaroundData
    {
        [JsonProperty("data")]
        public ICollection<CurrentTurnaround> Records { get; set; }
        
        
        public CurrentTurnaroundData()
        {
            Records = new List<CurrentTurnaround>();
        }
    }

    public class LatestTurnaroundUpdateData
    {
        [JsonProperty("data")]
        public ICollection<LatestTurnaroundUpdate> Records { get; set; }
        
        
        public LatestTurnaroundUpdateData()
        {
            Records = new List<LatestTurnaroundUpdate>();
        }
    }
}