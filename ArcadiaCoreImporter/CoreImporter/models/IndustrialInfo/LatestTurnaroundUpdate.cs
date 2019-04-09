using System;
using System.ComponentModel.DataAnnotations;

namespace CoreImporter.IndustrialInfo
{
    public class LatestTurnaroundUpdate
    {
         [Key]
        public int Id
        {
            get; set;
        }
        public string CAPACITY_OFFLINE {get; set;}
        public string MARKET_REGION_NAME {get; set;}
        public string OUTAGE_DURATION {get; set;}
        public string OUTAGE_END_DATE {get; set;}
        public int OUTAGE_ID {get; set;}
        public string OUTAGE_PRECISION {get; set;}
        public string OUTAGE_START_DATE {get; set;}
        public string OUTAGE_STATUS {get; set;}
        public string OUTAGE_TYPE {get; set;}
        public string OWNER_NAME {get; set;}
        public string PARENTNAME {get; set;}
        public int PARENT_ID {get; set;}
        public string PHYS_CITY {get; set;}
        public string PHYS_POSTAL_CODE {get; set;}
        public string PLANT_COUNTY_NAME {get; set;}
        public int PLANT_ID {get; set;}
        public string PLANT_NAME {get; set;}
        public string PLANT_PHONE {get; set;}
        public string PLANT_STATE_NAME {get; set;}
        public string PREV_END_DATE {get; set;}
        public string PREV_START_DATE {get; set;}
        public string UNIT_CAPACITY {get; set;}
        public int UNIT_ID {get; set;}
        public string UNIT_NAME {get; set;}
        public string UNIT_STATUS {get; set;}
        public string UTYPE_DESC {get; set;}
        public DateTime LoadDate {get; set;}
    }
}