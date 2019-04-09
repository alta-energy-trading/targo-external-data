using System;
using Microsoft.EntityFrameworkCore.SqlServer;
using System.ComponentModel.DataAnnotations;

namespace CoreImporter.IndustrialInfo
{
    public class CurrentTurnaround
    {
        [Key]
        public int Id
        {
            get; set;
        }
        public string CAPACITY_OFFLINE_BBL_D { get; set; }
        public string PADD_REGION { get; set; }
        public string UNIT_TYPE_GROUP { get; set; }
        public DateTime LoadDate {get; set;}
    }
}