using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Newtonsoft.Json;

namespace ImportData.Clipper.Models
{
    [Table("ClipperStaging")]
    public class ClipperStaging
    {
        public ClipperStaging()
        {
            Pk = new PrimaryKey();
        }

        [NotMapped]
        public PrimaryKey Pk { get; set; }

        [Key, Column(Order = 0), JsonIgnore]
        public int DateNum { get { return Pk.DateNum; } set { Pk.DateNum = value; } }
        [Key, Column(Order = 1), JsonIgnore]
        public int RowNum { get { return Pk.RowNum; } set { Pk.RowNum = value; } }
        [Key, Column(Order = 2), JsonIgnore]
        public int StatNum { get { return Pk.StatNum; } set { Pk.StatNum = value; } }
        public string Type { get; set; }

        public string LoadArea { get; set; }

        public string OffTakeArea { get; set; }

        public string OffTakePoint { get; set; }

        public float Api { get; set; }

        public int Bbls { get; set; }

        public int BblsNominal { get; set; }

        public string Bill { get; set; }

        public DateTime? BillDate { get; set; }

        public string BillDescription { get; set; }

        public string Cas { get; set; }

        public string charter_grade { get; set; }

        public string charter_load_area { get; set; }

        public string charter_offtake_area { get; set; }

        public string charterer { get; set; }

        public string CdReport { get; set; }

        public string Consignee { get; set; }

        public string declaredDest { get; set; }

        public float? draught { get; set; }

        public string fix_date { get; set; }

        public string Grade { get; set; }

        public string GradeApi { get; set; }

        public string GradeCountry { get; set; }

        public string GradeRegion { get; set; }

        public string GradeSubtype { get; set; }

        public string GradeType { get; set; }

        public string GradeSulfur { get; set; }

        public int Imo { get; set; }

        public string Lat { get; set; }

        public string Laycan { get; set; }

        public string Lightering_vessel { get; set; }

        public string LoadAreaDescr { get; set; }

        public string LoadCountry { get; set; }

        public string LoadDate { get; set; }

        public string LoadOwner { get; set; }

        public string LoadPoint { get; set; }

        public string LoadPort { get; set; }

        public string LoadRegion { get; set; }

        public string LoadStsVessel { get; set; }

        public string Load_sts_imo { get; set; }

        public string Lon { get; set; }

        public int? LoadTradingWeek { get; set; }

        public int? LoadTradingYear { get; set; }

        public string LzTanker { get; set; }

        public int? mt { get; set; }

        public int? mtNominal { get; set; }

        public string Notification { get; set; }

        public string OffTakeAreaDescription { get; set; }

        public string OffTakeCountry { get; set; }

        public string OffTakeDate { get; set; }

        public string OffTakeOwner { get; set; }

        public string OffTakePort { get; set; }

        public string OffTakeRegion { get; set; }

        public string OffTakeState { get; set; }

        public string OffTakeStsVessel { get; set; }

        public string OffTake_sts_imo { get; set; }

        public int? OfftakeTradingWeek { get; set; }

        public int? OfftakeTradingYear { get; set; }

        public string OpecNopec { get; set; }

        public float Probability { get; set; }

        public string ProbabilityGroup { get; set; }

        public bool Processed { get; set; }

        public string Projection { get; set; }

        public string Route { get; set; }

        public string Shipper { get; set; }

        public int? ShipToShipped { get; set; }

        public string Source { get; set; }

        public float? Speed { get; set; }

        public float Sulfur { get; set; }

        public string Vessel { get; set; }

        public string VesselClass { get; set; }

        public string VesselClassDescription { get; set; }

        public string VesselFlag { get; set; }

        public string VesselType { get; set; }

        public int? WeightMt { get; set; }

        public DateTime RowVersion { get; set; }
    }

    public class PrimaryKey
    {
        public int DateNum { get; set; }
        public int RowNum { get; set; }
        public int StatNum { get; set; }
    }
}
