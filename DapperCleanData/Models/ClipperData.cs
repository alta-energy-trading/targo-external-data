using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DapperCleanData.Models
{
    public class ClipperData
    {
        public int DateNum { get; set; }
        public int RowNum { get; set; }
        public int StatNum { get; set; }
        public string Type { get; set; }
        public string LoadArea { get; set; }
        public string OffTakeArea { get; set; }
        public string OffTakePoint { get; set; }
        public double Api { get; set; }
        public int Bbls { get; set; }
        public int BblsNominal { get; set; }
        public string Bill { get; set; }
        public DateTime? BillDate { get; set; }
        public string BillDescription { get; set; }
        public string Cas { get; set; }
        public string Charter_Grade { get; set; }
        public string Charter_Load_Area { get; set; }
        public string Charter_Offtake_Area { get; set; }
        public string Charterer { get; set; }
        public string CdReport { get; set; }
        public string Consignee { get; set; }
        public string DeclaredDest { get; set; }
        public double? Draught { get; set; }
        public string Fix_Date { get; set; }
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
        public string Lightering_Vessel { get; set; }
        public string LoadAreaDescr { get; set; }
        public string LoadCountry { get; set; }
        public string LoadDate { get; set; }
        public string LoadOwner { get; set; }
        public string LoadPoint { get; set; }
        public string LoadPort { get; set; }
        public string LoadRegion { get; set; }
        public string LoadStsVessel { get; set; }
        public string Load_Sts_Imo { get; set; }
        public string Lon { get; set; }
        public int? LoadTradingWeek { get; set; }
        public int? LoadTradingYear { get; set; }
        public string LzTanker { get; set; }
        public int? Mt { get; set; }
        public int? MtNominal { get; set; }
        public string Notification { get; set; }
        public string OffTakeAreaDescription { get; set; }
        public string OffTakeCountry { get; set; }
        public string OffTakeDate { get; set; }
        public string OffTakeOwner { get; set; }
        public string OffTakePort { get; set; }
        public string OffTakeRegion { get; set; }
        public string OffTakeState { get; set; }
        public string OfftakeStsVessel { get; set; }
        public string Offtake_Sts_Imo { get; set; }
        public int? OffTakeTradingWeek { get; set; }
        public int? OffTakeTradingYear { get; set; }
        public string OpecNopec { get; set; }
        public double Probability { get; set; }
        public string ProbabilityGroup { get; set; }
        public bool Processed { get; set; }
        public string Projection { get; set; }
        public string Route { get; set; }
        public string Shipper { get; set; }
        public string Source { get; set; }
        public double? Speed { get; set; }
        public double Sulfur { get; set; }
        public string Vessel { get; set; }
        public string VesselClass { get; set; }
        public string VesselClassDescription { get; set; }
        public string VesselFlag { get; set; }
        public string VesselType { get; set; }
        public int? WeightMt { get; set; }
        public byte[] ClipperDataRowVersion { get; set; }
        public int? LoadPointId { get; set; }
        public int? OfftakePointId { get; set; }
        public string LoadPointName { get; set; }
        public string OfftakePointName { get; set; }
    }
}
