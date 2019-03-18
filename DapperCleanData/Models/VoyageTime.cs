namespace DapperCleanData.Models
{
    public class VoyageTime
    {
        public int? Id { get; set; }
        public int IdLoadPort { get; set; }
        public int IdDischargePort { get; set; }
        public int? IdShippingRoute { get; set; }
        public string VesselClass { get; set; }
        public int? DaysMinimum { get; set; }
        public int? Month { get; set; }
        public bool? IsUpdated { get; set; }
    }
}
