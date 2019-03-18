using System;

namespace DapperCleanData.Models
{
    public class ClipperCleanerData
    {
        public long Id { get; set; }
        public int DateNum { get; set; }
        public int RowNum { get; set; }
        public int StatNum { get; set; }
        public string CargoId { get; set; }
        public bool ChartererInfo { get; set; }
        public DateTime? DischargeDate { get; set; }
        public double? Draught { get; set; }
        public DateTime? EndLaycan { get; set; }
        public DateTime? FixDate { get; set; }
        public int? IdCharterer { get; set; }
        public int? IdClipperCharterer { get; set; }
        public int? IdDestination { get; set; }
        public int? IdDisport { get; set; }
        public int? IdEquity { get; set; }
        public int IdGrade { get; set; }
        public int? IdLoadLocation { get; set; }
        public int? IdLoadPort { get; set; }
        public int? IdShip { get; set; }
        public int? IdShippingRoute { get; set; }
        public int? IdUnitOfMeasure { get; set; }
        public int? IdVoyageCostType { get; set; }
        public DateTime? LoadDate { get; set; }
        public int? Month { get; set; }
        public string Notes { get; set; }
        public bool? Pipeline { get; set; }
        public bool Processed { get; set; }
        public double Quantity { get; set; }
        public string Settings { get; set; }
        public bool ShipToShip { get; set; }
        public bool? Short { get; set; }
        public int? Source { get; set; }
        public double? Speed { get; set; }
        public DateTime? StartLaycan { get; set; }
        public bool ToFloatingStorage { get; set; }
        public bool ToLandStorage { get; set; }
        public decimal? VoyageCost { get; set; }
        public int? Year { get; set; }
        public DateTime? CreationDate { get; set; }
        public string CreationName { get; set; }
        public DateTime? RevisionDate { get; set; }
        public string RevisionName { get; set; }
        public byte[] ClipperDataRowVersion { get; set; }
        public string LoadPointName { get; set; }
        public string OfftakePointName { get; set; }
        public int? LoadPointId { get; set; }
        public int? OfftakePointId { get; set; }
    }
}
