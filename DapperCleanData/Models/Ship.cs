using System;

namespace DapperCleanData.Models
{
    public class Ship
    {
        public int Id { get; set; }
        public string Imo { get; set; }
        public string ShipClass { get; set; }
        public int? IdGrade { get; set; }
        public int? DeadweightTons { get; set; }
        public bool IsDeleted { get; set; }
        public int Source { get; set; }
        public string Name { get; set; }
        public DateTime? ValidUntil { get; set; }
    }
}
