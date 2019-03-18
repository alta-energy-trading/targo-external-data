using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace ImportData.Clipper.Models
{
    [Table("ImportLog")]
    public class ImportLog
    {
        [Key]
        public int Id { get ; set; }
        public DateTime Timestamp { get; set; }
        public string Source { get; set; }
        public string TableName { get; set; }
        public string Action { get; set; }
        public int IdRecord { get; set; }
        public int ExtRef { get; set; }
    }
}
