using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace ClipperDataReader.Models
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
