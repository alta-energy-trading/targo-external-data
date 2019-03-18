using System;
using System.Collections.Generic;
using System.Linq;
namespace DapperCleanData.Models
{
    public class Mapping
    {
        public int Id { get; private set; }
        public string MapType { get; private set; }
        public string ExternalValue { get; private set; }
        public string TargoValue { get; private set; }
    }
}
