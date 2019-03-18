using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ImportData.PetroLogistics.Models
{
    public class Movement
    {
        [Key]
        public long cargo_id { get; set; }
        public string query { get; set; }
        public string tankers_name { get; set; }
        public string loadport_date { get; set; }
        public string load_port { get; set; }
        public string load_country { get; set; }
        public string ports_report_group { get; set; }
        public string load_port_area { get; set; }
        public string qty_tonnes { get; set; }
        public string qty_bbl { get; set; }
        public string c_f { get; set; }
        public string cargo_type { get; set; }
        public string cargo_grade { get; set; }
        public string pc { get; set; }
        public string dischargeport_date { get; set; }
        public string discharge_port { get; set; }
        public string second_discharge_port { get; set; }
        public string via { get; set; }
        public string discharge_country { get; set; }
        public string discharge_area { get; set; }
        public string supplier_name { get; set; }
        public string middle_man { get; set; }
        public string company_name { get; set; }
        public string supplier_note { get; set; }
        public string customer_note { get; set; }
        public string note { get; set; }
        public string client_cargo_status { get; set; }
    }

    public class Envelope
    {
        public IList<IList<object>> header { get; set; }
        public IList<Movement> movements { get; set; }
    }

    public class Response
    {
        public Envelope envelope { get; set; }
    }


}
