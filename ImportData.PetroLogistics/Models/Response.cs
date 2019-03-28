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
        public string tanker_name { get; set; }
        public string tanker_imo { get; set; }
        public string tanker_dwt { get; set; }
        public string tanker_flag { get; set; }
        public string tanker_owner { get; set; }
        public string load_terminal { get; set; }
        public string grade_confidence { get; set; }
        public string company_confidence { get; set; }
        public string quality_category { get; set; }
        public string transit_time { get; set; }
        public string loadport_date { get; set; }
        public string load_port { get; set; }
        public string load_country { get; set; }
        public string report_group { get; set; }
        public string load_port_area { get; set; }
        public string load_port_date { get; set; }
        public string qty_tonnes { get; set; }
        public string qty_barrels { get; set; }
        public string c_f { get; set; }
        public string cargo_type { get; set; }
        public string cargo_grade { get; set; }
        public string pc { get; set; }
        public string discharge_port_date { get; set; }
        public string discharge_port { get; set; }
        public string second_discharge_port { get; set; }
        public string via { get; set; }
        public string discharge_country { get; set; }
        public string discharge_area { get; set; }
        public string supplier { get; set; }
        public string middle_man { get; set; }
        public string customer { get; set; }
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
