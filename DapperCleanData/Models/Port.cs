namespace DapperCleanData.Models
{
    public class Port
    {
        public int Id { get; set; }
        public int? IdParent { get; set; }
        public int Kind { get; set; }
        public string Name { get; set; }
    }
}
