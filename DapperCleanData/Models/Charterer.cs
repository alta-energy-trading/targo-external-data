namespace DapperCleanData.Models
{
    public class Charterer
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public int? IdParent { get; set; }
    }
}
