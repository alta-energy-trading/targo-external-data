namespace DapperCleanData.Models
{
    public class Grade
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int? IdParent { get; set; }
    }
}
