namespace DapperCleanData.Models
{
    public class CleanerDataValidation
    {
        public int Id { get; set; }
        public string SourceName { get; set; }
        public int ExtRef { get; set; }
        public bool? IsSuccess { get; set; }
        public string Value { get; set; }
        public string Message { get; set; }
    }
}
