using ImportData.PetroLogistics;

namespace ImportData.PetroLogisticsService
{
    public static class PetroLogisticsService
    {
        public static void ImportData(string connectionString, string user, string pass, string key, string hash,
            string domain, string path, string query)
        {
            Importer importer = new Importer(connectionString, user, pass, key, hash, domain, path, query);
            importer.Import();
        }
    }
}
