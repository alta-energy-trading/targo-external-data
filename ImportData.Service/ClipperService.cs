using ImportData.Clipper;

namespace ImportData.ClipperService
{
    public static class ClipperService
    {
        public static void ImportData(string url, string targoDb, string username, string password, string types,
            int baseline)
        {
            Importer importer = new Importer(targoDb, url, username, password);
            importer.Import(types,baseline);
        }
    }
}
