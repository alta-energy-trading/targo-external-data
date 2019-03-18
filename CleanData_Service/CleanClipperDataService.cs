using System.Threading.Tasks;
using DapperCleanData;

namespace CleanData_Service
{
    public static class CleanClipperDataService
    {
        public static void CleanData(string targoDb)
        {
            CleanClipperData cleanClipperData = new CleanClipperData(targoDb);
            cleanClipperData.GetCleanData().Wait();
        }
    }
}
