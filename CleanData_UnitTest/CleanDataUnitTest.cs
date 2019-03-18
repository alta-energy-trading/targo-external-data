using System.Threading.Tasks;
using DapperCleanData;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CleanData_UnitTest
{
    [TestClass]
    public class CleanDataUnitTest
    {
        #region Constant
        private const string ConnectionString = "Data Source=STCHGS112;Initial Catalog=STG_Targo;Integrated Security=SSPI;MultipleActiveResultSets=true";
        #endregion

        #region Test method

        [TestMethod]
        public async Task CleanClipperData()
        {
            DapperCleanData.CleanClipperData cleanClipperData = new CleanClipperData(ConnectionString);
            await cleanClipperData.GetCleanData();
        }

        #endregion
    }
}
