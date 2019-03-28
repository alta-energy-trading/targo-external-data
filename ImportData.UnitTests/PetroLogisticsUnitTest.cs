using ImportData.PetroLogisticsService;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ImportData.UnitTests
{
    [TestClass]
    public class PetroLogisticsUnitTest
    {
        #region Constant
        private const string ConnectionString = "Data Source=Lon-PC53;Initial Catalog=STG_Targo;Integrated Security=SSPI;MultipleActiveResultSets=true";
        private const string User = @"garym_http_A0gtFsUMb234";
        private const string Pass = @"Y67jAi3UOoKWIJF781pXXz7IKC69r7Wx";
        private const string Key = @"iaut20xif17ly51xc5330n67";
        private const string Hash = @"vqDLzZiQwuKx4m7eplhguyEBZ0jMq70mDOhFG1cLQkl9xqfiPN1Fe9vBgkL8hntx";
        private const string Path = "/api/v2/movementsdata.php";
       private const string Query = "Iraq_CO_2019_Present";
        // private const string Query = "";
        private const string Domain = @"https://secure.petro-logistics.com";
        #endregion

        #region Test method

        [TestMethod]
        public void ImportPetroLogisticsData()
        {
            PetroLogisticsService.PetroLogisticsService.ImportData(ConnectionString, User, Pass, Key, Hash, Domain, Path, Query);
        }

        #endregion
    }
}
