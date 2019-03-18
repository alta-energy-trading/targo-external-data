using ImportData.PetroLogisticsService;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ImportData.UnitTests
{
    [TestClass]
    public class PetroLogisticsUnitTest
    {
        #region Constant
        private const string ConnectionString = "Data Source=STCHGS112;Initial Catalog=STG_Targo;Integrated Security=SSPI;MultipleActiveResultSets=true";
        private const string User = @"ragarwal_http_s1BP49bfzV9M";
        private const string Pass = @"39Bsl15qj5CDRYVkS34AOTW49An2AShO";
        private const string Key = @"b15ex3kdz006p3761wdt56qi";
        private const string Hash = @"c5WtePpLssFsjIkJnj2jC9XhrolyXyicUWBltvEOf08m2Dq87lkgGw2OyvFwZuP7";
        private const string Path = "/api/movementsdata.php";
       private const string Query = "Oman_CO_2018_Present|OPEC_CN_2016|Oman_CO_2017";
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
