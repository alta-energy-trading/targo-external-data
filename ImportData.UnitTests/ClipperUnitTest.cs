using ImportData.ClipperService;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ImportData.UnitTests
{
    [TestClass]
    public class ClipperUnitTest
    {
        #region Constant
        private const string Url = "http://appserver.clipperdata.com:8080/ClipperDataAPI-2/rest/clipperapi/data/";
        private const string ConnectionString = "Data Source=STCHGS112;Initial Catalog=STG_Targo;Integrated Security=SSPI;MultipleActiveResultSets=true";
        private const string User = "henry_tindal";
        private const string Pass = "htindal887";
        private const string Types = "global_crude";
        private const string PathToBin = @"C:\temp\ImportData.Clipper\bin";
        private const int Baseline = 0;
        #endregion

        #region Test method

        [TestMethod]
        public void ImportClipperData()
        {
            ClipperService.ClipperService.ImportData(Url, ConnectionString, User, Pass, Types, Baseline);
        }

        #endregion
    }
}
