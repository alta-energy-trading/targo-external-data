using Microsoft.VisualStudio.TestTools.UnitTesting;
using Socar.Clipper.ImportClipperData;

namespace ImportClipperData.UnitTest
{
    [TestClass]
    public class ImportClipperDataUnitTest
    {
        #region Constant
        private const string Url = "http://appserver.clipperdata.com:8080/ClipperDataAPI-2/rest/clipperapi/data/";
        private const string ConnectionString = "Data Source=STCHGS112;Initial Catalog=STG_Targo;Integrated Security=SSPI;MultipleActiveResultSets=true";
        private const string User = "henry_tindal";
        private const string Pass = "htindal887";
        private const string Types = "global_crude";
        private const string PathToBin = @"C:\Dev\Projects.Git\Socar.Clipper\Socar.Clipper.ImportClipperData\bin\Debug";
        private const int Baseline = 0;
        #endregion

        #region Test method

        [TestMethod]
        public void ImportClipperData()
        {
            ImportClipperDataService.ImportData(Url,ConnectionString,User,Pass,Types,Baseline);
        }

        #endregion
    }
}
