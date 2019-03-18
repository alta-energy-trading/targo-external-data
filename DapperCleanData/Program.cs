using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using DapperCleanData.Connection;
using DapperCleanData.Models;

namespace DapperCleanData
{
    class Program
    {
        static async void Main(string[] args)
        {
            CleanClipperData cleanClipperData = new CleanClipperData("Data Source=stchgs112;Initial Catalog=STG_Targo;Integrated Security=SSPI;MultipleActiveResultSets=true");
            await cleanClipperData.GetCleanData();
            Console.ReadLine();
        }
    }
}
