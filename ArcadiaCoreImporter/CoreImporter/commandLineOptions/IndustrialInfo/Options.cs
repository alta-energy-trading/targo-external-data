using CommandLine;

namespace CoreImporter.IndustrialInfo
{
    public class Options
    {
        [Option('u', "user", Required= false, HelpText = "The user name to login to IIR", Default="middleditchg19")]
        public string UserName { get; set; }

        [Option('p', "password", Required= false, HelpText = "The password to login to IIR", Default="Energy19!")]
        public string Password { get; set; }
    }
}