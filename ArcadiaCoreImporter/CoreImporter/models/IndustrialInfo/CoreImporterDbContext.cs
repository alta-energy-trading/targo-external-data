using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.FileExtensions;
using Microsoft.Extensions.Configuration.Json;
using System.IO;
using System;

namespace CoreImporter.IndustrialInfo
{
    public class CoreImporterDbContext : DbContext
    {
        // dotnet ef migrations script
        public DbSet<CurrentTurnaround> CurrentTurnarounds { get; set; }
        public DbSet<LatestTurnaroundUpdate> LatestTurnaroundUpdates { get; set; }
        public string ConnectionString {get; set;}

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(Path.Combine(Directory.GetCurrentDirectory()))
                .AddJsonFile("appsettings.json", optional: false)
                .Build();

            var connectionString = config.GetConnectionString("Value");
            
            if(connectionString != null)
            {
                Console.WriteLine($"Connecting to {connectionString}");
                optionsBuilder
                    .UseSqlServer(config.GetConnectionString("Value"));
            }
            else
            {
                Console.WriteLine($"Missing appsettings.json file in {Directory.GetCurrentDirectory()}" + Environment.NewLine +
                    $"Connecting to Data Source=Lon-PC53;Initial Catalog=RefineryInfo;Integrated Security=SSPI;");
                optionsBuilder.UseSqlServer(
                    "Data Source=Lon-PC53;Initial Catalog=RefineryInfo;Integrated Security=SSPI;"
                );
            }
        }
    }
}