using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using ClipperDataReader.Models;

namespace ClipperDataReader
{
    [DbConfigurationType(typeof(ClipperDbConfiguration))]
    public class ClipperDataContext : DbContext
    {
        public ClipperDataContext(string connectionString)
            : base(connectionString)
        {
            Configuration.LazyLoadingEnabled = false;
            Database.SetInitializer<ClipperDataContext>(null);
        }

        public DbSet<ClipperData> Records { get; set; }
        public DbSet<ImportLog> Log { get; set; }
    }

    public class ClipperDbConfiguration : DbConfiguration
    {
        public ClipperDbConfiguration()
        {
            this.SetDefaultConnectionFactory(new SqlConnectionFactory());
            this.SetProviderServices("System.Data.SqlClient", System.Data.Entity.SqlServer.SqlProviderServices.Instance);
            this.SetDatabaseInitializer<ClipperDataContext>(null);
        }
    }
}
