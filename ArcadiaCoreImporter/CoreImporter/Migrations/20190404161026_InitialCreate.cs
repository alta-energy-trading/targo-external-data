using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;

namespace CoreImporter.Migrations
{
    public partial class InitialCreate : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "CurrentTurnarounds",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    CAPACITY_OFFLINE_BBL_D = table.Column<string>(nullable: true),
                    PADD_REGION = table.Column<string>(nullable: true),
                    UNIT_TYPE_GROUP = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_CurrentTurnarounds", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "LatestTurnaroundUpdates",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    CAPACITY_OFFLINE = table.Column<string>(nullable: true),
                    MARKET_REGION_NAME = table.Column<string>(nullable: true),
                    OUTAGE_DURATION = table.Column<string>(nullable: true),
                    OUTAGE_END_DATE = table.Column<string>(nullable: true),
                    OUTAGE_ID = table.Column<int>(nullable: false),
                    OUTAGE_PRECISION = table.Column<string>(nullable: true),
                    OUTAGE_START_DATE = table.Column<string>(nullable: true),
                    OUTAGE_STATUS = table.Column<string>(nullable: true),
                    OUTAGE_TYPE = table.Column<string>(nullable: true),
                    OWNER_NAME = table.Column<string>(nullable: true),
                    PARENTNAME = table.Column<string>(nullable: true),
                    PARENT_ID = table.Column<int>(nullable: false),
                    PHYS_CITY = table.Column<string>(nullable: true),
                    PHYS_POSTAL_CODE = table.Column<string>(nullable: true),
                    PLANT_COUNTY_NAME = table.Column<string>(nullable: true),
                    PLANT_ID = table.Column<int>(nullable: false),
                    PLANT_NAME = table.Column<string>(nullable: true),
                    PLANT_PHONE = table.Column<string>(nullable: true),
                    PLANT_STATE_NAME = table.Column<string>(nullable: true),
                    PREV_END_DATE = table.Column<string>(nullable: true),
                    PREV_START_DATE = table.Column<string>(nullable: true),
                    UNIT_CAPACITY = table.Column<string>(nullable: true),
                    UNIT_ID = table.Column<int>(nullable: false),
                    UNIT_NAME = table.Column<string>(nullable: true),
                    UNIT_STATUS = table.Column<string>(nullable: true),
                    UTYPE_DESC = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_LatestTurnaroundUpdates", x => x.Id);
                });
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "CurrentTurnarounds");

            migrationBuilder.DropTable(
                name: "LatestTurnaroundUpdates");
        }
    }
}
