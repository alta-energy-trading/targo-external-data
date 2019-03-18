Create Table [dbo].[CargoAnalysis] (
	IdStem int NOT NULL
	, IdRegionTree int
	, RegionTree varchar(25)
	, Source varchar(25)
	, IdCleanGrade int
	, CleanGrade varchar(100)
	, IdCleanGradeParent int
	, CleanGradeParent varchar(100)
	, LoadPoint varchar(255)
	, LoadPort varchar(255)
	, LoadCountry varchar(255)
	, LoadSubRegion varchar(255)
	, LoadRegion varchar(255)
	, DischargePoint varchar(255)
	, DischargePort varchar(255)
	, DischargeRefinery varchar(255)
	, DischargeCountry varchar(255)
	, DischargeSubRegion varchar(255)
	, DischargeRegion varchar(255)
	, CleanQuantity Decimal
	, IdCleanUnit int
	, CleanUnit varchar(25)
	, Chain varchar(255)
	, CurrentOwner varchar(255)
	, Keeper varchar(255)
	, LastPrice varchar(255)
	, LastBasis varchar(255)
	, EquityHolder varchar(255)
	, Imo varchar(50)
	, VesselName varchar(255)
	, ShipClass varchar(55)
	, Api Decimal(18,5)
	, Sulphur Decimal (18,5)
	, Midpoint Date
CONSTRAINT [PK_CargoAnalysis] PRIMARY KEY CLUSTERED 
(
	[Source],
	[IdStem],
	[RegionTree],
	[CleanUnit]
))