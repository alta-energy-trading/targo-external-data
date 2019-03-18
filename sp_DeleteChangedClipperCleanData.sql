Create Procedure [dbo].[sp_DeleteChangedClipperCleanData]

As
create table [#temp_CleanDataLog] (
	IdSource int,
	TableName nvarchar(255),
	[Action] nvarchar(255),
	IdRecord int,
	ExtRef int,
);

CREATE TABLE #temp_ClipperCleanerData(
	[Id] [bigint] NOT NULL,
	[DateNum] [int] NOT NULL,
	[RowNum] [int] NOT NULL,
	[StatNum] [int] NOT NULL,
	[Arbitrage] [bit] NULL,
	[Available] [bit] NULL,
	[CargoId] [nvarchar](50) NULL,	
	[ChartererInfo] [bit] NULL,	
	[ClipperDestinationArea] [varchar](255) NULL,
	[ClipperDestinationRegion] [varchar](255) NULL,
	[Clocked] [bit]NULL,
	[Coordinates] [geography] NULL,	
	[CurrentOffer] [varchar](255) NULL,
	[Deleted] [bit] NULL,
	[DemurrageRate] [numeric](18, 4) NULL,
	[DestinationConfirmed] [bit] NULL,
	[DischargeDate] [date] NULL,
	[Draught] [float] NULL,
	[Dropped] [bit] NULL,
	[EndLaycan] [date] NULL,
	[Error] [varchar](max) NULL,
	[Finalized] [bit] NULL,
	[FixDate] [date] NULL,
	[FloatingStorage] [bit] NULL,	
	[IdCharterer] [int] NULL,
	[IdClipperCharterer] [bigint] NULL,
	[IdClipperDeclaredDestination] [bigint] NULL,
	[IdClipperLoadStsVessel] [bigint] NULL,
	[IdClipperLoadArea] [bigint] NULL,
	[IdClipperLighteringVessel] [bigint] NULL,
	[IdClipperOfftakeStsVessel] [bigint] NULL,
	[IdDataSource] [bigint] NULL,
	[IdDestination] [int] NULL,
	[IdDisport] [int] NULL,
	[IdEquity] [int] NULL,
	[IdGrade] [int] NULL,
	[IdKeeper] [bigint] NULL,
	[IdLoadLocation] [int] NULL,
	[IdLoadPort] [int] NULL,
	[IdParent] [bigint] NULL,
	[IdShip] [int] NULL,
	[IdShippingRoute] [int] NULL,
	[IdStatus] [bigint] NULL,
	[IdUnitOfMeasure] [int] NULL,
	[IdVoyageCostType] [int] NULL,
	[LoadDate] [date] NULL,
	[Month] [int] NULL,
	[Notes] [nvarchar](max) NULL,
	[OriginId] [int] NULL,
	[Pipeline] [bit] NULL,	
	[Processed] [bit] NULL,
	[Quantity] [float] NULL,	
	[RowVersion] [timestamp] NULL,
	[Settings] [varchar](max) NULL,
	[Speed] [float] NULL,
	[ShipToShip] [bit] NULL,
	[Short] [bit] NULL,
	[Source] [int] NULL,
	[StartLaycan] [date] NULL,
	[ToFloatingStorage] [bit] NULL,
	[ToLandStorage] [bit] NULL,
	[VoyageCost] [numeric](18, 4) NULL,
	[Year] [int] NULL,
	[CreationDate] [datetime] NULL,
	[CreationName] [varchar](100) NULL,
	[RevisionDate] [datetime] NULL,
	[RevisionName] [varchar](100) NULL,
	[ClipperDataRowVersion] [varbinary](8) NULL
);

Begin Try
	Begin Tran
	-- get rows to keep
	Insert Into #temp_ClipperCleanerData (
		Id
		, DateNum
		, RowNum
		, StatNum
		, Arbitrage
		, Available
		, CargoId
		, ChartererInfo
		, ClipperDestinationArea
		, ClipperDestinationRegion
		, Clocked
		, Coordinates
		, CurrentOffer
		, Deleted
		, DemurrageRate
		, DestinationConfirmed
		, DischargeDate
		, Draught
		, Dropped
		, EndLaycan
		, Error
		, Finalized
		, FixDate
		, FloatingStorage
		, IdCharterer
		, IdClipperCharterer
		, IdClipperDeclaredDestination
		, IdClipperLoadStsVessel
		, IdClipperLoadArea
		, IdClipperLighteringVessel
		, IdClipperOfftakeStsVessel
		, IdDataSource
		, IdDestination
		, IdDisport
		, IdEquity
		, IdGrade
		, IdKeeper
		, IdLoadLocation
		, IdLoadPort
		, IdParent
		, IdShip
		, IdShippingRoute
		, IdStatus
		, IdUnitOfMeasure
		, IdVoyageCostType
		, LoadDate
		, Month
		, Notes
		, OriginId
		, Pipeline
		, Processed
		, Quantity
		, Settings
		, Speed
		, ShipToShip
		, Short
		, Source
		, StartLaycan
		, ToFloatingStorage
		, ToLandStorage
		, VoyageCost
		, Year
		, CreationDate
		, CreationName
		, RevisionDate
		, RevisionName
		, ClipperDataRowVersion 
	)
	Select	Id
			, DateNum
			, ccd.RowNum
			, StatNum
			, Arbitrage
			, Available
			, CargoId
			, ChartererInfo
			, ClipperDestinationArea
			, ClipperDestinationRegion
			, Clocked
			, Coordinates
			, CurrentOffer
			, Deleted
			, DemurrageRate
			, DestinationConfirmed
			, DischargeDate
			, Draught
			, Dropped
			, EndLaycan
			, Error
			, Finalized
			, FixDate
			, FloatingStorage
			, IdCharterer
			, IdClipperCharterer
			, IdClipperDeclaredDestination
			, IdClipperLoadStsVessel
			, IdClipperLoadArea
			, IdClipperLighteringVessel
			, IdClipperOfftakeStsVessel
			, IdDataSource
			, IdDestination
			, IdDisport
			, IdEquity
			, IdGrade
			, IdKeeper
			, IdLoadLocation
			, IdLoadPort
			, IdParent
			, IdShip
			, IdShippingRoute
			, IdStatus
			, IdUnitOfMeasure
			, IdVoyageCostType
			, LoadDate
			, Month
			, Notes
			, OriginId
			, Pipeline
			, Processed
			, Quantity
			, Settings
			, Speed
			, ShipToShip
			, Short
			, Source
			, StartLaycan
			, ToFloatingStorage
			, ToLandStorage
			, VoyageCost
			, Year
			, CreationDate
			, CreationName
			, RevisionDate
			, RevisionName
			, ccd.ClipperDataRowVersion 
	--Select	*
	From	ClipperCleanerData ccd
			Left Outer Join dbo.ClipperData_Deletes d	
				On	d.RowNum = ccd.RowNum
	Where	d.ClipperDataRowVersion = ccd.ClipperDataRowVersion
	
	-- check rows to be deleted
	Insert	#temp_CleanDataLog (
			IdSource
			, TableName
			, [Action]
			, IdRecord
			, ExtRef
		)
		Select	src.Id
				, 'ClipperCleanerData'
				, 'Delete'
				, cd.Id
				, cd.Rownum
		From	dbo.ClipperCleanerData cd
				Left Outer Join dbo.ClipperData_Deletes d
					On cd.Rownum = d.RowNum
				Left Join SourceType st 
					On	st.Name = 'Stem'
				Inner Join Source src
					On	src.IdType = st.Id
					And	src.Id = cd.Source
					And	src.Name = 'Clipper'
		Where	(d.RowNum IS NULL or d.ClipperDataRowVersion <> cd.ClipperDataRowVersion)

		Insert CleanDataLog (
			IdSource
			, TableName
			, [Action]
			, IdRecord
			, ExtRef
		)
		Select	*
		From	#temp_CleanDataLog

	Truncate Table ClipperCleanerData
	
	-- reinsert records to keep
	Insert Into ClipperCleanerData (
		DateNum
		, RowNum
		, StatNum
		, Arbitrage
		, Available
		, CargoId
		, ChartererInfo
		, ClipperDestinationArea
		, ClipperDestinationRegion
		, Clocked
		, Coordinates
		, CurrentOffer
		, Deleted
		, DemurrageRate
		, DestinationConfirmed
		, DischargeDate
		, Draught
		, Dropped
		, EndLaycan
		, Error
		, Finalized
		, FixDate
		, FloatingStorage
		, IdCharterer
		, IdClipperCharterer
		, IdClipperDeclaredDestination
		, IdClipperLoadStsVessel
		, IdClipperLoadArea
		, IdClipperLighteringVessel
		, IdClipperOfftakeStsVessel
		, IdDataSource
		, IdDestination
		, IdDisport
		, IdEquity
		, IdGrade
		, IdKeeper
		, IdLoadLocation
		, IdLoadPort
		, IdParent
		, IdShip
		, IdShippingRoute
		, IdStatus
		, IdUnitOfMeasure
		, IdVoyageCostType
		, LoadDate
		, Month
		, Notes
		, OriginId
		, Pipeline
		, Processed
		, Quantity
		, Settings
		, Speed
		, ShipToShip
		, Short
		, Source
		, StartLaycan
		, ToFloatingStorage
		, ToLandStorage
		, VoyageCost
		, Year
		, CreationDate
		, CreationName
		, RevisionDate
		, RevisionName
		, ClipperDataRowVersion 
	)
	Select	DateNum
			, RowNum
			, StatNum
			, Arbitrage
			, Available
			, CargoId
			, ChartererInfo
			, ClipperDestinationArea
			, ClipperDestinationRegion
			, Clocked
			, Coordinates
			, CurrentOffer
			, Deleted
			, DemurrageRate
			, DestinationConfirmed
			, DischargeDate
			, Draught
			, Dropped
			, EndLaycan
			, Error
			, Finalized
			, FixDate
			, FloatingStorage
			, IdCharterer
			, IdClipperCharterer
			, IdClipperDeclaredDestination
			, IdClipperLoadStsVessel
			, IdClipperLoadArea
			, IdClipperLighteringVessel
			, IdClipperOfftakeStsVessel
			, IdDataSource
			, IdDestination
			, IdDisport
			, IdEquity
			, IdGrade
			, IdKeeper
			, IdLoadLocation
			, IdLoadPort
			, IdParent
			, IdShip
			, IdShippingRoute
			, IdStatus
			, IdUnitOfMeasure
			, IdVoyageCostType
			, LoadDate
			, Month
			, Notes
			, OriginId
			, Pipeline
			, Processed
			, Quantity
			, Settings
			, Speed
			, ShipToShip
			, Short
			, Source
			, StartLaycan
			, ToFloatingStorage
			, ToLandStorage
			, VoyageCost
			, Year
			, CreationDate
			, CreationName
			, RevisionDate
			, RevisionName
			, ClipperDataRowVersion 
	From	#temp_ClipperCleanerData
	
	IF OBJECT_ID('tempdb.dbo.#temp_ClipperCleanerData', 'U') IS NOT NULL drop Table #temp_ClipperCleanerData
	IF OBJECT_ID('tempdb.dbo.#temp_RownumsToDelete', 'U') IS NOT NULL drop Table #temp_RownumsToDelete

	Commit Tran
End Try
Begin Catch
	If @@TRANCOUNT > 0
		rollback tran
	
	declare @ermessage nvarchar(2048), @erseverity int, @erstate int;
	Select @ermessage = ERROR_MESSAGE(), @erseverity = ERROR_SEVERITY(), @erstate = ERROR_STATE();
	raiserror(@ermessage, @erseverity,@erstate)
	
	IF OBJECT_ID('tempdb.dbo.#temp_ClipperCleanerData', 'U') IS NOT NULL drop Table #temp_ClipperCleanerData
	IF OBJECT_ID('tempdb.dbo.#temp_RownumsToDelete', 'U') IS NOT NULL drop Table #temp_RownumsToDelete
	Truncate Table dbo.ClipperData_Deletes
End Catch