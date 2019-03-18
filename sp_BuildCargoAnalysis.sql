Create Procedure [dbo].[sp_BuildCargoAnalysis]	@stemId varchar(25) = null
												, @tree varchar(25) = null
												, @unit varchar(25) = null
As


Declare @id int = null;
IF @stemId Is Not NULL Set @id = Convert(int, @stemId);

Create table #analysisTemp
(	
	IdStem int NOT NULL
	, IdRegionTree int  NOT NULL
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
	, IdCleanUnit int  NOT NULL
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
)
DECLARE @ImportRows INT;

BEGIN TRY
        Insert Into #analysisTemp (
		IdStem
		, IdRegionTree
		, RegionTree
		, Source
		, IdCleanGrade
		, CleanGrade
		, IdCleanGradeParent
		, CleanGradeParent
        , LoadPoint
        , LoadPort
		, LoadCountry 
		, LoadSubRegion 
		, LoadRegion 
        , DischargePoint
	    , DischargePort
		, DischargeRefinery
		, DischargeCountry 
		, DischargeSubRegion 
		, DischargeRegion 
		, CleanQuantity
		, IdCleanUnit
		, CleanUnit
        , Chain
	    , CurrentOwner
		, Keeper
	    , LastPrice
	    , LastBasis
	    , EquityHolder
	    , Imo
	    , VesselName
	    , ShipClass
		, Api
		, Sulphur
		, Midpoint
	)		
	Select	s.id 'IdStem'
			, Tree.Id 
			, Tree.Name 
			, dataSource.Name 
			, DefaultGrade.Id 
			, DefaultGrade.Name
			, ParentGrade.Id 
			, ParentGrade.Name		
            , LoadRegions.TerminalName
            , LoadRegions.PortName
			, LoadRegions.CountryName
			, LoadRegions.SubRegionName
			, LoadRegions.RegionName
            , DischargeRegions.TerminalName
            , DischargeRegions.PortName
			, Refinery.Name
			, DischargeRegions.CountryName
			, DischargeRegions.SubRegionName
			, DischargeRegions.RegionName
			, CleanQuantity.Quantity
			, CleanUnit.Id
			, CleanUnit.Name
            , deals.Chain
            , ISNULL(deals.CurrentOwner,RefineryOwnerName.Name) 'CurrentOwner'
			, Keeper.Name 'Keeper'
            , deals.LastPrice
            , deals.LastBasis
            , equityHolder.Name
            , Vessel.Imo
            , VesselDetails.name
            , Vessel.ShipClass
			, Coalesce(ApiLevel.NumValue,ApiLevelGlobal.NumValue) 
			, Coalesce(SulphurLevel.NumValue,SulphurLevelGlobal.NumValue)
			, (Select DateAdd(day, DATEDIFF(day, s.StartLaycan, s.EndLaycan)/2, s.StartLaycan)) Midpoint
	From	Stem s		
			Cross Join RegionTree Tree					
	        Left Join Source dataSource 
				On	dataSource.Id = s.source
	        Left Join RegionView LoadRegions 
				On LoadRegions.Id = ISNULL(s.IdLoadLocation,s.IdLoadPort) and LoadRegions.IdView = Tree.Id
	        Left Join RegionView DischargeRegions 
				On DischargeRegions.Id = ISNULL(s.IdDestination,s.IdDisport) and DischargeRegions.IdView = Tree.Id
			Left Join MapType RefineryMapType 
			    On	RefineryMapType.Name = 'Refinery'
		    Outer Apply (
				Select top 1 IdMap 
				From	LocationMap
				Where	IdMapType = RefineryMapType.Id
				And		IdLocation = DischargeRegions.IdTerminal
				And		IdRegionTree = Tree.Id
			) DefaultRefinery
		    Left Join Refinery Refinery
			    On	Refinery.Id = DefaultRefinery.IdMap
			Left Join RefineryOwner RefineryOwner
				On	RefineryOwner.IdRefinery = Refinery.Id
				And	RefineryOwner.ValidUntil is null
			Left Join Counterpart RefineryOwnerName
				On RefineryOwnerName.Id = RefineryOwner.IdOwner
	        Left Join view_DefaultGrades DefaultGradeAtPoint 
		        on	DefaultGradeAtPoint.IdLocation = s.IdLoadLocation 
		        and DefaultGradeAtPoint.IdRegionTree = Tree.Id 
		        and	dataSource.Name <> 'Internal'
	        Left Join view_DefaultGrades DefaultGradeAtPort 
		        on	DefaultGradeAtPort.IdLocation = s.IdLoadPort 
		        and DefaultGradeAtPort.IdRegionTree = Tree.Id	
		        and	dataSource.Name <> 'Internal'		
	        Left Join Grade DefaultGrade 
		        on DefaultGrade.Id = IsNULL(ISNULL(DefaultGradeAtPoint.IdDefaultGrade,DefaultGradeAtPort.IdDefaultGrade),s.IdGrade)
	        Left Join Grade ParentGrade 
		        on ParentGrade.Id = DefaultGrade.IdParent
	        Left Join Grade FallbackGrade 
		        on FallbackGrade.Id = (Select Top 1 Id From Grade where name = Tree.Name)
	        Left Join Ship Vessel 
		        on  Vessel.Id = s.IdShip
            Left Join ShipDetails VesselDetails
                on  VesselDetails.IdShip = Vessel.Id
                and VesselDetails.ValidUntil is null
            Left Join Counterpart equityHolder
		        on equityHolder.id = s.IdEquity
			Left Join Counterpart keeper
		        on keeper.id = s.IdKeeper
	        Left Join DealView deals
		        on deals.IdStem = s.Id
	        Left Join ConversionsView FallBackConversions 
		        on	FallBackConversions.IdUnitFrom = s.IdUnitOfMeasure 
		        and	FallBackConversions.IdGrade = FallbackGrade.Id	
	        Left Join ConversionsView StemConversions 
		        on	StemConversions.IdUnitFrom = s.IdUnitOfMeasure 
		        and	StemConversions.IdGrade = s.IdGrade		
		        and	StemConversions.IdUnitTo = FallBackConversions.IdUnitTo
	        Left Join view_DefaultVolumes DefaultVolumeAtPoint 
		        on	DefaultVolumeAtPoint.IdRegionTree = Tree.Id 
		        and DefaultVolumeAtPoint.IdLocation = s.IdLoadLocation 
		        and DefaultVolumeAtPoint.VesselClass = Vessel.ShipClass
		        and	dataSource.Name <> 'Internal'
	        Left Join view_DefaultVolumes DefaultVolumeAtPort 
		        on	DefaultVolumeAtPort.IdRegionTree = Tree.Id 
		        and DefaultVolumeAtPort.IdLocation = s.IdLoadLocation 
		        and DefaultVolumeAtPort.VesselClass = Vessel.ShipClass
		        and	dataSource.Name <> 'Internal'
	        Left Join ConversionsView DefaultConversions 
		        on	DefaultConversions.IdUnitFrom = ISNULL(DefaultVolumeAtPoint.IdUnit, DefaultVolumeAtPort.IdUnit)
		        and	DefaultConversions.IdGrade = DefaultGrade.Id
		        and	DefaultConversions.IdUnitTo = FallBackConversions.IdUnitTo
		        Outer Apply (
		        Select 
			        Case When  
					        Round((ISNULL(DefaultVolumeAtPoint.Quantity,DefaultVolumeAtPort.Quantity) * 
						        ISNULL(DefaultConversions.Factor,FallBackConversions.Factor)),4) /
						        Round(s.Quantity * ISNULL(StemConversions.Factor,FallBackConversions.Factor),4)
					        BETWEEN 0.67 
					        and		1.33 
				        Then Round((ISNULL(DefaultVolumeAtPoint.Quantity,DefaultVolumeAtPort.Quantity) * ISNULL(DefaultConversions.Factor,FallBackConversions.Factor)),4) 
				        Else Round(s.Quantity * ISNULL(StemConversions.Factor,FallBackConversions.Factor),4) 
			        End 'Quantity'
			        , ISNULL(DefaultConversions.IdUnitTo,FallBackConversions.IdUnitTo) 'IdUnit'
	        ) CleanQuantity
	        Left Join UnitOfMeasure CleanUnit 
		        on	CleanUnit.Id = CleanQuantity.IdUnit
			Left Join GradeLocationAssay SulphurLevel		
				on	SulphurLevel.IdRegionTree = Tree.Id
				and	SulphurLevel.IdGrade = DefaultGrade.Id
				and	SulphurLevel.IdAssay = (Select id from Assay Where Name = 'Sulphur')
				and	SulphurLevel.IdLocation = ISNULL(s.IdLoadLocation,s.IdLoadPort)
			Left Join GradeLocationAssay ApiLevel 
                on ApiLevel.IdRegionTree = Tree.Id
                and ApiLevel.IdGrade = DefaultGrade.Id
                and ApiLevel.IdAssay = (Select id from Assay Where Name = 'Api')
				and	ApiLevel.IdLocation = ISNULL(s.IdLoadLocation,s.IdLoadPort)
			Left Join GradeLocationAssay SulphurLevelGlobal		
				on	SulphurLevelGlobal.IdRegionTree = Tree.Id
				and	SulphurLevelGlobal.IdGrade = DefaultGrade.Id
				and	SulphurLevelGlobal.IdAssay = (Select id from Assay Where Name = 'Sulphur')
				and	SulphurLevelGlobal.IdLocation is null
			Left Join GradeLocationAssay ApiLevelGlobal
                on ApiLevelGlobal.IdRegionTree = Tree.Id
                and ApiLevelGlobal.IdGrade = DefaultGrade.Id
                and ApiLevelGlobal.IdAssay = (Select id from Assay Where Name = 'Api')
				and	ApiLevelGlobal.IdLocation is null
	Where	s.Deleted = 0 
	And		ISNULL(s.ChartererInfo, 0) = 0	    
	And		(@id is null or s.Id = @id)	
	And		(@tree is null or Tree.Name = @tree)	
	And		(@unit is null or CleanUnit.Name = @unit)	

    SET @ImportRows = @@ROWCOUNT;

    IF (@ImportRows = 0 And @stemId is null And	@tree is not null)
    BEGIN
    RAISERROR('Have you set default unit and default view in Settings?', 16, 1); -- no rows to import, and proc called from Targo Client
    END;            

    Delete 
	From	CargoAnalysis 
	Where	(@id is null or IdStem = @id)	 
	And		(@tree is null or RegionTree = @tree)	
	And		(@unit is null or CleanUnit = @unit)	
 
    INSERT INTO [dbo].[CargoAnalysis] (
		IdStem
		, IdRegionTree
		, RegionTree
		, Source
		, IdCleanGrade
		, CleanGrade
		, IdCleanGradeParent
		, CleanGradeParent
        , LoadPoint
        , LoadPort
		, LoadCountry 
		, LoadSubRegion 
		, LoadRegion 
        , DischargePoint
	    , DischargePort
		, DischargeRefinery
		, DischargeCountry 
		, DischargeSubRegion 
		, DischargeRegion 
		, CleanQuantity
		, IdCleanUnit
		, CleanUnit
        , Chain
	    , CurrentOwner
		, Keeper
	    , LastPrice
	    , LastBasis
	    , EquityHolder
	    , Imo
	    , VesselName
	    , ShipClass
		, Api
		, Sulphur
		, Midpoint
	)
    SELECT  IdStem
			, IdRegionTree
			, RegionTree
			, Source
			, IdCleanGrade
			, CleanGrade
			, IdCleanGradeParent
			, CleanGradeParent
			, LoadPoint
			, LoadPort
			, LoadCountry 
			, LoadSubRegion 
			, LoadRegion 
			, DischargePoint
			, DischargePort
			, DischargeRefinery
			, DischargeCountry 
			, DischargeSubRegion 
			, DischargeRegion 
			, CleanQuantity
			, IdCleanUnit
			, CleanUnit
			, Chain
			, CurrentOwner
			, Keeper
			, LastPrice
			, LastBasis
			, EquityHolder
			, Imo
			, VesselName
			, ShipClass
			, Api
			, Sulphur
			, Midpoint
    FROM	#analysisTemp tmp

END TRY
BEGIN CATCH
    IF (@@TRANCOUNT > 0) ROLLBACK;

    Declare @ermessage nvarchar(2048), @erseverity int, @erstate int, @erline int;
	Select @ermessage = CONCAT('Line Number: ', ERROR_LINE(), '. ', ERROR_MESSAGE()), @erseverity = ERROR_SEVERITY(), @erstate = ERROR_STATE(); 
	raiserror(@ermessage, @erseverity,@erstate)
END CATCH;