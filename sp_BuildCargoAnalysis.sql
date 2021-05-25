USE [STG_Targo]
GO

/****** Object:  StoredProcedure [dbo].[sp_BuildCargoAnalysis]    Script Date: 25/05/2021 11:42:16 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP PROCEDURE IF EXISTS [dbo].[sp_BuildCargoAnalysis]
GO

CREATE Procedure [dbo].[sp_BuildCargoAnalysis]	@stemId varchar(25) = null
												, @tree varchar(25) = null
												, @unit varchar(25) = null
												, @source varchar(55) = null
As
/*
- APPLY VIEW RULES:
	- Default Grade at port
	- Default volumes
	- Load/Discharge Regions
	- Api 
	- Sulphur 
	- Laycan midpoint
	- Arrival/Loading week/end

*/

Declare @id int = null;
IF @stemId Is Not NULL Set @id = Convert(int, @stemId);
SELECT TOP 0 * INTO #analysisTemp FROM CargoAnalysis

DECLARE @ImportRows INT;

BEGIN TRY

		if @stemId is null exec [dbo].[sp_BuildDealView]
	
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
		, ShippingRoute
		, Api
		, Sulphur
		, Midpoint
		, LoadingWeek
		, ArrivalWeek
		, EstimatedDischargeDate
	)		
	Select	s.id 'IdStem'
			, Tree.Id 
			, Tree.Name 
			, dataSource.Name 
			, DefaultGrade.Id 
			, DefaultGrade.Name
			, ParentGrade.Id 
			, ParentGrade.Name		
            , ISNULL(LoadRegions.TerminalName,FallbackLoadPoint.Name)
            , ISNULL(LoadRegions.PortName,FallbackLoadPort.Name) 
			, LoadRegions.CountryName
			, LoadRegions.SubRegionName
			, LoadRegions.RegionName
            , ISNULL(DischargeRegions.TerminalName,FallbackDischargePoint.Name)
            , ISNULL(DischargeRegions.PortName,FallbackDischargePort.Name) 
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
			, ShipRoute.Name 'ShippingRoute'
			, Coalesce(ApiLevel.NumValue,ApiLevelParent.NumValue,ApiLevelGlobal.NumValue) 
			, Coalesce(SulphurLevel.NumValue,SulphurLevelParent.NumValue,SulphurLevelGlobal.NumValue)
			, (Select DateAdd(day, DATEDIFF(day, s.StartLaycan, s.EndLaycan)/2, s.StartLaycan)) Midpoint
			, pLoading.Period
			, pArrival.Period
			, DATEADD(DAY, vt.AvgDays, s.LoadDate) EstimatedDischargeDate
	From	Stem s		
			Cross Join RegionTree Tree		
			LEFT JOIN Period pLoading
				ON  pLoading.Type = 'week'
				AND LoadDate BETWEEN pLoading.Period AND DATEADD(DAY,6,pLoading.Period)
			LEFT JOIN Period pArrival
				ON  pArrival.Type = 'week'
				AND DischargeDate BETWEEN pArrival.Period	AND DATEADD(DAY,6,pArrival.Period)
	        Left Join Source dataSource 
				On	dataSource.Id = s.source
	        Left Join RegionView LoadRegions 
				On LoadRegions.Id = ISNULL(s.IdLoadLocation,s.IdLoadPort) and LoadRegions.IdView = Tree.Id
			Left Join Location FallbackLoadPoint
				On FallbackLoadPoint.Id = s.IdLoadLocation
			Left Join Location FallbackLoadPort
				On FallbackLoadPort.Id = s.IdLoadPort
	        Left Join RegionView DischargeRegions 
				On DischargeRegions.Id = ISNULL(s.IdDestination,s.IdDisport) and DischargeRegions.IdView = Tree.Id
			Left Join Location FallbackDischargePoint
				On FallbackDischargePoint.Id = s.IdDestination
			Left Join Location FallbackDischargePort
				On FallbackDischargePort.Id = s.IdDisPort
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
			LEFT JOIN Grade TargoGrade 
				ON  TargoGrade.Id = s.idGrade
				AND TargoGrade.IdParent is not null
	        Left Join Grade DefaultGrade 
				ON DefaultGrade.Id = COALESCE(DefaultGradeAtPoint.IdDefaultGrade, DefaultGradeAtPort.IdDefaultGrade, s.IdGrade, TargoGrade.Id)
	        Left Join Grade ParentGrade 
		        on ParentGrade.Id = DefaultGrade.IdParent
	        Left Join Grade FallbackGrade 
		        on FallbackGrade.Id = (Select Top 1 Id From Grade where name = Tree.Name)
	        Left Join Ship Vessel 
		        on  Vessel.Id = s.IdShip
            Left Join ShipDetails VesselDetails
                on  VesselDetails.IdShip = Vessel.Id
                and VesselDetails.ValidUntil is null
			Left Join ShippingRoute ShipRoute
				On	ShipRoute.Id = s.IdShippingRoute
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
				and DefaultVolumeAtPoint.IdSource = s.Source
		        and	dataSource.Name <> 'Internal'
	        Left Join view_DefaultVolumes DefaultVolumeAtPort 
		        on	DefaultVolumeAtPort.IdRegionTree = Tree.Id 
		        and DefaultVolumeAtPort.IdLocation = s.IdLoadPort 
		        and DefaultVolumeAtPort.VesselClass = Vessel.ShipClass
				and DefaultVolumeAtPort.IdSource = s.Source
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
						        ISNULL(NULLIF(Round(s.Quantity * ISNULL(StemConversions.Factor,FallBackConversions.Factor),4),0),1)
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
				and	SulphurLevel.IdLocation is null
			Left Join GradeLocationAssay ApiLevel 
                on ApiLevel.IdRegionTree = Tree.Id
                and ApiLevel.IdGrade = DefaultGrade.Id
                and ApiLevel.IdAssay = (Select id from Assay Where Name = 'Api')
				and	ApiLevel.IdLocation is null
			Left Join GradeLocationAssay SulphurLevelParent	
				on	SulphurLevelParent.IdRegionTree = Tree.Id
				and	SulphurLevelParent.IdGrade = ParentGrade.Id
				and	SulphurLevelParent.IdAssay = (Select id from Assay Where Name = 'Sulphur')
				and	SulphurLevelParent.IdLocation  is null
			Left Join GradeLocationAssay ApiLevelParent
                on ApiLevelParent.IdRegionTree = Tree.Id
                and ApiLevelParent.IdGrade = ParentGrade.Id
                and ApiLevelParent.IdAssay = (Select id from Assay Where Name = 'Api')
				and	ApiLevelParent.IdLocation is null
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
			LEFT Join view_VoyageDays_Location_Month_ShippingRoute vt
				ON  vt.IdLoadLocation = ISNULL(LoadRegions.IdSubRegion, LoadRegions.IdRegion)
				AND	vt.IdDischargeLocation = ISNULL(DischargeRegions.IdSubRegion, DischargeRegions.IdRegion)
	Where	s.Deleted = 0 
	And		ISNULL(s.ChartererInfo, 0) = 0	    
	And		(@id is null or s.Id = @id)	
	And		(@tree is null or Tree.Name = @tree)	
	And		(@unit is null or CleanUnit.Name = @unit)	
	And		(@source is null or DataSource.Name = @source)	
	
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
	And		(@source is null or Source = @source)	
 
    INSERT INTO [dbo].[CargoAnalysis] 
    SELECT *
    FROM	#analysisTemp tmp

END TRY
BEGIN CATCH
    IF (@@TRANCOUNT > 0) ROLLBACK;

    Declare @ermessage nvarchar(2048), @erseverity int, @erstate int, @erline int;
	Select @ermessage = CONCAT('Line Number: ', ERROR_LINE(), '. ', ERROR_MESSAGE()), @erseverity = ERROR_SEVERITY(), @erstate = ERROR_STATE(); 
	raiserror(@ermessage, @erseverity,@erstate)
END CATCH;

IF OBJECT_ID('tempdb.dbo.#analysisTemp', 'U') IS NOT NULL DROP TABLE #analysisTemp;
GO


