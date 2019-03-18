Create Procedure [dbo].[sp_LoadClipperCleanerDataToStem]	

As
Begin Try
	IF OBJECT_ID('FK_CellFormatInfo_Stem', 'F') IS NOT NULL
	BEGIN
		ALTER TABLE CellFormatInfo
		DROP CONSTRAINT FK_CellFormatInfo_Stem; 
	END;

		IF OBJECT_ID('FK_Deal_Stem', 'F') IS NOT NULL
	BEGIN
		ALTER TABLE Deal
		DROP CONSTRAINT FK_Deal_Stem; 
	END;

	IF OBJECT_ID('FK_STEM_PARENTSTEM', 'F') IS NOT NULL
	BEGIN
		ALTER TABLE Stem
		DROP CONSTRAINT FK_STEM_PARENTSTEM; 
	END;

	Merge	Stem t
	Using	ClipperCleanerData s
			On	s.Rownum = t.ExtRef
			And	s.Source = t.Source
	When Matched
	And		s.ClipperDataRowversion <> t.ClipperDataRowversion
	Then
	Update	
	Set		t.IdGrade=s.IdGrade
			, t.IdLoadPort=s.IdLoadPort
			, t.Year=s.Year
			, t.Month=s.Month
			, t.StartLaycan=s.StartLaycan
			, t.EndLaycan=s.EndLaycan
			, t.LoadDate=s.LoadDate
			, t.IdLoadLocation=s.IdLoadLocation
			, t.Quantity=s.Quantity
			, t.IdUnitOfMeasure=s.IdUnitOfMeasure
			, t.IdEquity=s.IdEquity
			, t.IdDestination=s.IdDestination
			, t.IdDisport=s.IdDisport
			, t.IdShip=s.IdShip
			, t.DischargeDate=s.DischargeDate
			, t.IdCharterer=s.IdCharterer
			, t.IdShippingRoute=s.IdShippingRoute
			, t.RevisionName='Clean Clipper Data'
			, t.RevisionDate=GETDATE()
			, t.ClipperDataRowVersion=s.ClipperDataRowVersion
	When Not Matched By Target
	Then
	Insert	(
		CargoId
		, IdParent
		, IdGrade
		, IdLoadPort
		, Year
		, Month
		, StartLaycan
		, EndLaycan
		, LoadDate
		, IdLoadLocation
		, Quantity
		, IdUnitOfMeasure
		, IdEquity
		, IdDestination
		, IdDisport
		, IdShip
		, DischargeDate
		, IdCharterer
		, IdShippingRoute	
		, Source
		, CreationName
		, CreationDate
		, RevisionName
		, RevisionDate
		, ExtRef
		, ClipperDataRowVersion
		, ChartererInfo
	)
	Values (
		s.CargoId
		, s.IdParent
		, s.IdGrade
		, s.IdLoadPort
		, s.Year
		, s.Month
		, s.StartLaycan
		, s.EndLaycan
		, s.LoadDate
		, s.IdLoadLocation
		, s.Quantity
		, s.IdUnitOfMeasure
		, s.IdEquity
		, s.IdDestination
		, s.IdDisport
		, s.IdShip
		, s.DischargeDate
		, s.IdCharterer
		, s.IdShippingRoute
		, s.Source
		, s.CreationName
		, s.CreationDate
		, s.RevisionName
		, s.RevisionDate
		, s.Rownum
		, s.ClipperDataRowVersion
		, s.ChartererInfo
	)
	When Not Matched By Source
	And		t.Source = (
				Select	s.Id
				From	Source s
						Inner Join SourceType st
							On	st.Id = s.IdType
							And	st.Name like 'stem%'
							And	s.Name = 'Clipper'
			)
	Then	Delete;

	ALTER TABLE Stem
	ADD CONSTRAINT FK_STEM_PARENTSTEM
	FOREIGN KEY (IdParent) REFERENCES Stem(Id);

	ALTER TABLE Deal
	ADD CONSTRAINT FK_Deal_Stem
	FOREIGN KEY (IdStem) REFERENCES Stem(Id);

	ALTER TABLE CellFormatInfo
	ADD CONSTRAINT FK_CellFormatInfo_Stem
	FOREIGN KEY (IdStem) REFERENCES Stem(Id);
		
	-- Update any adopted stems
	Update	t
	Set		t.CargoId = s.CargoId
			, t.IdGrade = s.IdGrade
			, t.IdLoadPort = s.IdLoadPort
			, t.Year = s.Year
			, t.Month = s.Month
			, t.StartLaycan = s.StartLaycan
			, t.EndLaycan = s.EndLaycan
			, t.LoadDate = s.LoadDate
			, t.IdLoadLocation = s.IdLoadLocation
			, t.Quantity = s.Quantity
			, t.IdUnitOfMeasure = s.IdUnitOfMeasure
			, t.IdEquity = s.IdEquity
			, t.IdDestination = s.IdDestination
			, t.IdDisport = s.IdDisport
			, t.IdShip = s.IdShip
			, t.DischargeDate = s.DischargeDate
			, t.IdCharterer = s.IdCharterer
			, t.IdShippingRoute = s.IdShippingRoute
			, t.CreationName = s.CreationName 
			, t.CreationDate = s.CreationDate 
			, t.RevisionName = s.RevisionName
			, t.RevisionDate = s.RevisionDate
			, t.ClipperDataRowVersion = s.ClipperDataRowVersion
			, t.ChartererInfo = s.ChartererInfo 
	From	Stem t 
			Left Join SourceType st
				On	st.Name = 'Stem'
			Left Join Source src 
				On	src.IdType = st.Id
				And	src.Name = 'Internal'
			Inner Join Stem s 
				On	s.ExtRef = t.Extref 
				And t.Source <> s.Source 
				And t.Source = src.Id
End Try
Begin Catch
	If @@TRANCOUNT > 0
		rollback tran
	
	declare @ermessage nvarchar(2048), @erseverity int, @erstate int;
	Select @ermessage = ERROR_MESSAGE(), @erseverity = ERROR_SEVERITY(), @erstate = ERROR_STATE();
	raiserror(@ermessage, @erseverity,@erstate)
End Catch

delete from ClipperData_Deletes