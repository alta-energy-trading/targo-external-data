USE [STG_Targo]
GO
IF OBJECT_ID('[dbo].[sp_Clean_PetroLogistics]','p') IS NOT NULL
DROP PROC [dbo].[sp_Clean_PetroLogistics]
GO
CREATE PROC [dbo].[sp_Clean_PetroLogistics] @debug INT = 0
AS
SET NOCOUNT ON
DECLARE @rows INT

BEGIN TRY
	BEGIN -- Get and lookup data
				SELECT 
			targoLoadTerminal.Name [LoadPoint]
			, targoLoadTerminal.Id [IdLoadPoint]
			, petLog.load_terminal [PetLog_load_terminal]
			, ISNULL(targoLoadPort.Name,targoLoadCountry.Name) [LoadPort]
			, ISNULL(targoLoadPort.Id,targoLoadCountry.Id) [IdLoadPort]
			, petLog.load_port [PetLog_load_port]
			, ISNULL(targoDischargePort.Name, targoDischargeCountry.Name) [DischargePort]
			, ISNULL(targoDischargePort.Id, targoDischargeCountry.Id) [IdDischargePort]
			, petLog.discharge_port [PetLog_discharge_port]
			, targoLoadCountry.Name [LoadCountry]
			, targoLoadCountry.Id [IdLoadCountry]
			, petLog.load_port [PetLog_load_country]
			, targoDischargeCountry.Name [DischargeCountry]
			, targoDischargeCountry.Id [IdDischargeCountry]
			, petLog.discharge_country [PetLog_discharge_country]
			, targoGrade.Name [Grade]
			, targoGrade.Id [IdGrade]
			, petLog.cargo_grade [PetLog_cargo_grade]
			, targoCounterpart.Name [Counterpart]
			, targoCounterpart.Id [IdCounterpart]
			, petLog.customer [PetLog_customer]
			, targoEquity.Name [Equity]
			, targoEquity.Id [IdEquity]
			, petLog.supplier [PetLog_supplier]
			, shp.Id [IdShip]
			, petLog.tanker_imo [PetLog_tanker_imo]
			, petLog.tanker_name [PetLog_tanker_name]
			, cargo_id
			, load_port_date
			, qty_barrels
			, NULLIF(discharge_port_date, '0000-00-00') discharge_port_date
			, DATEPART(year, load_port_date) Year
			, DATEPART(month, load_port_date) Month
			, uom.Id [IdUnitOfMeasure]
			, src.Id [IdSource]
			, MovementHash
		INTO 
			#temp_PetLog_Cleaned
		FROM 
			Movement petLog
			OUTER APPLY (
				SELECT TOP 1
					Id
					, Imo
				FROM
					Ship
				WHERE
					Imo = CONCAT('IMO', petLog.tanker_imo)
				ORDER BY Id
				) shp
			LEFT JOIN MapType locationMapType
				ON	locationMapType.Name = 'Location'
			LEFT JOIN MapType gradeMapType
				ON	gradeMapType.Name = 'Grade'
			LEFT JOIN MapType counterpartMapType
				ON	counterpartMapType.Name = 'Counterpart'
			LEFT JOIN Mapping loadTerminalMap
				ON  loadTerminalMap.ExternalValue = petLog.load_terminal
				AND loadTerminalMap.IdMapType = locationMapType.Id
			LEFT JOIN Mapping loadPortMap
				ON  loadPortMap.ExternalValue = petLog.load_port
				AND loadPortMap.IdMapType = locationMapType.Id
			LEFT JOIN Mapping dischargePortMap
				ON  dischargePortMap.ExternalValue = petLog.discharge_port
				AND dischargePortMap.IdMapType = locationMapType.Id
			LEFT JOIN Mapping loadCountryMap
				ON  loadCountryMap.ExternalValue = petLog.load_country
				AND loadCountryMap.IdMapType = locationMapType.Id
			LEFT JOIN Mapping dischargeCountryMap
				ON  dischargeCountryMap.ExternalValue = petLog.discharge_country
				AND dischargeCountryMap.IdMapType = locationMapType.Id
			LEFT JOIN Mapping gradeMap
				ON  gradeMap.ExternalValue = petLog.cargo_grade
				AND gradeMap.IdMapType = gradeMapType.Id
			LEFT JOIN Mapping customerMap
				ON  customerMap.ExternalValue = petLog.customer
				AND customerMap.IdMapType = counterpartMapType.Id
			LEFT JOIN Mapping equityMap
				ON  equityMap.ExternalValue = petLog.supplier
				AND equityMap.IdMapType = counterpartMapType.Id
			LEFT JOIN (
				SELECT
					l.Id
					, l.Name
				FROM
					Location l 
					INNER JOIN Port p
						ON  p.Id = l.Id
						AND p.Kind = 2
			) targoLoadTerminal
				ON	targoLoadTerminal.Name = COALESCE(loadTerminalMap.TargoValue,petLog.load_terminal)
			LEFT JOIN (
				SELECT 
					l.Id
					, l.Name
				FROM
					Location l 
					INNER JOIN Port p
						ON  p.Id = l.Id
						AND p.Kind = 1
			) targoLoadPort
				ON	targoLoadPort.Name = COALESCE(loadPortMap.TargoValue,petLog.load_port)
			LEFT JOIN (
				SELECT
					l.Id
					, l.Name
				FROM
					Location l 
					INNER JOIN Port p
						ON  p.Id = l.Id
						AND p.Kind = 1
			) targoDischargePort
				ON	targoDischargePort.Name = COALESCE(dischargePortMap.TargoValue,petLog.discharge_port)
			LEFT JOIN (
				SELECT
					l.Id
					, l.Name
				FROM
					Location l 
					INNER JOIN Area a
						ON  a.Id = l.Id
						AND a.Kind = 3
			) targoLoadCountry
				ON	targoLoadCountry.Name = COALESCE(loadCountryMap.TargoValue,petLog.load_country)
			LEFT JOIN (
				SELECT
					l.Id
					, l.Name
				FROM
					Location l 
					INNER JOIN Area a
						ON  a.Id = l.Id
						AND a.Kind = 3
			) targoDischargeCountry
				ON	targoDischargeCountry.Name = COALESCE(dischargeCountryMap.TargoValue,petLog.discharge_country)
			LEFT JOIN Grade targoGrade
				ON	targoGrade.Name = COALESCE(gradeMap.TargoValue, petLog.cargo_grade)
			LEFT JOIN Counterpart targoCounterpart
				ON	targoCounterpart.Name = COALESCE(customerMap.TargoValue, petLog.customer)
			LEFT JOIN Counterpart targoEquity
				ON	targoEquity.Name = COALESCE(equityMap.TargoValue, petLog.supplier)
			LEFT JOIN UnitOfMeasure uom
				ON	uom.Name = 'bbl'
			LEFT JOIN SourceType st
				ON	st.Name = 'Stem'
			LEFT JOIN Source src
				ON	src.IdType = st.Id
				AND	src.Name = 'PetroLogistics'		
				AND	src.Name = 'PetroLogistics'		
	
		SET @rows = @@ROWCOUNT
		PRINT CAST(@rows as VARCHAR(10)) + ' rows in PetroLogistics Movement'
	END

	IF @debug = 1 -- Show Results if debugging
	SELECT * FROM #temp_PetLog_Cleaned

	IF @debug = 0 -- Don't run this if debugging
	BEGIN -- Get Validations
		SELECT 
			[Name]
			, [Type]
		INTO #temp_validations
		FROM (
			-- Get Locations not found in Targo
				SELECT DISTINCT 
					PetLog_load_terminal [Name]
					, 'Terminal' [Type]
				FROM 
					#temp_PetLog_Cleaned
				WHERE IdLoadPoint IS NULL AND PetLog_load_terminal IS NOT NULL
			UNION
				SELECT DISTINCT 
					PetLog_load_port [Name]
					, 'Port' [Type]
				FROM 
					#temp_PetLog_Cleaned
				WHERE IdLoadPort IS NULL AND PetLog_load_port IS NOT NULL
				AND PetLog_load_port <> 'Not known'
			UNION
				SELECT DISTINCT 
					PetLog_discharge_port [Name]
					, 'Port' [Type]
				FROM 
					#temp_PetLog_Cleaned
				WHERE IdDischargePort IS NULL AND PetLog_discharge_port IS NOT NULL
				AND PetLog_discharge_port <> 'Not known'
			UNION
				SELECT DISTINCT 
					PetLog_discharge_country [Name]
					, 'Country' [Type]
				FROM 
					#temp_PetLog_Cleaned
				WHERE IdDischargeCountry IS NULL AND PetLog_discharge_country IS NOT NULL
				AND PetLog_discharge_country <> 'Not known'
			UNION
				SELECT DISTINCT 
					PetLog_load_country [Name]
					, 'Country' [Type]
				FROM 
					#temp_PetLog_Cleaned
				WHERE IdLoadCountry IS NULL AND NULLIF(PetLog_load_country,'Not Known') IS NOT NULL
				AND PetLog_load_country <> 'Not known'
			-- Get Grades not found in Targo
			UNION
				SELECT DISTINCT
					PetLog_cargo_grade [Name]
					, 'Grade' [Type]
				FROM
					#temp_PetLog_Cleaned
				WHERE 
					IdGrade IS NULL AND PetLog_cargo_grade IS NOT NULL
					AND PetLog_cargo_grade <> 'Not known'
			-- Get Counterparts not found in Targo
			UNION
				SELECT DISTINCT
					PetLog_customer [Name]
					, 'Counterpart' [Type]
				FROM
					#temp_PetLog_Cleaned
				WHERE 
					IdCounterpart IS NULL AND PetLog_customer IS NOT NULL
					AND PetLog_customer <> 'Not known'
			UNION
				SELECT DISTINCT
					PetLog_supplier
					, 'Counterpart'
				FROM
					#temp_PetLog_Cleaned
				WHERE 
					IdEquity IS NULL AND PetLog_supplier IS NOT NULL
					AND PetLog_supplier <> 'Not known'
				-- Get Vessels not found in Targo
			UNION
				SELECT DISTINCT
					PetLog_tanker_imo
					, 'Vessel'
				FROM
					#temp_PetLog_Cleaned
				WHERE 
					IdShip IS NULL AND PetLog_tanker_imo IS NOT NULL
	
		) missingNames
		
		SET @rows = @@ROWCOUNT
		PRINT CAST(@rows as VARCHAR(10)) + ' items added to validation'
	END

	IF @debug = 0 -- Don't run this if debugging
	BEGIN -- Build Staging Table
		SELECT TOP 0
			CargoId
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
			, Available
			, IdDestination
			, IdDisport
			, Dropped
			, Deleted
			, IdShip
			, DischargeDate
			, Clocked
			, Short
			, DestinationConfirmed
			, Arbitrage
			, Finalized
			, shiptoship
			, Source
			, ExtRef
			, CreationName
			, CreationDate
			, FloatingStorage
			, Pipeline
			, ToFloatingStorage
			, ToLandStorage
			, FromLandStorage
			, loadSTS
			, PetLogHash
		INTO 
			#temp_StemStaging
		FROM
			Stem
					
		Declare @nowDate DateTime = GetDate();

		INSERT #temp_StemStaging
		SELECT
		CONCAT(SUBSTRING(Grade ,1,3),'_',CAST([Year] AS VARCHAR(4)), SUBSTRING(CAST([Month]  + 100 AS varchar(3)), 2, 2),SUBSTRING(CAST(DATEPART(Day,load_port_date)  + 100 AS varchar(3)), 2, 2),'_PetLog') cargoId
			, IdGrade
			, IdLoadPort
			, Year
			, Month
			, load_port_date
			, load_port_date
			, load_port_date
			, IdLoadPoint
			, qty_barrels
			, IdUnitOfMeasure
			, IdEquity
			, 1 Available
			, IdDischargePort
			, IdDischargePort
			, 0 Dropped
			, 0 Deleted
			, IdShip
			, discharge_port_date
			, 0 Clocked
			, 0 Short
			, 0 DestinationConfirmed
			, 0 Arbitrage
			, 0 Finalized
			, 0 shiptoship
			, IdSource
			, cargo_id ExtRef
			, 'Clean Job' CreationName
			, @nowDate CreationDate
			, 0 FloatingStorage
			, 0 Pipeline
			, 0 ToFloatingStorage
			, 0 ToLandStorage
			, 0 FromLandStorage
			, 0 loadSTS
			, MovementHash PetLogHash
		FROM 
			#temp_PetLog_Cleaned
		WHERE
			IdGrade IS NOT NULL

		SET @rows = @@ROWCOUNT
		PRINT CAST(@rows as VARCHAR(10)) + ' rows added to stem staging table'
	END

	IF @debug = 0 -- Don't run this if debugging
	BEGIN -- Merge with Stem
		MERGE
			dbo.Stem t
		USING
			#temp_StemStaging s
		ON
			t.petLogHash = s.petLogHash
		WHEN MATCHED 
		AND	HASHBYTES(
				'SHA1'
				, UPPER(LTRIM(RTRIM(ISNULL(t.IdGrade,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(t.IdLoadPort,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(t.LoadDate,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(t.IdLoadLocation,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(t.Quantity,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(t.IdEquity,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(t.IdDestination,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(t.IdDisport,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(t.IdShip,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(t.DischargeDate,''))))
			) <> 
			HASHBYTES(
				'SHA1'
				, UPPER(LTRIM(RTRIM(ISNULL(s.IdGrade,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(s.IdLoadPort,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(s.LoadDate,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(s.IdLoadLocation,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(s.Quantity,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(s.IdEquity,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(s.IdDestination,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(s.IdDisport,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(s.IdShip,'')))) + '|' + 
					UPPER(LTRIM(RTRIM(ISNULL(s.DischargeDate,''))))
			) 
		THEN UPDATE SET 
			t.IdGrade = s.IdGrade  
			, t.IdLoadPort = s.IdLoadPort
			, t.LoadDate = s.LoadDate
			, t.IdLoadLocation = s.IdLoadLocation
			, t.Quantity = s.Quantity
			, t.IdEquity = s.IdEquity
			, t.IdDestination = s.IdDestination
			, t.IdDisport = s.IdDisport
			, t.IdShip = s.IdShip
			, t.DischargeDate = s.DischargeDate
		WHEN NOT MATCHED BY TARGET
		THEN INSERT (
			CargoId
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
			, Available
			, IdDestination
			, IdDisport
			, Dropped
			, Deleted
			, IdShip
			, DischargeDate
			, Clocked
			, Short
			, DestinationConfirmed
			, Arbitrage
			, Finalized
			, shiptoship
			, Source
			, ExtRef
			, CreationName
			, CreationDate
			, FloatingStorage
			, Pipeline
			, ToFloatingStorage
			, ToLandStorage
			, FromLandStorage
			, loadSTS
			, petLogHash
		)
		VALUES (
			CargoId
			, s. IdGrade
			, s. IdLoadPort
			, s. Year
			, s. Month
			, s. StartLaycan
			, s. EndLaycan
			, s. LoadDate
			, s. IdLoadLocation
			, s. Quantity
			, s. IdUnitOfMeasure
			, s. IdEquity
			, s. Available
			, s. IdDestination
			, s. IdDisport
			, s. Dropped
			, s. Deleted
			, s. IdShip
			, s. DischargeDate
			, s. Clocked
			, s. Short
			, s. DestinationConfirmed
			, s. Arbitrage
			, s. Finalized
			, s. shiptoship
			, s. Source
			, s. ExtRef
			, s. CreationName
			, s. CreationDate
			, s. FloatingStorage
			, s. Pipeline
			, s. ToFloatingStorage
			, s. ToLandStorage
			, s. FromLandStorage
			, s. loadSTS
			, s. petLogHash
		)
		WHEN NOT MATCHED BY SOURCE AND t.Source = (Select src.Id FROM SourceType st LEFT JOIN Source src ON src.IdType = st.Id WHERE st.Name = 'Stem' AND src.Name = 'PetroLogistics')
		THEN DELETE;
		
		SET @rows = @@ROWCOUNT
		PRINT CAST(@rows as VARCHAR(10)) + ' rows affected by merge'
	END

	IF @debug = 0 -- Don't run this if debugging
	BEGIN -- Update Deals so Current Owner column is populated in CargoAnalysis
		DELETE
		FROM	
			Deal
		WHERE 
			IdStem IN (
				SELECT 
					s.Id 
				FROM
					Stem s
					LEFT JOIN SourceType st
						ON	st.Name = 'Stem'
					INNER JOIN Source src 
						ON	src.Name = 'PetroLogistics'
						AND	src.IdType = st.Id
						AND src.Id = s.Source

			)
		INSERT INTO 
			Deal (
				IdStem
				, Position
				, IdBuyer
			)
		SELECT
			s.Id
			, 0
			, m.IdCounterpart
		FROM
			Stem s
			LEFT JOIN SourceType st
				ON	st.Name = 'Stem'
			INNER JOIN Source src 
				ON	src.Name = 'PetroLogistics'
				AND	src.IdType = st.Id
				AND src.Id = s.Source
			LEFT JOIN #temp_PetLog_Cleaned m
				ON	m.MovementHash = s.PetLogHash
		WHERE
			m.IdCounterpart IS NOT NULL

	END

	IF @debug = 0 -- Don't run this if debugging
	BEGIN -- Update Cargo Analysis
		exec sp_BuildCargoAnalysis @source = 'PetroLogistics'
	END

	IF @debug = 0 -- Don't run this if debugging
	BEGIN -- Send Validations
		DECLARE @xml NVARCHAR(MAX)
		DECLARE @body NVARCHAR(MAX) = '<html><body><p>Dear Targo Admin,<p>'
		DECLARE @recipients NVARCHAR(100) = CASE @@SERVERNAME WHEN 'ARCSQL' THEN 'targo.support@arcpet.co.uk' ELSE 'targo.support.test@arcpet.co.uk' END

		BEGIN -- Send Validation email if there are any validation errors
			IF (SELECT COUNT(1) FROM #temp_validations) > 0
			BEGIN
				SET @xml = CAST((SELECT [Name] as 'td','',[Type] as 'td',''
				FROM #temp_validations
				ORDER By [Type],[Name]
				FOR XML PATH('tr'), ELEMENTS ) AS NVARCHAR(MAX))

				SET @body = @body + '<p>These names are missing from Targo. The external names need to be mapped to Targo names, or added to Targo</p>
				<table border = 1> 
				<tr>
				<th> Name </th> <th> Type </th></tr>' + @xml + '</table>' +
				'<p>Best Regards,</p><p>Targo Support</p></body></html>'

				EXEC msdb.dbo.sp_send_dbmail
				@profile_name = 'Targo'
				, @body = @body
				, @body_format = 'HTML'
				, @recipients = @recipients
				, @subject = 'Petro Logistics Validation'
			END
		END
	END

	IF @debug = 0 -- Don't run this if debugging
	BEGIN -- Send summary email	
		SET @body = '<html><body><p>Dear Targo Admin,<p>'
		SET @body = @body + '<p>PetroLogistics cleaning job has completed</p>
		<p>Best Regards,</p><p>Targo Support</p></body></html>'

		EXEC msdb.dbo.sp_send_dbmail
		@profile_name = 'Targo'
		, @body = @body
		, @body_format = 'HTML'
		, @recipients = @recipients
		, @subject = 'Clean PetroLogistics'		
	END

END TRY
BEGIN CATCH
    IF (@@TRANCOUNT > 0) ROLLBACK;

    Declare @ermessage nvarchar(2048), @erseverity int, @erstate int, @erline int;
	Select @ermessage = CONCAT('Line Number: ', ERROR_LINE(), '. ', ERROR_MESSAGE()), @erseverity = ERROR_SEVERITY(), @erstate = ERROR_STATE(); 
	
	DECLARE @errorbody NVARCHAR(MAX) = '<html><body><p>Dear Analytics Admin,<p>'
	DECLARE @errorrecipients NVARCHAR(100) = CASE @@SERVERNAME WHEN 'ARCSQL' THEN 'targo.support@arcpet.co.uk' ELSE 'targo.support.test@arcpet.co.uk' END

	BEGIN -- Send error email	
		SET @errorbody = '<html><body><p>Dear Analytics Admin,<p>'
		SET @errorbody = @errorbody + '<p>Cleaning PetroLogistics failed with the following error:</p><p>' + @ermessage + '</p>
		<p>Best Regards,</p><p>Analytics Support</p></body></html>'

		EXEC msdb.dbo.sp_send_dbmail
		@profile_name = 'Targo'
		, @body = @errorbody
		, @body_format = 'HTML'
		, @recipients = @errorrecipients
		, @subject = 'Error - Targo - PetroLogistics'		
	END

	raiserror(@ermessage, @erseverity,@erstate)
END CATCH
GO


