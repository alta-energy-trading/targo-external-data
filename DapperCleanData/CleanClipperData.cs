using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using DapperCleanData.Connection;
using DapperCleanData.Models;
using FastMember;

namespace DapperCleanData
{
    public class CleanClipperData
    {
        private int _sourceClipper;
        private int _gradeUnallocated;
        private int _portUnallocated;

        public ConcurrentBag<Grade> GradesList { get; private set; }
        public ConcurrentBag<Ship> ShipsList { get; private set; }
        public ConcurrentBag<Area> AreasList { get; private set; }
        public ConcurrentBag<Port> PortsList { get; private set; }
        public ConcurrentBag<UnitOfMeasure> UnitOfMeasuresList { get; private set; }
        public ConcurrentBag<Counterpart> CounterpartsList { get; private set; }
        public ConcurrentBag<Charterer> CharterersList { get; private set; }
        public ConcurrentBag<ShippingRoute> ShippingRoutesList { get; private set; }
        public ConcurrentBag<Source> SourcesList { get; private set; }
        public ConcurrentQueue<VoyageTime> VoyageTimesQueue { get; private set; }
        public ConcurrentBag<Mapping> MappingsList { get; private set; }

        List<ClipperCleanerData> ClipperCleanerDataList = new List<ClipperCleanerData>();

        private int _processedCount;
        private string _connectionString;

        public CleanClipperData(string connectionString)
        {
            ConnectionFactory.SetConnectionString(connectionString);

            VoyageTimesQueue = new ConcurrentQueue<VoyageTime>();

            GetStaticData();

            _connectionString = connectionString;

            SetGlobalVariables();
        }

        public async Task GetCleanData()
        {
            var newRows = GetRawDataRowsToInsert();

            Debug.WriteLine($"{newRows.Count} rows  {DateTime.Now}");

            List<Task> tasks = new List<Task>();
            foreach (var batch in newRows.Batch(1000))
            {
                try
                {
                    await CleanBatch(batch);
                }
                catch (Exception e)
                {

                }
            // tasks.Add(Task.Run(() => CleanBatch(batch)));
        }
           // var task = Task.WhenAll(tasks);
            //try
            //{
            //    await task;
            //}
            //catch
            //{
            //    throw new Exception(string.Join(", ",
            //        task?.Exception?.Flatten().InnerExceptions?.Select(e => e.Message)));
            //}

            

            InsertClipperCleanerData();

            UpdateVoyageTimes();
        }

        private void InsertClipperCleanerData()
        {
            try
            {
                Debug.WriteLine($"Inserting CleanClipperData... {DateTime.Now}");
                using (var bcp = new SqlBulkCopy(_connectionString))
                {
                    bcp.BatchSize = 50000;
                    bcp.BulkCopyTimeout = 0;
                    using (var reader = ObjectReader.Create(ClipperCleanerDataList))
                    {
                        bcp.DestinationTableName = "ClipperCleanerData";
                        bcp.ColumnMappings.Add("DateNum", "DateNum");
                        bcp.ColumnMappings.Add("RowNum", "RowNum");
                        bcp.ColumnMappings.Add("StatNum", "StatNum");
                        bcp.ColumnMappings.Add("CargoId", "CargoId");
                        bcp.ColumnMappings.Add("ChartererInfo", "ChartererInfo");
                        bcp.ColumnMappings.Add("ClipperDataRowVersion", "ClipperDataRowVersion");
                        bcp.ColumnMappings.Add("CreationDate", "CreationDate");
                        bcp.ColumnMappings.Add("CreationName", "CreationName");
                        bcp.ColumnMappings.Add("DischargeDate", "DischargeDate");
                        bcp.ColumnMappings.Add("EndLaycan", "EndLaycan");
                        bcp.ColumnMappings.Add("IdCharterer", "IdCharterer");
                        bcp.ColumnMappings.Add("IdClipperCharterer", "IdClipperCharterer");
                        bcp.ColumnMappings.Add("IdDestination", "IdDestination");
                        bcp.ColumnMappings.Add("IdDisport", "IdDisport");
                        bcp.ColumnMappings.Add("IdEquity", "IdEquity");
                        bcp.ColumnMappings.Add("IdGrade", "IdGrade");
                        bcp.ColumnMappings.Add("IdLoadLocation", "IdLoadLocation");
                        bcp.ColumnMappings.Add("IdLoadPort", "IdLoadPort");
                        bcp.ColumnMappings.Add("IdShippingRoute", "IdShippingRoute");
                        bcp.ColumnMappings.Add("IdShip", "IdShip");
                        bcp.ColumnMappings.Add("IdUnitOfMeasure", "IdUnitOfMeasure");
                        bcp.ColumnMappings.Add("LoadDate", "LoadDate");
                        bcp.ColumnMappings.Add("Month", "Month");
                        bcp.ColumnMappings.Add("Quantity", "Quantity");
                        bcp.ColumnMappings.Add("ShipToShip", "ShipToShip");
                        bcp.ColumnMappings.Add("Source", "Source");
                        bcp.ColumnMappings.Add("StartLaycan", "StartLaycan");
                        bcp.ColumnMappings.Add("Year", "Year");
                        bcp.WriteToServer(reader);
                        Debug.WriteLine($"Inserted {ClipperCleanerDataList.Count} rows. {DateTime.Now}");
                    }
                }
            }
            catch (Exception e)
            {
                string error =
                    $"INSERT INTO CleanerData_Validation (SourceName, ExtRef,IsSuccess,Value,Message) VALUES (@source,@extRef,@success,@value,@message)";

                using (var connection = ConnectionFactory.GetOpenConnection())
                {
                    connection.Execute(error,
                        new
                        {
                            source = "Clipper",
                            extRef = 0,
                            success = 0,
                            value = e.InnerException?.InnerException?.Message ?? e.InnerException?.Message ?? e.Message,
                            message = "Error: Insert"
                        });
                }
            }
        }

        private void UpdateVoyageTimes()
        {
            string sql =
                @"  Merge	    VoyageTime t
                    Using	(
			                    Select	Month
					                    , Coalesce(IdLoadPort, IdLoadLocation) IdLoadPort
					                    , Coalesce(IdDisPort, IdDestination) IdDischargePort
					                    , IdShippingRoute
					                    , s.ShipClass VesselClass
					                    , min(DateDiff(day, LoadDate, DischargeDate)) DaysMinimum
					                    from ClipperCleanerData ccd

					                    Left Join Ship s
					                    On S.Id = ccd.IdShip
			                    Where	Coalesce(IdLoadPort, IdLoadLocation) is not null
			                    And		Coalesce(IdDisPort, IdDestination) is not null
			                    Group by
					                    Month
					                    , Coalesce(IdLoadPort, IdLoadLocation)
					                    , Coalesce(IdDisPort, IdDestination)
					                    , IdShippingRoute
					                    , s.ShipClass
		                    ) s
		                    On	s.IdDischargePort = t.IdDischargePort
		                    And	s.IdLoadPort = t.IdLoadPort
		                    And s.Month = t.Month
		                    And	ISNULL(s.IdShippingRoute, 0) = ISNULL(t.IdShippingRoute,0)
		                    And	ISNULL(NULLIF(s.VesselClass,''), 0) = ISNULL(NULLIF(t.VesselClass,''),0)
                    When	Matched And	s.DaysMinimum < t.DaysMinimum Then
                    Update	Set t.DaysMinimum = s.DaysMinimum
                    When	Not Matched By Target Then
                    Insert	(Month,IdLoadport,IdDischargePort,IdShippingRoute,VesselClass,DaysMinimum)
                    Values	(s.Month, s.IdLoadPort, s.IdDischargePort, IdShippingRoute, VesselClass, DaysMinimum);";

            using (var connection = ConnectionFactory.GetOpenConnection())
            {
                connection.Execute(sql);
            }
        }

        private async Task CleanBatch(IEnumerable<ClipperData> batch)
        {
            Debug.WriteLine($"Clean Batch {DateTime.Now}");

            foreach (ClipperData clipperData in batch)
            {
                try
                {
                    ClipperCleanerDataList.Add(CleanStem(clipperData).Result);
                }
                catch (Exception e)
                {
                    string error =
                        $"INSERT INTO CleanerData_Validation (SourceName, ExtRef,IsSuccess,Value,Message) VALUES (@source,@extRef,@success,@value,@message)";

                    using (var connection = ConnectionFactory.GetOpenConnection())
                    {
                        await connection.ExecuteAsync(error,
                            new {source = "Clipper", extRef = clipperData.RowNum, success = 0,
                                value = e.InnerException?.InnerException?.Message ?? e.InnerException?.Message ?? e.Message, message = "Error"});
                    }
                }
            }
        }

        public async Task<ClipperCleanerData> CleanStem(ClipperData clipperData)
        {
            var theloadDate = TryGetDate(clipperData.LoadDate);
            var idLoadLocation = GetLocationId(clipperData, true);
            var loadPort = idLoadLocation != null ? GetPortParent((int)idLoadLocation) : null;


            var grade = GetGradeByName(clipperData, loadPort);
            var idDischargeLocation = GetLocationId(clipperData, false) ??
                                      GetLocationIdByRegion(
                                          !string.IsNullOrWhiteSpace(clipperData.OffTakeArea)
                                              ? clipperData.OffTakeArea
                                              : !string.IsNullOrWhiteSpace(clipperData.OffTakeRegion)
                                                  ? clipperData.OffTakeRegion
                                                  : null,
                                          clipperData.OffTakeRegion);
            var loadDate = theloadDate;
            var dischargeDate = TryGetDate(clipperData.OffTakeDate);
            var dischargePort = idDischargeLocation != null ? GetPortParent((int)idDischargeLocation) : null;

            var quantity = clipperData.BblsNominal;
            var idUom = GetUnitOfMeasureId(clipperData);
            var idEquity = GetCounterpartId(clipperData);
            var vessel = GetVesselFromImoString(clipperData.Imo.ToString());
            var idCharterer = GetChartererIdFromName(clipperData.Consignee);
            var idSource = _sourceClipper;
            var route = GetRoute(clipperData.Route);
            var speed = clipperData.Speed;
            var draught = clipperData.Draught;
            var fixDate = TryGetDate(clipperData.Fix_Date);
            var laycan = TryGetDate(clipperData.Laycan);
            var loadDay = theloadDate?.Day ?? laycan?.Day ?? 0;
            var loadMonth = theloadDate?.Month ?? laycan?.Month ?? 0;
            var loadYear = theloadDate?.Year ?? laycan?.Year ?? 0;
            var cargoId = $"{(grade.Name.Length > 2 ? grade.Name.Substring(0, 3) : grade.Name.Substring(0, 2))}_{loadYear}{loadMonth % 100:00}{loadDay % 100:00}_CLPR";
            var chartererInfo = !string.IsNullOrEmpty(clipperData.Fix_Date) && theloadDate == null;
            var idClipperCharterer = !string.IsNullOrWhiteSpace(clipperData.Charterer)
                ? GetChartererIdFromName(clipperData.Charterer)
                : null;

            //var thisVoyageTime = dischargeDate?.Subtract(theloadDate.Value).Days ?? -1;

            //if (thisVoyageTime != -1)
            //{
            //    if((loadPort != null || idLoadLocation != null) && (dischargePort != null || idDischargeLocation != null))
            //        AddVoyageTime(loadPort ?? (idLoadLocation ?? -1), dischargePort ?? (idDischargeLocation ?? -1), route, vessel.ShipClass, loadMonth,
            //            thisVoyageTime);
            //}

            var clipperCleanerData = new ClipperCleanerData();
            clipperCleanerData.DateNum = clipperData.DateNum;
            clipperCleanerData.RowNum = clipperData.RowNum;
            clipperCleanerData.StatNum = clipperData.StatNum;
            clipperCleanerData.CargoId = cargoId;
            clipperCleanerData.ChartererInfo = chartererInfo;
            clipperCleanerData.ClipperDataRowVersion = clipperData.ClipperDataRowVersion;
            clipperCleanerData.CreationDate = DateTime.Now;
            clipperCleanerData.CreationName = "Clipper";
            clipperCleanerData.DischargeDate = dischargeDate;
            clipperCleanerData.Draught = draught;
            clipperCleanerData.EndLaycan = laycan ?? loadDate;
            clipperCleanerData.FixDate = fixDate;
            clipperCleanerData.IdCharterer = idCharterer;
            clipperCleanerData.IdClipperCharterer = idClipperCharterer;
            clipperCleanerData.IdDestination = idDischargeLocation;
            clipperCleanerData.IdDisport = dischargePort;
            clipperCleanerData.IdEquity = idEquity;
            clipperCleanerData.IdGrade = grade.Id;
            clipperCleanerData.IdLoadLocation = idLoadLocation;
            clipperCleanerData.IdLoadPort = loadPort;
            clipperCleanerData.IdShippingRoute = route;
            clipperCleanerData.IdShip = vessel?.Id;
            clipperCleanerData.IdUnitOfMeasure = idUom;
            clipperCleanerData.LoadDate = loadDate;
            clipperCleanerData.Month = loadMonth;
            clipperCleanerData.Quantity = quantity;
            clipperCleanerData.Source = idSource;
            clipperCleanerData.Speed = speed;
            clipperCleanerData.StartLaycan = laycan ?? loadDate;
            clipperCleanerData.Year = loadYear;

            if (++_processedCount % 1000 == 0)
                Debug.WriteLine($"Processed {_processedCount} rows");

            return clipperCleanerData;
        }

        private static List<ClipperData> GetRawDataRowsToInsert()
        {
            Debug.WriteLine($"Get Rows begin {DateTime.Now}.{DateTime.Now.Millisecond}");
            using (var connection = ConnectionFactory.GetOpenConnection())
            {
                var newRows = connection.Query<ClipperData>(
                    @"Create Table #temp_UnmatchedRows (
	                    Rownum int
	                    , Imo int
	                    , BblsNominal int
	                    , LoadDate varchar(100)
	                    , Probability float
	                    , Projection varchar(5)
	                    , grade varchar(100)
                    );

                    Create Table #temp_NewRows (
	                    Rownum int
                    );
                    Begin Try
	                    Insert #temp_UnmatchedRows
                    Select	Rownum 
		                    , Imo
		                    , BblsNominal
		                    , LoadDate
		                    , Probability
		                    , Projection
		                    , grade
                    From	ClipperData 
                    Where	Rownum in ( 
                                SELECT Rownum FROM (
                                    SELECT 'ClipperData' as TableName, cd.RowNum, cd.ClipperDataRowVersion
                                    FROM ClipperData cd
                                    UNION ALL
                                    SELECT 'CleanerStaging' as TableName, ccd.RowNum, ccd.ClipperDataRowVersion
                                    FROM ClipperCleanerData ccd
                                ) tmp
                                Group By
                                RowNum, ClipperDataRowVersion
                                HAVING COUNT(*) = 1
                            )

	                    insert	#temp_NewRows
                    Select	allUnmatched.Rownum 
                    from	#temp_UnmatchedRows allUnmatched
		                    Left Join (
		                        Select	
			                        Imo, BblsNominal, LoadDate, Probability, RowNum, Rank() over (Partition by Imo, BblsNominal, LoadDate, Grade order by Probability desc, RowNum asc) as Rank
		                        From	
			                        ClipperData
		                        Where	
			                        projection = 'yes'
	                        ) lowRankedProjects
			                    On	lowRankedProjects.RowNum = allUnmatched.Rownum
			                    And	Rank = 1
                    Where	(lowRankedProjects.Probability is not null or (len(allUnmatched.Projection) = 0 or allUnmatched.Projection = 'no'))

	                Select DateNum
                            , rawdata.rownum
		                    , statnum
		                    , cast( rawdata.[Type] as varchar(200)) Type 
		                    , BblsNominal
		                    , cast( charter_grade as varchar(50)) charter_grade 
		                    , cast( charter_load_area as varchar(100)) charter_load_area 
		                    , cast( charter_offtake_area as varchar(100)) charter_offtake_area 
		                    , cast( charterer as varchar(10)) charterer 
		                    , cast( Consignee as varchar(100)) Consignee 
		                    , cast( declaredDest as varchar(100)) declaredDest 
		                    , draught
		                    , cast( fix_date as varchar(50)) fix_date 
		                    , cast( grade as varchar(50)) grade 
		                    , Imo
		                    , cast( Lat as varchar(100)) Lat 
		                    , cast( Laycan as varchar(100)) Laycan 
		                    , cast( Lightering_vessel as varchar(100)) Lightering_vessel 
		                    , cast( LoadDate as varchar(50)) LoadDate 
		                    , cast( LoadOwner as varchar(50)) LoadOwner 
		                    , cast( LoadPoint as varchar(100)) LoadPoint 
		                    , cast( Loadport as varchar(100)) Loadport 
		                    , cast( LoadArea as varchar(100)) LoadArea 
		                    , cast( LoadRegion as varchar(100)) LoadRegion 
		                    , cast( LoadStsVessel as varchar(100)) LoadStsVessel 
		                    , cast( Load_sts_imo as varchar(50)) Load_sts_imo 
		                    , cast( Lon as varchar(50)) Lon 
		                    , cast( Offtakepoint as varchar(100)) Offtakepoint 
		                    , cast( offtakePort as varchar(100)) offtakePort 
		                    , cast( offTakeArea as varchar(100)) offTakeArea 
		                    , cast( offtakeRegion as varchar(100)) offtakeRegion 
		                    , cast( OffTakeDate as varchar(100)) OffTakeDate 
		                    , cast( OffTakeStsVessel as varchar(100)) OffTakeStsVessel 
		                    , cast( OffTake_sts_imo as varchar(50)) OffTake_sts_imo 
		                    , cast( OffTakeOwner as varchar(50)) OffTakeOwner 
		                    , cast( OpecNopec as varchar(10)) OpecNopec 
		                    , Probability 
		                    , cast( ProbabilityGroup as varchar(50)) ProbabilityGroup 
		                    , Processed
		                    , cast( Projection as varchar(5)) Projection 
		                    , cast( Route as varchar(50)) Route 
		                    , cast( Shipper as varchar(50)) Shipper 
		                    , cast( rawdata.Source as varchar(50)) Source 
		                    , Speed
		                    , cast( Vessel as varchar(50)) Vessel 
		                    , ClipperDataRowVersion
                            , lLoadPoint.Id 'LoadPointId'
		                    , lLoadPoint.Name 'LoadPointName'
		                    , pofftakePoint.Id 'offtakePointId'
		                    , lofftakePoint.Name 'offtakePointName'
                    From	#temp_NewRows newRows
		                    Inner Join ClipperData rawdata
			                    On rawdata.Rownum = newrows.rownum	
                            Left Join MapType mt 
			                    On	mt.Name = 'Location'
		                    Left Join Mapping mGradeCountry
			                    On	mGradeCountry.IdMapType = mt.Id
			                    And	mGradeCountry.ExternalValue = rawData.GradeCountry
		                    Left Join Mapping mGradeRegion
			                    On	mGradeRegion.IdMapType = mt.Id
			                    And	mGradeRegion.ExternalValue = rawData.GradeRegion
		                    LEFT JOIN Location lGradeCountry
			                    ON  lGradeCountry.Name = Coalesce(mGradeCountry.TargoValue, rawdata.GradeCountry) 
			                    AND lGradeCountry.Id in (Select Id From Area Where kind = 3)
			                    AND Coalesce(mGradeCountry.TargoValue, rawdata.GradeCountry) not in (Select distinct CountryName from RegionView WHERE CountryName  <> '' and RegionName IS NULL)
		                    LEFT JOIN (
		                        SELECT 
			                        Name
		                        FROM
			                        Location l
		                        GROUP BY
			                        Name
	                        ) lGradeRegion
		                        ON  lGradeRegion.Name = Coalesce(mGradeRegion.TargoValue, rawdata.GradeRegion) 
		                    Left Join Mapping mLoadPoint
			                    On	mLoadPoint.IdMapType = mt.Id
			                    And	mLoadPoint.ExternalValue = Coalesce(nullif(rawdata.LoadPoint,'UNKNOWN'),lGradeCountry.Name, lGradeRegion.name)
		                    LEFT JOIN  (
			                    Select 
				                    min(l.Id) Id 
				                    , l.Name
			                    From
				                    Location l
				                    Left Join Port p
					                    On	p.Id = l.Id
			                    GROUP BY
				                    l.Name
		                    ) lLoadPoint
			                    ON lLoadPoint.Name = Coalesce(mLoadPoint.TargoValue, nullif(rawdata.LoadPoint,'UNKNOWN'),lGradeCountry.Name, lGradeRegion.name)
		                    Left Join Mapping mofftakePoint
			                    On	mofftakePoint.IdMapType = mt.Id
			                    And	mofftakePoint.ExternalValue = rawdata.offtakePoint
		                    LEFT JOIN (
			                    Select 
				                    min(l.Id) Id 
				                    , l.Name
			                    From
				                    Location l
				                    Left Join Port p
					                    On	p.Id = l.Id
			                    GROUP BY
				                    l.Name
		                    ) lofftakePoint
			                    ON  lofftakePoint.Name = Coalesce(mofftakePoint.TargoValue, rawdata.offtakePoint)
		                    Left Join Port pofftakePoint
			                    On	pofftakePoint.Id = lofftakePoint.Id


	                    IF OBJECT_ID('tempdb.dbo.#temp_UnmatchedRows', 'U') IS NOT NULL
	                    DROP TABLE #temp_UnmatchedRows;

	                    IF OBJECT_ID('tempdb.dbo.#temp_NewRows', 'U') IS NOT NULL
	                    DROP TABLE #temp_NewRows;
                    End Try
                    Begin Catch
	                    if @@trancount > 0
		                    rollback transaction;

	                    -- Clean Up
	                    IF OBJECT_ID('tempdb.dbo.#temp_UnmatchedRows', 'U') IS NOT NULL
	                    DROP TABLE #temp_UnmatchedRows;

	                    IF OBJECT_ID('tempdb.dbo.#temp_NewRows', 'U') IS NOT NULL
	                    DROP TABLE #temp_NewRows;

	                    THROW;
                    End Catch", commandTimeout: 120000);
                Console.WriteLine($"Get Rows end {DateTime.Now}");
                return newRows.ToList();
            }
        }

        public int? GetPortParent(int idLocation)
        {
            return (from p in PortsList
                    where p.Id == idLocation
                    select p.IdParent).FirstOrDefault();
        }

        public Grade GetGradeByName(ClipperData clipperData, int? idLoadPort)
        {
            if (string.IsNullOrWhiteSpace(clipperData.Grade) && string.IsNullOrWhiteSpace(clipperData.Charter_Grade)) return null;
            string stemGrade = !string.IsNullOrWhiteSpace(clipperData.Grade)
                ? clipperData.Grade
                : clipperData.Charter_Grade;

            var mappedGrade = string.Empty;

            mappedGrade = GetTargoValue("Grade", stemGrade).ToLower();

            if (mappedGrade?.ToLower() == "exception")
                mappedGrade = ResolveGrade(stemGrade, clipperData.LoadPort);

            return GradesList.Any(x => x.Name.ToLower() == mappedGrade.ToLower())
                ? GradesList.FirstOrDefault(x => x.Name.ToLower() == mappedGrade.ToLower())
                : !string.IsNullOrWhiteSpace(mappedGrade)
                    ? AddGrade(mappedGrade.ToUpper())
                    : null;
        }

        public string GetTargoValue(string type, string externalValue)
        {
            if (string.IsNullOrWhiteSpace(externalValue)) return string.Empty;

            return MappingsList.Any(x => x.MapType == type && x.ExternalValue.ToLower() == externalValue.ToLower())
                ? MappingsList.FirstOrDefault(x => x.MapType == type && x.ExternalValue.ToLower() == externalValue.ToLower())?
                    .TargoValue
                : externalValue;
        }

        public int? GetLocationId(ClipperData clipperData, bool isLoad)
        {
            var point = isLoad ? GetTargoValue("Location", clipperData.LoadPoint)
                : GetTargoValue("Location", clipperData.OffTakePoint);

            var port = isLoad ? GetTargoValue("Location", clipperData.LoadPort)
                : GetTargoValue("Location", clipperData.OffTakePort);

            if (string.IsNullOrWhiteSpace(point) && string.IsNullOrWhiteSpace(port)) return null;

            int? terminalId;
            int? portId;
            
            if (port?.ToLower() == "exception")
            {
                var area = isLoad
                    ? GetTargoValue("Location", clipperData.LoadArea)
                    : GetTargoValue("Location", clipperData.OffTakeArea);
                port = ResolveLocation(point, port, area).ToLower();

                terminalId = !string.IsNullOrWhiteSpace(point) ? GetTargoLocationId(point, 2)?.Id : null;

                var targoPort = GetTargoLocationId(port, 1);

                portId = null;
                if (targoPort?.Id == null)
                {
                    if(!string.IsNullOrWhiteSpace(port))
                    {
                        portId = AddPort(port, 1, _portUnallocated);
                    }
                }
            }
            else
            {
                terminalId = isLoad ? clipperData.LoadPointId : clipperData.OfftakePointId;
                var idParent = terminalId.HasValue ? GetPortParent(terminalId.Value) : null;

                if (idParent != null)
                    portId = idParent;
                else
                {
                    portId = GetTargoLocationId(port, 1)?.Id;
                    if (portId == null)
                    {
                        if (!string.IsNullOrWhiteSpace(port))
                        {
                            portId = AddPort(port, 1, _portUnallocated);
                        }
                    }
                }
            }
            if(terminalId == null)
            {
                if (!string.IsNullOrWhiteSpace(point))
                {
                    terminalId = AddPort(point, 2, portId);
                }
            }

            return terminalId ?? portId;
        }

        public int? GetLocationIdByRegion(string areaName, string regionName)
        {
            var mappedLocation = string.Empty;
            mappedLocation = GetTargoValue("Location", areaName).ToLower();
            
            if (mappedLocation == "exception")
                mappedLocation = GetTargoValue("Location", regionName).ToLower();

            int? idLocation;
            idLocation = AreasList.Any(x => x.Name.ToLower() == mappedLocation)
                ? AreasList.FirstOrDefault(x => x.Name.ToLower() == mappedLocation)?.Id
                : 0;

            return idLocation == 0 ? null : idLocation;
        }

        public int? GetRoute(string route)
        {
            if (string.IsNullOrWhiteSpace(route)) return null;

            return ShippingRoutesList.FirstOrDefault(sr => sr.Name == route)?.Id ??
                   (!string.IsNullOrWhiteSpace(route) ? AddRoute(route) : null);
        }

        public int? GetUnitOfMeasureId(ClipperData clipperData)
        {
            switch (clipperData.Type)
            {
                case "measuresGlobalCrudeEntity":
                    return UnitOfMeasuresList.FirstOrDefault(u => u.Name.ToLower().Contains("bbl"))?.Id;
                default:
                    return UnitOfMeasuresList.FirstOrDefault(u => u.Name.ToLower().Contains("bbl"))?.Id;

            }
        }

        public Ship GetVesselFromImoString(string imo)
        {
            var ship = (ShipsList.Any(s => s.Imo == $"IMO{imo}")
                           ? ShipsList.First(x => x.Imo == $"IMO{imo}")
                           : null) ?? ShipsList.First(x => x.Imo == "IMO0");

            return ship;
        }

        public Ship GetVesselFromName(string name)
        {
            return ShipsList.FirstOrDefault(p => p.Name == name);
        }

        public int? GetCounterpartId(ClipperData clipperData)
        {
            if (string.IsNullOrWhiteSpace(clipperData.LoadOwner) || clipperData.LoadOwner == "UNKNOWN") return null;

            var name = clipperData.LoadOwner;

            var result = CounterpartsList.Any(x => x.Name.ToLower() == name.ToLower())
                ? CounterpartsList.FirstOrDefault(x => x.Name.ToLower() == name.ToLower())?.Id
                : AddCounterpart(name);

            if (result == 0) return null;
            return result;
        }

        public Port GetTargoLocationId(string nameLocation, int kind)
        {
            var port = PortsList.FirstOrDefault(p => p.Name.ToLower() == nameLocation.ToLower());
            return port;
        }

        public int? GetChartererIdFromName(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) return null;
            var charterer = GetTargoValue("Charterer", name);

            var result = CharterersList.Any(x => x.Name.ToLower() == charterer.ToLower())
                ? CharterersList.FirstOrDefault(x => x.Name.ToLower() == charterer.ToLower())?.Id
                : AddCharterer(charterer);

            if (result == 0) return null;
            return result;
        }

        private int? AddCounterpart(string name)
        {
            var newObj = new Counterpart()
            {
                Name = name
            };

            string sql =
                $"INSERT INTO Counterpart (Name,IdSource) VALUES (@name,@_sourceClipper); SELECT CAST(SCOPE_IDENTITY() as int)";

            using (var connection = ConnectionFactory.GetOpenConnection())
            {
                newObj.Id = connection.Query<int>(sql, new {newObj.Name, _sourceClipper}).Single();
                CounterpartsList.Add(newObj);
            }

            return newObj.Id;
        }

        private int? AddCharterer(string name)
        {
            var newObj = new Charterer()
            {
                Name = name
            };
            string sql =
                $"INSERT INTO Charterer (Name, IdSource) VALUES (@name,@_sourceClipper); SELECT CAST(SCOPE_IDENTITY() as int)";

            using (var connection = ConnectionFactory.GetOpenConnection())
            {
                newObj.Id = connection.Query<int>(sql, new { newObj.Name, _sourceClipper }).Single();
                CharterersList.Add(newObj);

            }

            return newObj.Id;
        }

        private int? AddPort(string location, int kind, int? idParent)
        {
            if (location == "UNKNOWN") return null;

            var port = new Port()
            {
                Name = location,
                Kind = kind,
                IdParent = idParent
            };
            
            string sqlLocation =
                $"IF (SELECT top 1 Id from [Location] where Name = @name) IS NULL BEGIN  INSERT INTO [Location] (Name, Source) VALUES (@name,@_sourceClipper); SELECT CAST(SCOPE_IDENTITY() as int) END ELSE (SELECT top 1 Id from [Location] where Name = @name AND Source = @_sourceClipper)";
            string sqlPort =
                    $"INSERT INTO Port (Id, Kind, IdParent) VALUES (@Id,@Kind,@portParent);";

            using (var connection = ConnectionFactory.GetOpenConnection())
            {
                if (AreasList.Any(a => a.Name == port.Name)) return null;
                else port.Id = connection.Query<int>(sqlLocation, new { port.Name, _sourceClipper }).Single();

                var portParent = (port.IdParent == null ? _portUnallocated.ToString() : port.IdParent.ToString());

                if (PortsList.Select(s => s.Id == port.Id).Count() == 0)
                {
                    connection.Execute(sqlPort, new { port.Id, port.Kind, portParent });
                    PortsList.Add(port);
                }
            }

            return port.Id;
        }

        private int? AddRoute(string routeName)
        {
            var newObj = new ShippingRoute()
            {
                Name = routeName,
            };

            string sql =
                $"IF (SELECT top 1 Id from [ShippingRoute] where Name = @name) IS NULL BEGIN INSERT INTO ShippingRoute (Name) VALUES (@name); SELECT CAST(SCOPE_IDENTITY() as int) END ELSE (SELECT top 1 Id from [ShippingRoute] where Name = @name)";

            using (var connection = ConnectionFactory.GetOpenConnection())
            {
                newObj.Id = connection.Query<int>(sql, new {newObj.Name}).Single();
                if (ShippingRoutesList.Select(s => s.Id == newObj.Id).Count() == 0)
                {
                    ShippingRoutesList.Add(newObj);
                }
            }

            return newObj.Id;
        }

        private Grade AddGrade(string name)
        {
            var newObj = new Grade()
            {
                Name = name,
                IdParent = _gradeUnallocated
            };

            string sql =
                $"IF (SELECT top 1 Id from [Grade] where Name = @name) IS NULL BEGIN  INSERT INTO Grade (Name, IdParent, IdSource) VALUES (@name,@idParent,@_sourceClipper); SELECT CAST(SCOPE_IDENTITY() as int) END ELSE (SELECT top 1 Id from [Grade] where Name = @name)";

            using (var connection = ConnectionFactory.GetOpenConnection())
            {
                newObj.Id = connection.Query<int>(sql, new {newObj.Name, newObj.IdParent, _sourceClipper }).Single();

                if (GradesList.Select(s => s.Id == newObj.Id).Count() == 0)
                {
                    GradesList.Add(newObj);
                }
            }

            return newObj;
        }

        private static string ResolveLocation(string point, string nameLocation, string area)
        {
            string resolvedName;

            if (nameLocation == "PORTLAND")
            {
                switch (area)
                {
                    case "USAC":
                        resolvedName = "Portland(ME USA)";
                        break;
                    default:
                        resolvedName = "Portland(OR USA)";
                        break;
                }
            }
            else if (nameLocation == "FREEPORT")
            {
                switch (area)
                {
                    case "CAR":
                        resolvedName = "Freeport(BHS)";
                        break;
                    case "CMED":
                        resolvedName = "Malta STS";
                        break;
                    default:
                        resolvedName = "Freeport(Texas)";
                        break;
                }
            }
            else if (nameLocation == "HIGH SEAS")
            {
                resolvedName = point;
            }
            else resolvedName = point;
            return resolvedName;
        }

        private string ResolveGrade(string clipperGrade, string loadPort)
        {
            string portName = loadPort;

            if (string.IsNullOrWhiteSpace(portName)) return clipperGrade;

            if (clipperGrade.ToLower() == "turkish crude")
            {
                if (portName.ToLower() == "ceyhan botas")
                {
                    if (GradesList.Any(g => g.Name.ToLower() == "kirkuk"))
                        return GradesList.FirstOrDefault(g => g.Name.ToLower() == "kirkuk")?.Name;
                }
            }
            if (clipperGrade.ToLower() == "russian export blend" ||
                clipperGrade.ToLower() == "urals")
            {
                if (portName.ToLower() == "novorossiysk")
                {
                    if (GradesList.Any(g => g.Name.ToLower() == "urals med"))
                        return GradesList.FirstOrDefault(g => g.Name.ToLower() == "urals med")?.Name;
                }
                else
                {
                    if (GradesList.Any(g => g.Name.ToLower() == "urals nth"))
                        return GradesList.FirstOrDefault(g => g.Name.ToLower() == "urals nth")?.Name;
                }
            }
            if (clipperGrade.ToLower() == "russian crude")
            {
                if (portName.ToLower() == "novorossiysk")
                {
                    if (GradesList.Any(g => g.Name.ToLower() == "russian crude east"))
                        return GradesList.FirstOrDefault(g => g.Name.ToLower() == "russian crude east")?.Name;
                }
                else
                {
                    if (GradesList.Any(g => g.Name.ToLower() == "russian crude nwe"))
                        return GradesList.FirstOrDefault(g => g.Name.ToLower() == "russian crude nwe")?.Name;
                }
            }
            if (clipperGrade.ToLower() == "iraqi crude")
            {
                if (portName.ToLower() == "ceyhan botas" ||
                    portName.ToLower() == "mersin" ||
                    portName.ToLower().Contains("dortyol"))
                {
                    if (GradesList.Any(g => g.Name.ToLower() == "kirkuk"))
                        return GradesList.FirstOrDefault(g => g.Name.ToLower() == "kirkuk")?.Name;
                }
                else
                {
                    if (GradesList.Any(g => g.Name.ToLower() == "iraq crude south"))
                        return GradesList.FirstOrDefault(g => g.Name.ToLower() == "iraq crude south")?.Name;
                }
            }

            if (clipperGrade.ToLower() == "canadian crude" ||
                clipperGrade.ToUpper() == "GIRASSOL" ||
                clipperGrade.ToUpper() == "HEBRON" ||
                clipperGrade.ToUpper() == "HIBERNIA" ||
                clipperGrade.ToUpper() == "HUNGO" ||
                clipperGrade.ToUpper() == "SANGOS" ||
                clipperGrade.ToUpper() == "TERRA NOVA" ||
                clipperGrade.ToUpper() == "WHITE ROSE")
            {
                if (portName.ToLower().Contains("whiffen"))
                {
                    if (GradesList.Any(g => g.Name.ToLower() == "canadian sweet crude"))
                        return GradesList.FirstOrDefault(g => g.Name.ToLower() == "canadian sweet crude")?.Name;
                    return "Canadian Sweet Crude";
                }
            }

            if (clipperGrade.ToLower() == "crude")
            {
                if (portName.ToLower().Contains("whiffen"))
                {
                    if (GradesList.Any(g => g.Name.ToLower() == "canadian sweet crude"))
                        return GradesList.FirstOrDefault(g => g.Name.ToLower() == "canadian sweet crude")?.Name;
                    return "Canadian Sweet Crude";
                }
                return portName + " Crude";
            }

            return clipperGrade;
        }

        private DateTime? TryGetDate(string date)
        {
            if (string.IsNullOrWhiteSpace(date)) return null;
            DateTime result;
            DateTime.TryParse(date, out result);
            return result;
        }

        private void SetGlobalVariables()
        {
            if (SourcesList.Any(s => s.Type == "Stem" && s.Name == "Clipper"))
                _sourceClipper = SourcesList.FirstOrDefault(s => s.Type == "Stem" && s.Name == "Clipper").Id;

            if(PortsList.Any(p => p.Name.ToLower().Contains("unallocated")))
                _portUnallocated = PortsList.FirstOrDefault(p => p.Name.ToLower().Contains("unallocated")).Id;
            else throw new Exception("Add unallocated port to use as parent for new ports");

            if (GradesList.Any(p => p.Name.ToLower().Contains("unallocated")))
                _gradeUnallocated = GradesList.FirstOrDefault(g => g.Name.ToLower().Contains("unallocated")).Id;
            else throw new Exception("Add unallocated grade to use as parent for new grades");
        }

        private void GetStaticData()
        {
            using (var connection = ConnectionFactory.GetOpenConnection())
            {
                GradesList = new ConcurrentBag<Grade>(connection.Query<Grade>("Select * from Grade").ToList());
                ShipsList = new ConcurrentBag<Ship>(connection
                    .Query<Ship>(
                        "Select s.*, sd.Name, sd.ValidUntil from Ship s left Join ShipDetails sd on sd.IdShip = s.Id ")
                    .ToList());
                AreasList = new ConcurrentBag<Area>(connection.Query<Area>("Select a.Id, a.Kind, l.Name from Area a Inner Join Location l on l.Id = a.Id").ToList());
                MappingsList = new ConcurrentBag<Mapping>(connection
                    .Query<Mapping>(
                        "Select m.Id, m.ExternalValue,m.TargoValue,mt.Name 'MapType' From Mapping m left Join Maptype mt on mt.Id = m.IdMaptype")
                    .ToList());
                PortsList = new ConcurrentBag<Port>(connection.Query<Port>("Select p.Id, p.IdParent, p.Kind, l.Name from Port p inner join Location l on l.Id = p.Id").ToList());
                SourcesList = new ConcurrentBag<Source>(connection
                    .Query<Source>(
                        "Select s.*, st.Name 'Type' from Source s left Join SourceType st on st.Id = s.IdType")
                    .ToList());
                UnitOfMeasuresList = new ConcurrentBag<UnitOfMeasure>(connection.Query<UnitOfMeasure>("Select * from UnitOfMeasure").ToList());
                CounterpartsList = new ConcurrentBag<Counterpart>(connection.Query<Counterpart>("Select * from Counterpart").ToList());
                CharterersList = new ConcurrentBag<Charterer>(connection.Query<Charterer>("Select * from Charterer").ToList());
                ShippingRoutesList = new ConcurrentBag<ShippingRoute>(connection.Query<ShippingRoute>("Select * from ShippingRoute").ToList());
            }
        }
    }
}
