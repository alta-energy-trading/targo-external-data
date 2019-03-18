Create Procedure [dbo].[sp_LoadClipperStagingToClipperData] @server varchar(55)
															, @database varchar(55)
As

create table [#temp_CleanDataLog] (
	IdSource int,
	TableName nvarchar(255),
	[Action] nvarchar(255),
	IdRecord int,
	ExtRef int,
);

Begin Try
	Begin Tran

		Insert	#temp_CleanDataLog (
			IdSource
			, TableName
			, [Action]
			, IdRecord
			, ExtRef
		)
		Select	src.Id
				, 'ClipperData'
				, 'Delete'
				, cd.DateNum
				, cd.RowNum
		From	dbo.ClipperData cd
				Left Outer Join ClipperData_Deletes d
					On cd.RowNum = d.RowNum
				Left Join SourceType st 
					On	st.Name = 'Stem'
				Inner Join Source src
					On	src.IdType = st.Id
					And	src.Name = 'Clipper'
		Where	d.RowNum IS NULL or (cd.ClipperDataRowVersion is null  or cd.ClipperDataRowVersion <> d.ClipperDataRowVersion)

		Insert CleanDataLog (
			IdSource
			, TableName
			, [Action]
			, IdRecord
			, ExtRef
		)
		Select	*
		From	#temp_CleanDataLog

		Delete From dbo.ClipperData 
		Where RowNum in (
			Select	ExtRef
			From	#temp_CleanDataLog
		)

		-- Insert New and Updated rows into Clipper Data
		DECLARE @Sql nvarchar(Max) = '';
		DECLARE @SelectSql nvarchar(Max) = '';
		DECLARE @columnSql nvarchar(Max) = '';
		DECLARE @correctedColumnSql nvarchar(Max) = '';
		DECLARE @WhereSql nvarchar(Max) = '';
		DECLARE @JoinSql varchar(Max) = '';
		DECLARE @MyCursor CURSOR;
		DECLARE @columnName varchar(255);

		SET @MyCursor = CURSOR FOR
		SELECT 
			c.name 'ColumnName'
		FROM    
			sys.columns c
		WHERE
			c.object_id = OBJECT_ID('ClipperData')
			and c.Name not in ('ClipperDataRowVersion')

		OPEN @MyCursor 
		FETCH NEXT FROM @MyCursor 
		INTO @columnName

		WHILE @@FETCH_STATUS = 0
		BEGIN
			-- get select columns
			set @correctedColumnSql = @correctedColumnSql + 'ISNULL(c'+ @columnName + '.NewValue, cs.' + @columnName + ') ' + @columnName + ', '

			-- get insert columns
			set @columnSql = @columnSql + @columnName + ','

			--build join to correction table for every column except rowversion; this allows us to get a corrected value per row per column
			set @JoinSql = @JoinSql + 'left join [dbo].Correction c' +  @columnName + '
				on	c' +  @columnName + '.IdSource = (Select s.id from Source s left join SourceType st on st.Id = s.IdType Where s.name = ''Clipper'' and st.Name = ''Stem'') 
				and	c' +  @columnName + '.ExtRef = cs.RowNum
				and c' +  @columnName + '.ColumnName = ''' +  @columnName + '''
				and c' +  @columnName + '.OldValue = cs.' +  @columnName + '
				'
		  FETCH NEXT FROM @MyCursor 
		  INTO @columnName 
		END; 

		CLOSE @MyCursor ;
		DEALLOCATE @MyCursor;

		-- remove trailing comma
		Set @columnSql = STUFF(@columnSql, LEN(@columnSql), 1, '')
		Set @correctedColumnSql = STUFF(@correctedColumnSql, LEN(@correctedColumnSql), 1, '')

		-- build query
		SET @SelectSql = 
			'Insert Into [dbo].ClipperData 
			(' +
				@columnSql + ',ClipperDataRowVersion' +
			')
			Select ' +
				@correctedColumnSql + ',RowVersion
			FROM	[' + @server + '].[' + @database + '].[dbo].ClipperStaging cs '

		Set @WhereSql = '
			Where	RowNum in 
			(
				Select	d.Rownum 
				from	ClipperData_Deletes d 
						Left Join	ClipperData cd			on cd.Rownum = d.Rownum 
				Where	cd.RowNum is null
				Or		(cd.ClipperDataRowVersion is null  or cd.ClipperDataRowVersion <> d.ClipperDataRowVersion)
			)';

		Set @Sql = @SelectSql + @JoinSql + @WhereSql;
		  EXECUTE sp_executesql @Sql;
		  --select @sql
	
		truncate table ClipperData_Deletes
		IF OBJECT_ID('tempdb.dbo.#temp_CleanDataLog', 'U') IS NOT NULL drop Table #temp_CleanDataLog

	Commit Tran
End Try
Begin Catch
	If @@TRANCOUNT > 0
		rollback tran
	
	declare @ermessage nvarchar(2048), @erseverity int, @erstate int;
	Select @ermessage = ERROR_MESSAGE(), @erseverity = ERROR_SEVERITY(), @erstate = ERROR_STATE();
	raiserror(@ermessage, @erseverity,@erstate)
	
	IF OBJECT_ID('tempdb.dbo.#temp_CleanDataLog', 'U') IS NOT NULL drop Table #temp_CleanDataLog
End Catch

