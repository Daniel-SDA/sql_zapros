
		DECLARE @Sql_Create_Temp_Table VARCHAR (MAX)
		SET @Sql_Create_Temp_Table = '
EXEC [10.19.34.100].[trade].sys.sp_executesql N''
IF OBJECT_ID(''''[tempdb].[dbo].[Table_РегистрыСведений_м_СтатусыТоваров_СрезПоследних_33_TEMP]'''') IS NOT NULL
		BEGIN
			DROP TABLE [tempdb].[dbo].[Table_РегистрыСведений_м_СтатусыТоваров_СрезПоследних_33_TEMP];
        END;
CREATE TABLE [tempdb].[dbo].[Table_РегистрыСведений_м_СтатусыТоваров_СрезПоследних_33_TEMP]
    (
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Период] [datetime] NULL,
	[Регистратор_Наименование] [varchar](31) NULL,
	[Регистратор_Номер] [nchar](11) NULL,
	[Регистратор_Дата] [datetime] NULL,
	[НомерСтроки] [numeric](9, 0) NOT NULL,
	[Активность] [varchar](3) NULL,
	[Номенклатура_Ссылка] [binary](16) NOT NULL,
	[Подразделение_Ссылка] [binary](16) NOT NULL,
	[ПризнакНоменклатуры_Ссылка] [binary](16) NOT NULL,
	[Статус_Ссылка] [binary](16) NOT NULL,
	[ВременноеВыбытие_Ссылка] [binary](16) NOT NULL,
	[ЖизненныйЦикл_Ссылка] [binary](16) NOT NULL,
	[ЗначенияСвойствНоменклатуры_Ссылка] [binary](16) NOT NULL,
	[ПоДату] [datetime] NOT NULL,
	[Комментарий] [ntext] NULL,
	[Маркетинговый] [varchar](3) NULL,
	[БазаДанных] [int] NOT NULL
    CONSTRAINT PK_ID_Table_РегистрыСведений_м_СтатусыТоваров_СрезПоследних_33_TEMP PRIMARY KEY CLUSTERED ( ID ASC )
    );
INSERT INTO [tempdb].[dbo].[Table_РегистрыСведений_м_СтатусыТоваров_СрезПоследних_33_TEMP] 
SELECT DATEADD(YEAR,-2000,T1.Period_) as Период,  CASE WHEN T1.RecorderTRef = 0x000000F1 THEN ''''Расходный ордер на товары'''' WHEN T1.RecorderTRef = 0x000000BA THEN ''''Корректировка записей регистров'''' WHEN T1.RecorderTRef = 0x000000CD THEN ''''Установка статусов номенклатуры'''' END as Регистратор_Наименование , CASE WHEN T1.RecorderTRef = 0x000000F1 THEN T7._Number WHEN T1.RecorderTRef = 0x000000BA THEN T8._Number WHEN T1.RecorderTRef = 0x000000CD THEN T9._Number END as Регистратор_Номер , CASE WHEN T1.RecorderTRef = 0x000000F1 THEN DATEADD(YEAR,-2000,T7._Date_Time) WHEN T1.RecorderTRef = 0x000000BA THEN DATEADD(YEAR,-2000,T8._Date_Time) WHEN T1.RecorderTRef = 0x000000CD THEN DATEADD(YEAR,-2000,T9._Date_Time) END as Регистратор_Дата , T1.LineNo_ as НомерСтроки, CASE WHEN T1.Active_ = 0x01 THEN ''''Да'''' WHEN T1.Active_ = 0x00 THEN ''''Нет'''' END as Активность, T1.Fld7875RRef as Номенклатура_Ссылка, T1.Fld7876RRef as Подразделение_Ссылка, T1.Fld7878RRef as ПризнакНоменклатуры_Ссылка, T1.Fld7879RRef as Статус_Ссылка, T1.Fld7880RRef as ВременноеВыбытие_Ссылка, T1.Fld7881RRef as ЖизненныйЦикл_Ссылка, T1.Fld7882RRef as ЗначенияСвойствНоменклатуры_Ссылка, T1.Fld7883 as ПоДату, T1.Fld7884 as Комментарий,  CASE WHEN T1.Fld9550 = 0x01 THEN ''''Да'''' WHEN T1.Fld9550 = 0x00 THEN ''''Нет'''' END as Маркетинговый, 33 AS БазаДанных  FROM  (SELECT T6._Period AS Period_, T6._RecorderTRef AS RecorderTRef, T6._RecorderRRef AS RecorderRRef, T6._LineNo AS LineNo_, T6._Active AS Active_, T6._Fld7875RRef AS Fld7875RRef, T6._Fld7876RRef AS Fld7876RRef, T6._Fld7878RRef AS Fld7878RRef, T6._Fld7879RRef AS Fld7879RRef, T6._Fld7880RRef AS Fld7880RRef, T6._Fld7881RRef AS Fld7881RRef, T6._Fld7882RRef AS Fld7882RRef, T6._Fld7883 AS Fld7883, T6._Fld7884 AS Fld7884, T6._Fld9550 AS Fld9550 FROM  (SELECT T3.Fld7875RRef AS Fld7875RRef, T3.Fld7876RRef AS Fld7876RRef, T3.Fld7878RRef AS Fld7878RRef, T3.MAXPERIOD_ AS MAXPERIOD_,SUBSTRING(MAX(T5._RecorderTRef + T5._RecorderRRef),1,4) AS  MAXRECORDERTRef ,SUBSTRING(MAX(T5._RecorderTRef + T5._RecorderRRef),5,16) AS  MAXRECORDERRRef  FROM ( SELECT T4._Fld7875RRef AS Fld7875RRef, T4._Fld7876RRef AS Fld7876RRef, T4._Fld7878RRef AS Fld7878RRef, MAX(T4._Period) AS MAXPERIOD_  FROM [trade].dbo._InfoRg7874 T4 WITH(NOLOCK)  WHERE T4._Active = 0x01 GROUP BY  T4._Fld7875RRef, T4._Fld7876RRef, T4._Fld7878RRef) T3 INNER JOIN [trade].dbo._InfoRg7874 T5 WITH(NOLOCK) ON T3.Fld7875RRef = T5._Fld7875RRef AND T3.Fld7876RRef = T5._Fld7876RRef AND T3.Fld7878RRef = T5._Fld7878RRef AND T3.MAXPERIOD_ =  T5._Period WHERE  T5._Active = 0x01  GROUP BY  T3.Fld7875RRef, T3.Fld7876RRef, T3.Fld7878RRef, T3.MAXPERIOD_) T2 INNER JOIN [trade].dbo._InfoRg7874 T6 WITH(NOLOCK) ON T2.Fld7875RRef = T6._Fld7875RRef AND T2.Fld7876RRef = T6._Fld7876RRef AND T2.Fld7878RRef = T6._Fld7878RRef AND T2.MAXPERIOD_ =  T6._Period AND T2.MAXRECORDERTRef =  T6._RecorderTRef AND T2.MAXRECORDERRRef =  T6._RecorderRRef) T1 LEFT OUTER JOIN [trade].dbo._Document241 T7 WITH(NOLOCK) ON T1.RecorderTRef = 0x000000F1 AND T1.RecorderRRef = T7._IDRRef LEFT OUTER JOIN [trade].dbo._Document186 T8 WITH(NOLOCK) ON T1.RecorderTRef = 0x000000BA AND T1.RecorderRRef = T8._IDRRef LEFT OUTER JOIN [trade].dbo._Document205 T9 WITH(NOLOCK) ON T1.RecorderTRef = 0x000000CD AND T1.RecorderRRef = T9._IDRRef WHERE ((T1.Fld7876RRef = 0x00000000000000000000000000000000) or (T1.Fld7876RRef in (SELECT DISTINCT [Ссылка_ID] FROM OPENQUERY ([192.168.0.252], ''''SELECT DISTINCT [Ссылка_ID], [Наименование] FROM [ETL_1C_SQL].[dbo].[Table_Список_Подразделения] WHERE БазаДанных = 33''''))))'''
		--Формирование временной таблицы на удаленном сервере
		EXEC (@Sql_Create_Temp_Table);
		
		--Переменные для формирования списка
		DECLARE @SQL				NVARCHAR(MAX)
		DECLARE @Error_Message		VARCHAR (4000)
		DECLARE @Is_Debug			INT = 1	
		
		/*
		Создать табличную переменную для хранения диапазона значений равными частями для равномерной выгрузки
		*/
		DECLARE @Ids_Range TABLE 
		(
		id SMALLINT IDENTITY(1, 1) ,
		range_from BIGINT ,
		range_to BIGINT
		);

		/*
		Создание списка для отбора количества строк в запросе, 
		будет использоваться при формировании динамического запроса и выгрузки данных по частям.
		*/

		/*РЕАЛИЗАЦИЯ ЧЕРЕЗ УКАЗАНИЕ ШАГОВ, ЧЕРЕЗ ПАРАМЕТР @SQL_Exec_No, ПО УМОЛЧАНИЮ 5*/
		--SET @SQL =          'DECLARE @R1 BIGINT = (SELECT MIN(ID) AS ID FROM '													+CHAR(13)
		--SET @SQL = @SQL +   '[10.19.34.100].[tempdb].[dbo].[Table_РегистрыСведений_м_СтатусыТоваров_СрезПоследних_33_TEMP])'    +CHAR(13)
		--SET @SQL = @SQL +   'DECLARE @R2 BIGINT = (SELECT MAX(ID) AS ID FROM '													+CHAR(13)
		--SET @SQL = @SQL +   '[10.19.34.100].[tempdb].[dbo].[Table_РегистрыСведений_м_СтатусыТоваров_СрезПоследних_33_TEMP])'   +CHAR(13)
		--SET @SQL = @SQL +   'DECLARE @R3 BIGINT = (SELECT (@R2-@R1+1)/10000 AS ID )'										+CHAR(13)
		--SET @SQL = @SQL +   'DECLARE @t int = @R3+@R3+2 '																			+CHAR(13)
		--SET @SQL = @SQL +   'SELECT @R1, @R3 '																					+CHAR(13)
		--SET @SQL = @SQL +   'UNION ALL '																							+CHAR(13)
		--SET @SQL = @SQL +   'SELECT @R3+1, @R3+@R3+1 '																				+CHAR(13)
		--SET @SQL = @SQL +   'WHILE @t<=@R2 '																						+CHAR(13)
		--SET @SQL = @SQL +   'BEGIN '																								+CHAR(13)
		--SET @SQL = @SQL +   'SELECT @t, CASE WHEN (@t+@R3)>=@R2 THEN @R2 ELSE @t+@R3 END '											+CHAR(13)
		--SET @SQL = @SQL +   'SET @t = @t+@R3+1 '																					+CHAR(13)
		--SET @SQL = @SQL +   'END'																							+CHAR(13)

		/*РЕАЛИЗАЦИЯ ЧЕРЕЗ УКАЗАНИЕ КОЛИЧЕСТВО СТРОК, ЧЕРЕЗ ПАРАМЕТР @SQL_Exec_No, ПО УМОЛЧАНИЮ 10000*/
		SET @SQL =          'DECLARE @R1 BIGINT = (SELECT MIN(ID) AS ID FROM '													+CHAR(13)
		SET @SQL = @SQL +   '[10.19.34.100].[tempdb].[dbo].[Table_РегистрыСведений_м_СтатусыТоваров_СрезПоследних_33_TEMP])'    +CHAR(13)
		SET @SQL = @SQL +   'DECLARE @R2 BIGINT = (SELECT MAX(ID) AS ID FROM '													+CHAR(13)
		SET @SQL = @SQL +   '[10.19.34.100].[tempdb].[dbo].[Table_РегистрыСведений_м_СтатусыТоваров_СрезПоследних_33_TEMP])'   +CHAR(13)
		SET @SQL = @SQL +   'DECLARE @R3 BIGINT = (SELECT (@R2-@R1+1)/(CASE WHEN @R2 < 10000 THEN 1 ELSE (@R2/10000) END) AS ID)'		+CHAR(13)
		SET @SQL = @SQL +   'DECLARE @t int = @R3+@R3+2 '																			+CHAR(13)
		SET @SQL = @SQL +   'IF @R2 < (10000*2)'																					+CHAR(13)
		SET @SQL = @SQL +   'SELECT @R1, @R3 '																					+CHAR(13)
		SET @SQL = @SQL +   'ELSE '																							+CHAR(13)
		SET @SQL = @SQL +   'SELECT @R1, @R3 '																					+CHAR(13)
		SET @SQL = @SQL +   'UNION ALL '																							+CHAR(13)
		SET @SQL = @SQL +   'SELECT @R3+1, @R3+@R3+1 '																				+CHAR(13)
		SET @SQL = @SQL +   'WHILE @t<=@R2 '																						+CHAR(13)
		SET @SQL = @SQL +   'BEGIN '																								+CHAR(13)
		SET @SQL = @SQL +   'SELECT @t, CASE WHEN (@t+@R3)>=@R2 THEN @R2 ELSE @t+@R3 END '											+CHAR(13)
		SET @SQL = @SQL +   'SET @t = @t+@R3+1 '																					+CHAR(13)
		SET @SQL = @SQL +   'END'																							+CHAR(13)		

		INSERT INTO @Ids_Range (range_from, range_to) 
		EXEC(@SQL);

		/*
		Создать курсор и различные переменные, чтобы определить строку выполнения SQL для создания и управления 
		заданиями агента SQL сервера (точное число шагов на задание задается с помощью пременной @SQL_Exec_No). 
		Этот SQL разбивает и вставляйте задание в несколько шагов, которые работают в режиме asunchronous.
		Эта часть кода также управляет остановки и удаление уже созданных рабочих мест в случае повторного запуска, а также удаление
		Задания агента SQL, созданные в рамках этого процесса при успешном завершении
		*/
		--select CURSOR_STATUS('global','sp_cursor'), CURSOR_STATUS('local','sp_cursor')

		IF CURSOR_STATUS('local', 'sp_cursor') >= -1
			BEGIN
				DEALLOCATE sp_cursor
			END
			DECLARE @z INT
			DECLARE @err INT
			DECLARE sp_cursor CURSOR LOCAL
			FOR
			SELECT id FROM @ids_range
			SELECT  @err = @@error
			IF @err <> 0
				BEGIN
					DEALLOCATE sp_cursor
					--RETURN @err --нет возврата ошибки, т.к. это для хранимой процедуры
				END
			OPEN sp_cursor
			FETCH NEXT
			FROM sp_cursor INTO @z
			WHILE @@FETCH_STATUS = 0
				BEGIN
					DECLARE
					@range_from		VARCHAR(10)		= (SELECT CAST(range_from AS VARCHAR(10)) FROM @Ids_Range where id = @z),
					@range_to		VARCHAR(10)		= (SELECT CAST(range_to AS VARCHAR(10)) FROM @Ids_Range where id = @z)
					DECLARE
					@sql_name_step  VARCHAR(10) = (SELECT id FROM @ids_range WHERE id = @z)
					/*Динамически передаваемй параметр*/
					DECLARE @sql_command_while NVARCHAR(MAX)	= 
					'
					BEGIN TRAN;
					INSERT INTO [ETL_1C_SQL].[dbo].[Table_РегистрыСведений_м_СтатусыТоваров_СрезПоследних_TEMP]
					SELECT [Период]   ,[Регистратор_Наименование]      ,[Регистратор_Номер]      ,[Регистратор_Дата]
				,[НомерСтроки]      ,[Активность]      ,[Номенклатура_Ссылка]      ,[Подразделение_Ссылка]      ,[ПризнакНоменклатуры_Ссылка]
				,[Статус_Ссылка]      ,[ВременноеВыбытие_Ссылка]      ,[ЖизненныйЦикл_Ссылка]      ,[ЗначенияСвойствНоменклатуры_Ссылка]
				,[ПоДату]      ,[Комментарий]      ,[Маркетинговый]      ,[БазаДанных]      ,CONVERT(SMALLDATETIME,GETDATE()) AS ДатаАктуальности 
					FROM [10.19.34.100].[tempdb].[dbo].[Table_РегистрыСведений_м_СтатусыТоваров_СрезПоследних_33_TEMP]
					WHERE ID >= '+cast(@range_from as varchar (20))+' AND ID <= '+cast(@range_to as varchar(20))+';
					COMMIT TRAN;
					'
                      
					EXEC(@sql_command_while)

					FETCH NEXT
					FROM sp_cursor INTO @z
				END
				CLOSE sp_cursor 
				DEALLOCATE sp_cursor;

	/*сохраним данные о количестве строк*/
	DECLARE @Sql_Audit_Job_Row_Count VARCHAR (MAX)
	SET @Sql_Audit_Job_Row_Count = '
	SELECT 
	CONVERT(INT,REPLACE(RIGHT(RIGHT(job.Name,LEN(job.Name)-PATINDEX(''_'',job.Name)),2),''_'','''')) as [id_servers],
	4 as [id_reports],	
	job.Name as [job_name], 
	temp_tbl.[count_rows],
	DATEADD(ms,-DATEPART(ms,CURRENT_TIMESTAMP), CURRENT_TIMESTAMP) as [current_execution_date]
	FROM msdb.dbo.sysjobs_view job
	INNER JOIN msdb.dbo.sysjobactivity activity ON job.job_id = activity.job_id
	LEFT JOIN msdb.dbo.sysjobhistory history ON activity.job_history_id = history.instance_id
	CROSS APPLY (SELECT TOP 1 COUNT(*) [count_rows] FROM [10.19.34.100].[tempdb].[dbo].[Table_РегистрыСведений_м_СтатусыТоваров_СрезПоследних_33_TEMP]) AS temp_tbl
	WHERE job.name = ''РегистрыСведений_м_СтатусыТоваров_СрезПоследних_33'''

	/*INSERT INTO [ETL_1C_SQL].[dbo].[Table_Список_Результатов_Выполнения_Заданий_ETL_TEMP]*/
	INSERT INTO [ETL_1C_SQL].[dbo].[Table_Список_Результатов_Выполнения_Заданий_ETL_4_TEMP]
	EXEC(@Sql_Audit_Job_Row_Count)

	/*Удаление временной таблицы на удаленном сервере*/
	DECLARE @Sql_Drop_Temp_Table VARCHAR (MAX)
	SET @Sql_Drop_Temp_Table = 'EXEC [10.19.34.100].[trade].sys.sp_executesql N''
						IF OBJECT_ID(''''[tempdb].[dbo].[Table_РегистрыСведений_м_СтатусыТоваров_СрезПоследних_33_TEMP]'''') IS NOT NULL
						BEGIN
								DROP TABLE [tempdb].[dbo].[Table_РегистрыСведений_м_СтатусыТоваров_СрезПоследних_33_TEMP];
						END;'''
	EXEC (@Sql_Drop_Temp_Table);
	