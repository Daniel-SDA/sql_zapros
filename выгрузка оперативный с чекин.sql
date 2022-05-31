USE [ETL_1C_SQL];
/*Выгрузка статистики продаж по складам СПб_нов*/
/*ПЕРИОД ДЛЯ ВЫГРУЗКИ*/
DECLARE @DATE_START DATETIME
DECLARE @DATE_END DATETIME

--SELECT DATEADD=, 1/4, current_timestamp);
 
	select 	@DATE_START = cast (max([Дата_Актуальности]) as datetime) from [dbo].[рн_Продажи];
		select 	@DATE_START =dateadd(ww, -2,@DATE_START)  
	--	 DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0),  
	SELECT	@DATE_END = (CASE 
						WHEN DATEPART(mm,DATEADD(ss,-1,DATEADD(dd, DATEDIFF(dd,0,CURRENT_TIMESTAMP),0))) < DATEPART(mm, CURRENT_TIMESTAMP) 
						THEN DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0)
						ELSE DATEADD(ss,-1,DATEADD(dd, DATEDIFF(dd,0,CURRENT_TIMESTAMP),0))
					END);
					select @DATE_START, @DATE_END
/*ОБНОВЛЕНИЕ ПЕРИОДА В ТЕКСТЕ СКРИПТОВ*/
UPDATE [ETL_1C_SQL].[dbo].[Table_Список_Запросов]
SET [SQL_Text] = REPLACE([SQL_Text],
				SUBSTRING([SQL_Text],CHARINDEX('BETWEEN', [SQL_Text]),67),
				'BETWEEN '''''+CONVERT(VARCHAR,@DATE_START,121)+''''' AND '''''+CONVERT(VARCHAR,@DATE_END,121)+'''''')
FROM [ETL_1C_SQL].[dbo].[Table_Список_Запросов] AS Q WITH(NOLOCK)
INNER JOIN [ETL_1C_SQL].[dbo].[Table_Список_Отчетов] AS R WITH(NOLOCK) ON R.[id_reports]= Q.[id_reports]
WHERE R.[name_reports] = 'Выгрузка статистики продаж по складам СПб_нов' AND R.[id_reports] = 2 AND Q.[SQL_Text] like '%BETWEEN%';

/*УДАЛИТЬ ДАННЫЕ ВО ТАБЛИЦЕ ПРИЕМНИКЕ*/
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP];

/*ВЫКЛЮЧИТЬ ВСЕ ИНДЕКСЫ В ТАБЛИЦЕ ПРИЕМНИКЕ*/
ALTER INDEX ALL ON [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP] DISABLE;

/*ВЫГРУЗИТЬ ДАННЫЕ В ТАБЛИЦУ ПРИЕМНИК*/
--удалить временные таблицы
execute master.dbo.sp_executesql 
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##OwnOrg_'', N''U'') IS NOT NULL
          DROP TABLE ##OwnOrg_';
execute master.dbo.sp_executesql 
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##RegSal_'', N''U'') IS NOT NULL
          DROP TABLE ##RegSal_';
execute master.dbo.sp_executesql 
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##DocLzk_'', N''U'') IS NOT NULL
          DROP TABLE ##DocLzk_';
execute master.dbo.sp_executesql 
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##DocPer_'', N''U'') IS NOT NULL
          DROP TABLE ##DocPer_';
execute master.dbo.sp_executesql 
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##RoznSl_'', N''U'') IS NOT NULL
          DROP TABLE ##RoznSl_';
execute master.dbo.sp_executesql 
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##CheKKM_'', N''U'') IS NOT NULL
          DROP TABLE ##CheKKM_';
execute master.dbo.sp_executesql 
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##СборДанных_'', N''U'') IS NOT NULL
          DROP TABLE ##СборДанных_';
--создать временные таблицы
create table ##OwnOrg_
(
    [КонтрагентСсылка] [binary](16) NOT NULL,
	[Контрагент] [nvarchar](100) NULL,
	[ТипСправочника] [binary](4) NOT NULL,
	[Подразделение_Ссылка] [binary](16) NOT NULL,
	[Подразделение] [nvarchar](75) NULL,
	[БазаДанных] [INT] NOT NULL
);

create table ##RegSal_
(
	[ДокументПродажи_Ссылка] [binary](16) NOT NULL,
	[Номенклатура_Ссылка] [binary](16) NOT NULL,
	[Количество] [numeric](38, 3) NULL,
	[Стоимость] [numeric](38, 2) NULL,
	[СтоимостьБезСкидок] [numeric](38, 2) NULL,
	[СтоимостьВБазовыхММЦенах] [numeric](38, 2) NULL,
	[БазаДанных] [INT] NOT NULL
);

create table ##DocLzk_
(
	[м_ЛЗК_Ссылка] [binary](16) NOT NULL,
	[Номенклатура_Ссылка] [binary](16) NOT NULL,
	[Количество] [numeric](38, 3) NULL,
	[СуммаСНДС] [numeric](38, 2) NULL,
	[БазаДанных] [INT] NOT NULL
);

create table ##DocPer_
(
	[Перемещение_Ссылка] [binary](16) NOT NULL,
	[Номенклатура_Ссылка] [binary](16) NOT NULL,
	[Количество] [numeric](38, 3) NULL,
	[СуммаПеремещения] [numeric](38, 5) NULL,
	[БазаДанных] [INT] NOT NULL
);

create table ##RoznSl_
(
	[ОтчетОРозничныхПродажах_Ссылка] [binary](16) NOT NULL,
	[Номенклатура_Ссылка] [binary](16) NOT NULL,
	[Количество] [numeric](38, 3) NULL,
	[Сумма] [numeric](38, 3) NULL,
	[СуммаБезСкидок] [numeric](38, 3) NULL,
	[БазаДанных] [INT] NOT NULL
);

create table ##CheKKM_
(
	[ЧекККМ_Ссылка] [binary](16) NOT NULL,
	[Номенклатура_Ссылка] [binary](16) NOT NULL,
	[Количество] [numeric](38, 3) NULL,
	[Сумма] [numeric](38, 3) NULL,
	[СуммаБезСкидки] [numeric](38, 3) NULL,
	[БазаДанных] [INT] NOT NULL
);

create table ##СборДанных_
(
	[Период] [datetime] NULL,
	[Номенклатура_ссылка] [binary](16) NOT NULL,
	[НомерСтроки] [numeric] (9,0) NULL, 
	[Контрагент_ссылка] [binary](16) NULL,
	[Документ_Ссылка] [binary](16) NULL,
	[Ответственный_Ссылка] [binary](16) NULL,
	[Редактор_Ссылка] [binary](16) NULL,
	[ЛП_Отгрузки_Ссылка] [binary](16) NULL,
	[ЛП_Отгрузки_Тип_Ссылка] [binary](16) NULL,
	[ЛП_Получатель_Ссылка] [binary](16) NULL,
	[ЛП_Получатель_Тип_Ссылка] [binary](16) NULL,
	[Регистратор_Номер] [nchar](50) NULL,
	[Регистратор_Тип] [varchar](150) NULL,
	[Тип_Отгрузки_Ссылка] [binary](16) NULL,/*[varchar](50) NULL,*/
	[Количество] [numeric](38, 3) NULL,
	[СебестоимостьСНДС] [numeric](38, 3) NULL,
	[БазаДанных] [INT] NOT NULL
);

/*id_reports*/
declare @id_reports as int
		   set @id_reports = (SELECT distinct [id_reports] 
						      FROM [ETL_1C_SQL].[dbo].[Table_Список_Отчетов] 
							  where [name_reports] = 'Выгрузка статистики продаж по складам СПб_нов')
							  
declare  @serv_start datetime			
declare @serv_end datetime			  
declare @Sec INT
declare @errsqltext nvarchar(3000);
/*Серверы и БД*/
DECLARE @i_server INT 
SET @i_server =17/* (SELECT DISTINCT max([id_Linked_Servers])  
		FROM [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d
		INNER JOIN [ETL_1C_SQL].[dbo].[Table_Список_Связанных_Серверов] as t on t.id_servers = d.id_Linked_Servers 
		WHERE [id_reports] = @id_reports and status = 1		)
		*/
WHILE @i_server=17-- > 0
	 BEGIN
	 
	 set  @serv_start  = CURRENT_TIMESTAMP; 
	 
	set @errsqltext='';

select @Sec = DateDiff(s, @serv_start, @serv_end)
		DECLARE @id_server varchar(255)
		SET @id_server = (SELECT DISTINCT d.[id_Linked_Servers] 
				FROM [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d 
				INNER JOIN [ETL_1C_SQL].[dbo].[Table_Список_Связанных_Серверов] as t on t.id_servers = d.id_Linked_Servers 
				WHERE d.[id_Linked_Servers] = @i_server and status = 1) 
				
				
				
	 SET @i_server = @i_server - 1

/*SQL_Text*/
/*Объявляем перменные для курсора*/	 
DECLARE @IdReports INT
DECLARE @IdLinkedServer INT
DECLARE @N INT
DECLARE @SqlText VARCHAR (MAX)
/*Объявляем курсор*/
DECLARE @CURSOR CURSOR
/*Заполняем курсор*/
SET @CURSOR  = CURSOR SCROLL
	FOR SELECT [id_reports], [id_Linked_Servers], [N], [SQL_Text]
		FROM [ETL_1C_SQL].[dbo].[Table_Список_Запросов]
		WHERE [id_Linked_Servers] = @id_server and [id_reports] = @id_reports
		ORDER BY [N] ASC
/*Открываем курсор*/
OPEN @CURSOR
/*Выбираем первую строку*/
FETCH NEXT FROM @CURSOR INTO @IdReports, @IdLinkedServer, @N, @SqlText

/*Выполняем в цикле перебор строк*/
	WHILE @@FETCH_STATUS = 0
	    BEGIN
	  DECLARE @sql_text varchar(max)
	  
	  set @errsqltext= cast ( @serv_start as nvarchar(30))+' сервер '+cast(@IdLinkedServer as nvarchar(10))+ ' шаг '+cast (@N as nvarchar(10))+ ' '+@SqlText
	  SET @sql_text = (select distinct ' BEGIN TRAN; ' + REPLACE(d.[SQL_Text],'рн_Продажи','рн_Продажи_TEMP') + ' COMMIT TRAN; '
					   from [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d 
					   where d.[id_Linked_Servers] = convert(varchar(3),@IdLinkedServer) and 
					         d.[N] = convert(varchar(3),@N) and
							 d.[id_reports] = convert(varchar(3),@IdReports))
begin try	
 print N'выполняется обработка - '+ @errsqltext						 
      EXEC (@sql_text)
	  
end try

BEGIN CATCH
       DECLARE @ErrorMessage NVARCHAR(4000);  
    DECLARE @ErrorSeverity INT;  
    DECLARE @ErrorState INT;  
  
    SELECT   
        @ErrorMessage = ERROR_MESSAGE() + ' ' +@errsqltext,  
        @ErrorSeverity = ERROR_SEVERITY(),  
        @ErrorState = ERROR_STATE();  
  
    -- Use RAISERROR inside the CATCH block to return error  
    -- information about the original error that caused  
    -- execution to jump to the CATCH block.  
    RAISERROR (@ErrorMessage, -- Message text.  
               @ErrorSeverity, -- Severity.  
               @ErrorState -- State.  
               );  
END CATCH

	
/*Выбираем следующую строку*/
		FETCH NEXT FROM @CURSOR INTO @IdReports, @IdLinkedServer, @N, @SqlText
	END
CLOSE @CURSOR

TRUNCATE TABLE ##OwnOrg_
TRUNCATE TABLE ##RegSal_
TRUNCATE TABLE ##DocLzk_
TRUNCATE TABLE ##DocPer_
TRUNCATE TABLE ##RoznSl_
TRUNCATE TABLE ##CheKKM_
--TRUNCATE TABLE ##СборДанных_
   set  @serv_end  = CURRENT_TIMESTAMP; 
   
   set @sec=datediff(ss,@serv_start,@serv_end)
   if (@sec>1200)
   begin
   set @errsqltext =@errsqltext  +' sec = '+cast(@sec as nvarchar(10))
	EXEC msdb.dbo.sp_send_dbmail   @profile_name = 'ANALITIK_SQL', 
    @recipients ='sderjikov@minimaks.ru',
		@body =  @errsqltext,  
		@subject = 'обработка продажи оперативный период. затянулась' ;  
		print N'ВНИМАНИЕ'+ cast(@sec as nvarchar(10))+' sec- '+@errsqltext

   end
  
   
END;

--удалить временные таблицы
execute master.dbo.sp_executesql 
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##OwnOrg_'', N''U'') IS NOT NULL
          DROP TABLE ##OwnOrg_';
execute master.dbo.sp_executesql 
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##RegSal_'', N''U'') IS NOT NULL
          DROP TABLE ##RegSal_';
execute master.dbo.sp_executesql 
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##DocLzk_'', N''U'') IS NOT NULL
          DROP TABLE ##DocLzk_';
execute master.dbo.sp_executesql 
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##DocPer_'', N''U'') IS NOT NULL
          DROP TABLE ##DocPer_';
execute master.dbo.sp_executesql 
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##RoznSl_'', N''U'') IS NOT NULL
          DROP TABLE ##RoznSl_';
execute master.dbo.sp_executesql 
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##CheKKM_'', N''U'') IS NOT NULL
          DROP TABLE ##CheKKM_';
/*
execute master.dbo.sp_executesql 
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##СборДанных_'', N''U'') IS NOT NULL
          DROP TABLE ##СборДанных_';

/*ПЕРЕМЕЩЕНИЕ СЕКЦИИ*/
----1. Очистить и включить индексы в таблице мини-копии;
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP_PREV];
ALTER INDEX ALL ON [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP_PREV] REBUILD;

----2. Определить номера секций на основе выгруженных данных в таблице приемнике для перемещения из целевой таблицы в таблицу мини-копию 
----3. Переместить секции из целевой таблицы в таблицу мини-копию;
DECLARE @tbl_part TABLE ([id_row] int, [partition_number] int, [rows] int)
INSERT INTO @tbl_part
SELECT ROW_NUMBER() OVER(ORDER BY [partition_number] DESC) AS [id_row], [partition_number], [rows] 
FROM (SELECT DISTINCT [partition_number], [rows] FROM sys.partitions WITH(NOLOCK) 
	  WHERE object_id = object_id (N'рн_Продажи_TEMP', 'U' ) AND ROWS > 0) AS T;

DECLARE @id_row VARCHAR(3)
SET @id_row = (SELECT DISTINCT MAX(id_row) AS id_row FROM @tbl_part)
  WHILE @id_row > 0
  BEGIN 
  DECLARE @query VARCHAR(4000)
  SET @query = (SELECT 'ALTER TABLE [dbo].[рн_Продажи] SWITCH PARTITION ' + CONVERT(VARCHAR(3),[partition_number]) + ' TO [dbo].[рн_Продажи_TEMP_PREV] PARTITION ' + CONVERT(VARCHAR(3),[partition_number])
			   FROM @tbl_part WHERE id_row = @id_row)
  EXEC (@query)
SET @id_row = @id_row - 1
END;

----4. Сопоставить таблицу мини-копию с промежуточной таблицей, добавив не достающие данные (проверка по полям [БазаДанных], CONVERT(VARCHAR(7),[Период],126));
INSERT INTO [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
SELECT * FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP_PREV] AS T1
WHERE NOT EXISTS (SELECT * FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP] AS T2 
				  WHERE 
				  CONVERT(VARCHAR(7),T1.[Период],126) = CONVERT(VARCHAR(7),T2.[Период],126) AND
				  T1.[БазаДанных] = T2.[БазаДанных] AND 
				  T1.[Артикул] = T2.[Артикул] AND 
				  T1.[Регистратор_Номер] = T2.[Регистратор_Номер]);

/*Обновление полей в таблице Продаж*/
UPDATE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
SET [Документ_Ссылка] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
WHERE [Документ_Ссылка] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
SET [Контрагент_ссылка] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
WHERE [Контрагент_ссылка] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
SET [Ответственный_Ссылка] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
WHERE [Ответственный_Ссылка] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
SET [Редактор_Ссылка] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
WHERE [Редактор_Ссылка] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
SET [ЛП_Отгрузки_Ссылка] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
WHERE [ЛП_Отгрузки_Ссылка] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
SET [ЛП_Отгрузки_Тип_Ссылка] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
WHERE [ЛП_Отгрузки_Тип_Ссылка] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
SET [ЛП_Получатель_Ссылка] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
WHERE [ЛП_Получатель_Ссылка] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
SET [ЛП_Получатель_Тип_Ссылка] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
WHERE [ЛП_Получатель_Тип_Ссылка] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
SET [Тип_Отгрузки_Ссылка] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
WHERE [Тип_Отгрузки_Ссылка] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
SET [СтоимостьБезСкидок] = 0
FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
WHERE [СтоимостьБезСкидок] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
SET [СтоимостьВБазовыхММЦенах] = 0
FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
WHERE [СтоимостьВБазовыхММЦенах] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
SET [СебестоимостьСНДС] = 0
FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP]
WHERE [СебестоимостьСНДС] IS NULL;

----5. Включить индексы в промежуточной таблице;
ALTER INDEX ALL ON [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP] REBUILD;

----6. Переместить секции из промежуточной таблицы в целевую таблицу;
SET @id_row = (SELECT DISTINCT MAX(id_row) AS id_row FROM @tbl_part)
  WHILE @id_row > 0
  BEGIN 
  SET @query = (SELECT 'ALTER TABLE [dbo].[рн_Продажи_TEMP] SWITCH PARTITION ' + CONVERT(VARCHAR(3),[partition_number]) + ' TO [dbo].[рн_Продажи] PARTITION ' + CONVERT(VARCHAR(3),[partition_number])
			   FROM @tbl_part WHERE id_row = @id_row)
  EXEC (@query)
SET @id_row = @id_row - 1
END;

----7. Очистить таблицу мини-копию и промежуточную;
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP_PREV];
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP];

----8. Обновить статистику для целевой таблицы;
UPDATE STATISTICS [ETL_1C_SQL].[dbo].[рн_Продажи];

*/

select  */* [Период]
    --  ,[Артикул]
      ,[Номенклатура_ссылка]
      ,[НомерСтроки]
      ,[Документ_Ссылка]
     
      ,[Стоимость]*/
  FROM ##СборДанных_
 where convert(date,[Период])>'2021-07-18' and [БазаДанных]=17

-- order by 5,2


/*
declare @x varchar(max)
declare @y varchar(max)

select @x = replicate(cast('a' as varchar(max)),10000)
select @y = 'b'

set @y = replicate(@y,10000)

select len(@x), len(@y), len(@x + @y)

*/

USE [master]
GO
EXEC master.dbo.sp_serveroption @server=N'10.11.0.27', @optname=N'remote proc transaction promotion', @optvalue=N'false'
GO


truncate table ##СборДанных1_
insert into ##СборДанных1_


 exec  [10.11.0.27].Smr_trade.dbo.sp_executesql @SQL=N'CREATE TABLE ##tmpeieaz1 (ID INT); select * from [##СборДанных_] '

 /*

  exec sp_configure 'remote proc trans','0';  
RECONFIGURE WITH OVERRIDE;*/