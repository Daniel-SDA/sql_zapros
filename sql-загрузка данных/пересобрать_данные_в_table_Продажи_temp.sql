USE [ETL_1C_SQL];
--select * into #рн_продажи from [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP] 
--select * from #рн_продажи
/*Выгрузка статистики продаж по складам СПб_нов*/
/*ПЕРИОД ДЛЯ ВЫГРУЗКИ*/
DECLARE @DATE_START DATETIME

DECLARE @DATE_END DATETIME
 
		/*@DATE_START = DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0),  
		@DATE_END = (CASE 
						WHEN DATEPART(mm,DATEADD(ss,-1,DATEADD(dd, DATEDIFF(dd,0,CURRENT_TIMESTAMP),0))) < DATEPART(mm, CURRENT_TIMESTAMP) 
						THEN DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0)
						ELSE DATEADD(ss,-1,DATEADD(dd, DATEDIFF(dd,0,CURRENT_TIMESTAMP),0))
					END);*/

					set @DATE_START=convert(date,'2021-06-30 00:00')
	            	set @DATE_END = convert(date,'2021-07-01 00:00:00')
					
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
/*Серверы и БД*/
DECLARE @i_server INT 
SET @i_server = (SELECT DISTINCT max([id_Linked_Servers])  
		FROM [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d
		INNER JOIN [ETL_1C_SQL].[dbo].[Table_Список_Связанных_Серверов] as t on t.id_servers = d.id_Linked_Servers 
		WHERE [id_reports] = @id_reports and status = 1)
WHILE @i_server > 0
	 BEGIN
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
	  SET @sql_text = (select distinct ' BEGIN TRAN; ' + REPLACE(d.[SQL_Text],'рн_Продажи','рн_Продажи_TEMP') + ' COMMIT TRAN; '
					   from [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d 
					   where d.[id_Linked_Servers] = convert(varchar(3),@IdLinkedServer) and 
					         d.[N] = convert(varchar(3),@N) and
							 d.[id_reports] = convert(varchar(3),@IdReports))
							 print @sql_text
      EXEC (@sql_text)
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
TRUNCATE TABLE ##СборДанных_

END;