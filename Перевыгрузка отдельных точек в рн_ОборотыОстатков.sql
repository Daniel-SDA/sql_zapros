use [ETL_1C_SQL];
/*УДАЛИТЬ ДАННЫЕ ВО ТАБЛИЦЕ ПРИЕМНИКЕ*/
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP];

/*ВЫКЛЮЧИТЬ ВСЕ ИНДЕКСЫ В ТАБЛИЦЕ ПРИЕМНИКЕ*/
ALTER INDEX ALL ON [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP] DISABLE;

EXEC sp_executesql 
N'
/*----предыдущий месяц------------------------*/
BEGIN TRAN;
DECLARE @DATE_START_ DATETIME
DECLARE @DATE_END_ DATETIME
SELECT 
@DATE_START_ = DATEADD(YEAR,+2000,CONVERT(DATETIME,DATEADD(month,-4, DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0)))),  
@DATE_END_ = DATEADD(YEAR,+2000,DATEADD(MILLISECOND,-003,DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0))))

/*ОБНОВЛЕНИЕ ДИАПАЗОНА ПЕРИОДА В СТРОКАХ ЗАПРОСА*/
UPDATE [ETL_1C_SQL].[dbo].[Table_Список_Запросов]
SET[SQL_Text] = REPLACE([SQL_Text], 
			SUBSTRING([SQL_Text],CHARINDEX(''@P23 numeric(10)'', [SQL_Text])+LEN(''@P23 numeric(10)'''',''),53),
			''{ts '''''' + CONVERT(VARCHAR,@DATE_START_, 120)+ ''''''},{ts '''''' + CONVERT(VARCHAR,@DATE_END_,120)+''''''}'')
FROM [ETL_1C_SQL].[dbo].[Table_Список_Запросов] AS Q
INNER JOIN [ETL_1C_SQL].[dbo].[Table_Список_Отчетов] AS R ON R.[id_reports]= Q.[id_reports]
WHERE R.[name_reports] = ''Выгрузка статистики оборотов по остаткам_закр''

DECLARE @DATE_START_2_ VARCHAR(23)
DECLARE @DATE_END_2_ VARCHAR(23)

SELECT @DATE_START_2_ = CONVERT(VARCHAR,DATEADD(YEAR,-2000,@DATE_START_),121), 
       @DATE_END_2_ = CONVERT(VARCHAR,DATEADD(YEAR,-2000,@DATE_END_),121)

/*Выгрузка данных*/
/*id_reports*/
DECLARE @id_reports_ as varchar(3) 
SET @id_reports_ = (SELECT DISTINCT [id_reports] 
		FROM [ETL_1C_SQL].[dbo].[Table_Список_Отчетов] 
		WHERE [name_reports] = ''Выгрузка статистики оборотов по остаткам_закр'')
/*Серверы и БД*/
/*Табличная переменная, создать список id серверов из таблицы и порядковый номер row_number*/
DECLARE @tbl_srv_ TABLE (id_row int, [id_servers] int)
INSERT INTO @tbl_srv_
SELECT DISTINCT 
ROW_NUMBER() OVER(ORDER BY [id_Linked_Servers] asc) AS id_row,
[id_Linked_Servers] AS [id_servers]
FROM  
[ETL_1C_SQL].[dbo].[Table_Список_Запросов] AS F,
[ETL_1C_SQL].[dbo].[Table_Список_Связанных_Серверов] AS G
WHERE 
F.[id_Linked_Servers] = G.[id_servers] AND  
G.[status] = 1 AND
F.[id_reports] = @id_reports_ 

/*строка отбора нужных серверов*/
 --and F.[id_Linked_Servers] in (41, 63, 16, 23, 54, 62) 

DECLARE @id_row_ varchar(3)
SET @id_row_ = (SELECT DISTINCT max(id_row) as id_row
               FROM @tbl_srv_)
WHILE @id_row_ > 0  
      BEGIN
/*SQL_Text*/
      DECLARE @id_server__ varchar(3)
      SET @id_server__ = (SELECT DISTINCT [id_servers] FROM @tbl_srv_ WHERE id_row = @id_row_)
      DECLARE @sql_insert_text_ varchar(max)
      SET @sql_insert_text_ = (select distinct REPLACE(REPLACE(d.[SQL_Text], '''''''', ''''''''''''),'',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0'','',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0'''''')
		             from [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d 
		             where d.[id_Linked_Servers] = @id_server__ and d.[id_reports] = @id_reports_)
/*Вывод текущего состояния выгрузки*/
print @id_server__
print @sql_insert_text_
		             
EXEC (''BEGIN TRAN;
EXEC sp_executesql N''''
INSERT INTO [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP] 
EXEC sp_executesql '' + @sql_insert_text_ + '';
COMMIT TRAN;'')
SET @id_row_ = @id_row_ - 1
END;
COMMIT TRAN;
'
/*ПЕРЕМЕЩЕНИЕ СЕКЦИИ*/
----1. Очистить и включить индексы в таблице мини-копии;
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP_PREV];
ALTER INDEX ALL ON [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP_PREV] REBUILD;

----2. Определить номера секций на основе выгруженных данных в таблице приемнике для перемещения из целевой таблицы в таблицу мини-копию 
----3. Переместить секции из целевой таблицы в таблицу мини-копию;
DECLARE @tbl_part TABLE ([id_row] int, [partition_number] int, [rows] int)
INSERT INTO @tbl_part
SELECT ROW_NUMBER() OVER(ORDER BY [partition_number] DESC) AS [id_row], [partition_number], [rows] 
FROM (SELECT DISTINCT [partition_number], [rows] FROM [ETL_1C_SQL].sys.partitions WITH(NOLOCK) 
	  WHERE object_id = object_id (N'рн_ОборотыОстатков_TEMP', 'U' ) AND ROWS > 0) AS T;

DECLARE @id_row VARCHAR(3)
SET @id_row = (SELECT DISTINCT MAX(id_row) AS id_row FROM @tbl_part)
  WHILE @id_row > 0
  BEGIN 
  DECLARE @query VARCHAR(4000)
  SET @query = (SELECT 'ALTER TABLE [dbo].[рн_ОборотыОстатков] SWITCH PARTITION ' + CONVERT(VARCHAR(3),[partition_number]) + ' TO [dbo].[рн_ОборотыОстатков_TEMP_PREV] PARTITION ' + CONVERT(VARCHAR(3),[partition_number])
			   FROM @tbl_part WHERE id_row = @id_row)
  EXEC (@query)
SET @id_row = @id_row - 1
END;

----4. Сопоставить таблицу мини-копию с промежуточной таблицей, добавив не достающие данные (проверка по полям [БазаДанных], CONVERT(VARCHAR(7),[Период],126));
INSERT INTO [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP]
SELECT * FROM [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP_PREV] AS T1
WHERE NOT EXISTS (SELECT * FROM [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP] AS T2 WHERE T1.[БазаДанных] = T2.[БазаДанных] AND CONVERT(VARCHAR(7),T1.[Период],126) = CONVERT(VARCHAR(7),T2.[Период],126));

  /*Обновление полей в таблице ОборотыОстатков*/
UPDATE [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP]
SET [КоличествоПриход] = 0
FROM [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP]
WHERE [КоличествоПриход] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP]
SET [КоличествоРасход] = 0
FROM [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP]
WHERE [КоличествоРасход] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP]
SET [КоличествоОборот] = 0
FROM [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP]
WHERE [КоличествоОборот] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP]
SET [СтоимостьПриход] = 0
FROM [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP]
WHERE [СтоимостьПриход] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP]
SET [СтоимостьРасход] = 0
FROM [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP]
WHERE [СтоимостьРасход] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP]
SET [СтоимостьОборот]= 0
FROM [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP]
WHERE [СтоимостьОборот] IS NULL;

----5. Включить индексы в промежуточной таблице;
ALTER INDEX ALL ON [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP] REBUILD;

----6. Переместить секции из промежуточной таблицы в целевую таблицу;
SET @id_row = (SELECT DISTINCT MAX(id_row) AS id_row FROM @tbl_part)
  WHILE @id_row > 0
  BEGIN 
  SET @query = (SELECT 'ALTER TABLE [dbo].[рн_ОборотыОстатков_TEMP] SWITCH PARTITION ' + CONVERT(VARCHAR(3),[partition_number]) + ' TO [dbo].[рн_ОборотыОстатков] PARTITION ' + CONVERT(VARCHAR(3),[partition_number])
			   FROM @tbl_part WHERE id_row = @id_row)
  EXEC (@query)
SET @id_row = @id_row - 1
END;

----7. Очистить таблицу мини-копию и промежуточную;
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP_PREV];
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков_TEMP];

----8. Обновить статистику для целевой таблицы;
UPDATE STATISTICS [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков];