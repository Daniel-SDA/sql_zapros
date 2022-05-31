/*УДАЛИТЬ ДАННЫЕ ВО ТАБЛИЦЕ ПРИЕМНИКЕ*/
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам_TEMP];

/*ВЫКЛЮЧИТЬ ВСЕ ИНДЕКСЫ В ТАБЛИЦЕ ПРИЕМНИКЕ*/
ALTER INDEX ALL ON [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам_TEMP] DISABLE;

/*Ежедневное обновление таблицы ОстаткиПоМесяцам.*/
/*Правило №1: с 1 по 10 число запускается выгрузка данных из регистра сведений Товары на складах на первое число предыдущего и на первое число текущего месяца*/
/*Правило №2: с 10 числа запускается выгрузка данных из регистра сведений Партии Товаров на складах на первое число предыдущего месяца и из регистра сведений Товары на складах на первое число текущего месяца*/
IF ((DATEPART(day,CURRENT_TIMESTAMP)) >= 1 and (DATEPART(day,CURRENT_TIMESTAMP) < 10))
/*Правило №1*/
EXEC sp_executesql 
N'
/*Дата*/
DECLARE @DT VARCHAR(7)
SET @DT = (SELECT CONVERT(VARCHAR(7),DATEADD(MM,-1,CURRENT_TIMESTAMP),126))

DECLARE @DT_ VARCHAR(7)
SET @DT_ = (SELECT CONVERT(VARCHAR(7),DATEADD(MM,0,CURRENT_TIMESTAMP),126))

/*Название отчета*/
DECLARE @id_reports as varchar(3) 
SET @id_reports = (SELECT DISTINCT [id_reports] 
		FROM [ETL_1C_SQL].[dbo].[Table_Список_Отчетов] WITH(NOLOCK)
		WHERE [name_reports] = ''Выгрузка среза данных остатки по товарам на складах на 1 число месяца'')
		
/*Серверы и БД*/
DECLARE @i_server INT 
SET @i_server = (SELECT DISTINCT max([id_Linked_Servers])  
				 FROM [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d WITH(NOLOCK)
				 INNER JOIN [ETL_1C_SQL].[dbo].[Table_Список_Связанных_Серверов] as t WITH(NOLOCK) on t.id_servers = d.id_Linked_Servers 
				 WHERE status = 1 AND [id_reports] = @id_reports)
WHILE @i_server > 0
	 BEGIN
		DECLARE @id_server varchar(3)
		SET @id_server = (SELECT DISTINCT [id_Linked_Servers]  
				 FROM [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d WITH(NOLOCK)
				 INNER JOIN [ETL_1C_SQL].[dbo].[Table_Список_Связанных_Серверов] as t WITH(NOLOCK) on t.id_servers = d.id_Linked_Servers 
				 WHERE status = 1 AND [id_reports] = @id_reports AND  D.id_Linked_Servers = @i_server)
		DECLARE @DT_START VARCHAR(7)
		DECLARE @DT_END VARCHAR(7)
		SET @DT_START =  @DT
		SET @DT_END =  @DT_
		DECLARE @TEXT VARCHAR(MAX)
		SET @TEXT = 
(''
SELECT T.DT_WHERE,T.DATE_MOUNTH,T.ROW_NUM, ''+@id_server+'' as ID_BD, ''+@id_reports+'' as id_reports
FROM
(SELECT 
ROW_NUMBER() OVER(ORDER BY MAX(DATEADD(YEAR,+2000,DATEADD(ss,-1,DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,[Дата]),0))))) DESC) AS ROW_NUM,
MAX(DATEADD(YEAR,+2000,DATEADD(ss,-1,DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,[Дата]),0))))) AS DT_WHERE,
DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,[Дата]),0)) AS DATE_MOUNTH
FROM [ETL_1C_SQL].[dbo].[Table_Календарь] WITH(NOLOCK)
WHERE CONVERT(VARCHAR(7),[Дата],121) BETWEEN ''''''+@DT_START+'''''' AND ''''''+@DT_END+''''''
GROUP BY DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,[Дата]),0))) AS T
'')
DECLARE @TABL_DATE TABLE (DT_WHERE DATETIME,DATE_MOUNTH DATETIME,ROW_NUM INT,ID_BD INT, ID_REPORTS INT)
INSERT INTO @TABL_DATE EXEC (@TEXT)
SET @i_server = @i_server - 1

/*SQL_Text*/
/*Объявляем перменные для курсора*/	 
DECLARE @DT_WHERE DATETIME
DECLARE @DATE_MOUNTH VARCHAR(23)
DECLARE @ID_BD VARCHAR(3)
DECLARE @ID_REPORT INT
DECLARE @ROW_NUM INT
/*Объявляем курсор*/
DECLARE @CURSOR CURSOR
/*Заполняем курсор*/
SET @CURSOR = CURSOR SCROLL
	FOR SELECT DT_WHERE,CONVERT(VARCHAR,DATE_MOUNTH,121),ID_BD,ID_REPORTS,ROW_NUM
		FROM @TABL_DATE
		WHERE ID_BD = @id_server AND ID_REPORTS = @id_reports
		ORDER BY ROW_NUM DESC
/*Открываем курсор*/
OPEN @CURSOR
/*Выбираем первую строку*/
FETCH NEXT FROM @CURSOR INTO @DT_WHERE,@DATE_MOUNTH,@ID_BD,@ID_REPORT,@ROW_NUM
/*Выполняем в цикле перебор строк*/
	WHILE @@FETCH_STATUS = 0
	    BEGIN
	  DECLARE @sql_text varchar(max)
	  SET @sql_text = 
	  (select distinct '' BEGIN TRAN; 
	  '' + 
REPLACE(REPLACE(REPLACE(d.[SQL_Text],
''DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0)) AS Период'',
''{ts '''''''''''''''''' +CONVERT(VARCHAR,@DATE_MOUNTH,121) +''''''''''''''''''} AS Период'' ),
''_Period <= DATEADD(YEAR,+2000,DATEADD(ss,-1,DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0))))'',
''_Period <= {ts '''''''''''''''''' + CONVERT(VARCHAR,@DT_WHERE,121) +''''''''''''''''''}''),
''
,СерияНоменклатуры_Ссылка'','''') + ''; 
COMMIT TRAN; ''
from [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d WITH(NOLOCK)
where d.[id_Linked_Servers] = convert(varchar(3),@ID_BD) and d.[id_reports] = convert(varchar(3),@ID_REPORT))
 
 EXEC (''BEGIN TRAN;
 EXEC sp_executesql N''''
 INSERT INTO [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам_TEMP]
 EXEC sp_executesql N'''''''''' + @sql_text + '''''''''''''';
 COMMIT TRAN;'')
/*Выбираем следующую строку*/
		FETCH NEXT FROM @CURSOR INTO @DT_WHERE,@DATE_MOUNTH,@ID_BD,@ID_REPORT,@ROW_NUM
	END
CLOSE @CURSOR
END'

ELSE 

/*Правило №2*/
EXEC sp_executesql 
N'
/*--------на первое число предыдущего месяца--------*/
/*Дата*/
DECLARE @DT_ VARCHAR(7)
SET @DT_ = (SELECT CONVERT(VARCHAR(7),DATEADD(MM,-1,CURRENT_TIMESTAMP),126))

/*Название отчета*/
DECLARE @id_reports_ as varchar(3) 
SET @id_reports_ = (SELECT DISTINCT [id_reports] 
		FROM [ETL_1C_SQL].[dbo].[Table_Список_Отчетов] WITH(NOLOCK)
		WHERE [name_reports] = ''Выгрузка среза данных остатки по партиям товаров на складах'')

/*Серверы и БД*/
DECLARE @i_server_ INT 
SET @i_server_ = (SELECT DISTINCT max([id_Linked_Servers])  
				 FROM [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d WITH(NOLOCK)
				 INNER JOIN [ETL_1C_SQL].[dbo].[Table_Список_Связанных_Серверов] as t WITH(NOLOCK) on t.id_servers = d.id_Linked_Servers 
				 WHERE status = 1 AND [id_reports] = @id_reports_)
WHILE @i_server_ > 0
	 BEGIN
		DECLARE @id_server_ varchar(3)
		SET @id_server_ = (SELECT DISTINCT [id_Linked_Servers]  
				 FROM [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d WITH(NOLOCK)
				 INNER JOIN [ETL_1C_SQL].[dbo].[Table_Список_Связанных_Серверов] as t WITH(NOLOCK) on t.id_servers = d.id_Linked_Servers 
				 WHERE status = 1 AND [id_reports] = @id_reports_ AND  D.id_Linked_Servers = @i_server_)
		DECLARE @DT_START_ VARCHAR(7)
		DECLARE @DT_END_ VARCHAR(7)
		SET @DT_START_ =  @DT_
		SET @DT_END_ =  @DT_
		DECLARE @TEXT_ VARCHAR(MAX)
		SET @TEXT_ = 
(''
SELECT T.DT_WHERE,T.DATE_MOUNTH,T.ROW_NUM, ''+@id_server_+'' as ID_BD, ''+@id_reports_+'' as id_reports
FROM
(SELECT 
ROW_NUMBER() OVER(ORDER BY MAX(DATEADD(YEAR,+2000,DATEADD(ss,-1,DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,[Дата]),0))))) DESC) AS ROW_NUM,
MAX(DATEADD(YEAR,+2000,DATEADD(ss,-1,DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,[Дата]),0))))) AS DT_WHERE,
DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,[Дата]),0)) AS DATE_MOUNTH
FROM [ETL_1C_SQL].[dbo].[Table_Календарь] WITH(NOLOCK)
WHERE CONVERT(VARCHAR(7),[Дата],121) BETWEEN ''''''+@DT_START_+'''''' AND ''''''+@DT_END_+''''''
GROUP BY DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,[Дата]),0))) AS T
'')
DECLARE @TABL_DATE_ TABLE (DT_WHERE DATETIME,DATE_MOUNTH DATETIME,ROW_NUM INT,ID_BD INT, ID_REPORTS INT)
INSERT INTO @TABL_DATE_ EXEC (@TEXT_)
SET @i_server_ = @i_server_ - 1

/*SQL_Text*/
/*Объявляем перменные для курсора*/	 
DECLARE @DT_WHERE_ DATETIME
DECLARE @DATE_MOUNTH_ VARCHAR(23)
DECLARE @ID_BD_ VARCHAR(3)
DECLARE @ID_REPORT_ INT
DECLARE @ROW_NUM_ INT
/*Объявляем курсор*/
DECLARE @CURSOR_ CURSOR
/*Заполняем курсор*/
SET @CURSOR_ = CURSOR SCROLL
	FOR SELECT DT_WHERE,CONVERT(VARCHAR,DATE_MOUNTH,121),ID_BD,ID_REPORTS,ROW_NUM
		FROM @TABL_DATE_
		WHERE ID_BD = @id_server_ AND ID_REPORTS = @id_reports_
		ORDER BY ROW_NUM DESC
/*Открываем курсор*/
OPEN @CURSOR_
/*Выбираем первую строку*/
FETCH NEXT FROM @CURSOR_ INTO @DT_WHERE_,@DATE_MOUNTH_,@ID_BD_,@ID_REPORT_,@ROW_NUM_
/*Выполняем в цикле перебор строк*/
	WHILE @@FETCH_STATUS = 0
	    BEGIN
	  DECLARE @sql_text_ varchar(max)
	  SET @sql_text_ = 
	  (select distinct '' BEGIN TRAN; 
	  '' + 
REPLACE(REPLACE(REPLACE(d.[SQL_Text],
'',DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0)) as Период '',
'',{ts '''''''''''''''''' + CONVERT(VARCHAR,@DATE_MOUNTH_,121) + ''''''''''''''''''} as Период ''),
''_Period <= DATEADD(YEAR,+2000,DATEADD(ss,-1,DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0))))'',
''_Period <= {ts '''''''''''''''''' + CONVERT(VARCHAR,@DT_WHERE_,121) + ''''''''''''''''''}''),
''
,СерияНоменклатуры_Ссылка'','''') + ''; 
COMMIT TRAN; ''
from [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d WITH(NOLOCK)
where d.[id_Linked_Servers] = convert(varchar(3),@ID_BD_) and 
							 d.[id_reports] = convert(varchar(3),@ID_REPORT_))
 EXEC (''BEGIN TRAN;
 EXEC sp_executesql N''''
 INSERT INTO [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам_TEMP]
 EXEC sp_executesql N'''''''''' + @sql_text_ + '''''''''''''';
 COMMIT TRAN;'')
/*Выбираем следующую строку*/
		FETCH NEXT FROM @CURSOR_ INTO @DT_WHERE_,@DATE_MOUNTH_,@ID_BD_,@ID_REPORT_,@ROW_NUM_
	END
CLOSE @CURSOR_
END;

/*--------на первое число текущего месяца--------*/
/*Дата*/
DECLARE @DT VARCHAR(7)
SET @DT = (SELECT CONVERT(VARCHAR(7),DATEADD(MM,0,CURRENT_TIMESTAMP),126))

/*Название отчета*/
DECLARE @id_reports as varchar(3) 
SET @id_reports = (SELECT DISTINCT [id_reports] 
		FROM [ETL_1C_SQL].[dbo].[Table_Список_Отчетов] WITH(NOLOCK)
		WHERE [name_reports] = ''Выгрузка среза данных остатки по товарам на складах на 1 число месяца'')
		
/*Серверы и БД*/
DECLARE @i_server INT 
SET @i_server = (SELECT DISTINCT max([id_Linked_Servers])  
				 FROM [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d WITH(NOLOCK)
				 INNER JOIN [ETL_1C_SQL].[dbo].[Table_Список_Связанных_Серверов] as t WITH(NOLOCK) on t.id_servers = d.id_Linked_Servers 
				 WHERE status = 1 AND [id_reports] = @id_reports)
WHILE @i_server > 0
	 BEGIN
		DECLARE @id_server varchar(3)
		SET @id_server = (SELECT DISTINCT [id_Linked_Servers]  
				 FROM [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d WITH(NOLOCK)
				 INNER JOIN [ETL_1C_SQL].[dbo].[Table_Список_Связанных_Серверов] as t WITH(NOLOCK) on t.id_servers = d.id_Linked_Servers 
				 WHERE status = 1 AND [id_reports] = @id_reports AND  D.id_Linked_Servers = @i_server)
		DECLARE @DT_START VARCHAR(7)
		DECLARE @DT_END VARCHAR(7)
		SET @DT_START =  @DT
		SET @DT_END =  @DT
		DECLARE @TEXT VARCHAR(MAX)
		SET @TEXT = 
(''
SELECT T.DT_WHERE,T.DATE_MOUNTH,T.ROW_NUM, ''+@id_server+'' as ID_BD, ''+@id_reports+'' as id_reports
FROM
(SELECT 
ROW_NUMBER() OVER(ORDER BY MAX(DATEADD(YEAR,+2000,DATEADD(ss,-1,DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,[Дата]),0))))) DESC) AS ROW_NUM,
MAX(DATEADD(YEAR,+2000,DATEADD(ss,-1,DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,[Дата]),0))))) AS DT_WHERE,
DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,[Дата]),0)) AS DATE_MOUNTH
FROM [ETL_1C_SQL].[dbo].[Table_Календарь] WITH(NOLOCK)
WHERE CONVERT(VARCHAR(7),[Дата],121) BETWEEN ''''''+@DT_START+'''''' AND ''''''+@DT_END+''''''
GROUP BY DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,[Дата]),0))) AS T
'')
DECLARE @TABL_DATE TABLE (DT_WHERE DATETIME,DATE_MOUNTH DATETIME,ROW_NUM INT,ID_BD INT, ID_REPORTS INT)
INSERT INTO @TABL_DATE EXEC (@TEXT)
SET @i_server = @i_server - 1

/*SQL_Text*/
/*Объявляем перменные для курсора*/	 
DECLARE @DT_WHERE DATETIME
DECLARE @DATE_MOUNTH VARCHAR(23)
DECLARE @ID_BD VARCHAR(3)
DECLARE @ID_REPORT INT
DECLARE @ROW_NUM INT
/*Объявляем курсор*/
DECLARE @CURSOR CURSOR
/*Заполняем курсор*/
SET @CURSOR = CURSOR SCROLL
	FOR SELECT DT_WHERE,CONVERT(VARCHAR,DATE_MOUNTH,121),ID_BD,ID_REPORTS,ROW_NUM
		FROM @TABL_DATE
		WHERE ID_BD = @id_server AND ID_REPORTS = @id_reports
		ORDER BY ROW_NUM DESC
/*Открываем курсор*/
OPEN @CURSOR
/*Выбираем первую строку*/
FETCH NEXT FROM @CURSOR INTO @DT_WHERE,@DATE_MOUNTH,@ID_BD,@ID_REPORT,@ROW_NUM
/*Выполняем в цикле перебор строк*/
	WHILE @@FETCH_STATUS = 0
	    BEGIN
	  DECLARE @sql_text varchar(max)
	  SET @sql_text = 
	  (select distinct '' BEGIN TRAN; 
	  '' + 
REPLACE(REPLACE(REPLACE(d.[SQL_Text],
''DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0)) AS Период'',
''{ts '''''''''''''''''' +CONVERT(VARCHAR,@DATE_MOUNTH,121) +''''''''''''''''''} AS Период'' ),
''_Period <= DATEADD(YEAR,+2000,DATEADD(ss,-1,DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0))))'',
''_Period <= {ts '''''''''''''''''' + CONVERT(VARCHAR,@DT_WHERE,121) +''''''''''''''''''}''),
''
,СерияНоменклатуры_Ссылка'','''') + ''; 
COMMIT TRAN; ''
from [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d WITH(NOLOCK)
where d.[id_Linked_Servers] = convert(varchar(3),@ID_BD) and 
							 d.[id_reports] = convert(varchar(3),@ID_REPORT))
 
 EXEC (''BEGIN TRAN;
 EXEC sp_executesql N''''
 INSERT INTO [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам_TEMP]
 EXEC sp_executesql N'''''''''' + @sql_text + '''''''''''''';
 COMMIT TRAN;'')
/*Выбираем следующую строку*/
		FETCH NEXT FROM @CURSOR INTO @DT_WHERE,@DATE_MOUNTH,@ID_BD,@ID_REPORT,@ROW_NUM
	END
CLOSE @CURSOR
END';

/*ПЕРЕМЕЩЕНИЕ СЕКЦИИ*/
----1. Очистить и включить индексы в таблице мини-копии;
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам_TEMP_PREV];
ALTER INDEX ALL ON [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам_TEMP_PREV] REBUILD;

----2. Определить номера секций на основе выгруженных данных в таблице приемнике для перемещения из целевой таблицы в таблицу мини-копию 
----3. Переместить секции из целевой таблицы в таблицу мини-копию;
DECLARE @tbl_part TABLE ([id_row] int, [partition_number] int, [rows] int)
INSERT INTO @tbl_part
SELECT ROW_NUMBER() OVER(ORDER BY [partition_number] DESC) AS [id_row], [partition_number], [rows] 
FROM (SELECT DISTINCT [partition_number], [rows] FROM sys.partitions WITH(NOLOCK) 
	  WHERE object_id = object_id (N'рн_Остатки_По_Месяцам_TEMP', 'U' ) AND ROWS > 0) AS T;

DECLARE @id_row VARCHAR(3)
SET @id_row = (SELECT DISTINCT MAX(id_row) AS id_row FROM @tbl_part)
  WHILE @id_row > 0
  BEGIN 
  DECLARE @query VARCHAR(4000)
  SET @query = (SELECT 'ALTER TABLE [dbo].[рн_Остатки_По_Месяцам] SWITCH PARTITION ' + CONVERT(VARCHAR(3),[partition_number]) + ' TO [dbo].[рн_Остатки_По_Месяцам_TEMP_PREV] PARTITION ' + CONVERT(VARCHAR(3),[partition_number])
			   FROM @tbl_part WHERE id_row = @id_row)
  EXEC (@query)
SET @id_row = @id_row - 1
END;

----4. Сопоставить таблицу мини-копию с промежуточной таблицей, добавив не достающие данные (проверка по полям [БазаДанных], CONVERT(VARCHAR(7),[Период],126));
INSERT INTO [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам_TEMP]
SELECT * FROM [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам_TEMP_PREV] AS T1
WHERE NOT EXISTS (SELECT * FROM [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам_TEMP] AS T2 WHERE T1.[БазаДанных] = T2.[БазаДанных] AND CONVERT(VARCHAR(7),T1.[Период],126) = CONVERT(VARCHAR(7),T2.[Период],126));

----5. Включить индексы в промежуточной таблицей;
ALTER INDEX ALL ON [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам_TEMP] REBUILD;

----6. Переместить секции из промежуточной таблицы в целевую таблицу;
SET @id_row = (SELECT DISTINCT MAX(id_row) AS id_row FROM @tbl_part)
  WHILE @id_row > 0
  BEGIN 
  SET @query = (SELECT 'ALTER TABLE [dbo].[рн_Остатки_По_Месяцам_TEMP] SWITCH PARTITION ' + CONVERT(VARCHAR(3),[partition_number]) + ' TO [dbo].[рн_Остатки_По_Месяцам] PARTITION ' + CONVERT(VARCHAR(3),[partition_number])
			   FROM @tbl_part WHERE id_row = @id_row)
  EXEC (@query)
SET @id_row = @id_row - 1
END;

----7. Очистить таблицу мини-копии и промежуточную;
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам_TEMP_PREV];
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам_TEMP];

----8. Обновить статистику для целевой таблицы;
UPDATE STATISTICS [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам];