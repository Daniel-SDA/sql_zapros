use [ETL_1C_SQL];
/*������� ������ �� ������� ���������*/
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[��_���������������_TEMP];

/*��������� ��� ������� � ������� ���������*/
ALTER INDEX ALL ON [ETL_1C_SQL].[dbo].[��_���������������_TEMP] DISABLE;

EXEC sp_executesql 
N'
/*----���������� �����------------------------*/
BEGIN TRAN;
DECLARE @DATE_START_ DATETIME
DECLARE @DATE_END_ DATETIME
SELECT 
@DATE_START_ = DATEADD(YEAR,+2000,CONVERT(DATETIME,DATEADD(month,-4, DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0)))),  
@DATE_END_ = DATEADD(YEAR,+2000,DATEADD(MILLISECOND,-003,DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0))))

/*���������� ��������� ������� � ������� �������*/
UPDATE [ETL_1C_SQL].[dbo].[Table_������_��������]
SET[SQL_Text] = REPLACE([SQL_Text], 
			SUBSTRING([SQL_Text],CHARINDEX(''@P23 numeric(10)'', [SQL_Text])+LEN(''@P23 numeric(10)'''',''),53),
			''{ts '''''' + CONVERT(VARCHAR,@DATE_START_, 120)+ ''''''},{ts '''''' + CONVERT(VARCHAR,@DATE_END_,120)+''''''}'')
FROM [ETL_1C_SQL].[dbo].[Table_������_��������] AS Q
INNER JOIN [ETL_1C_SQL].[dbo].[Table_������_�������] AS R ON R.[id_reports]= Q.[id_reports]
WHERE R.[name_reports] = ''�������� ���������� �������� �� ��������_����''

DECLARE @DATE_START_2_ VARCHAR(23)
DECLARE @DATE_END_2_ VARCHAR(23)

SELECT @DATE_START_2_ = CONVERT(VARCHAR,DATEADD(YEAR,-2000,@DATE_START_),121), 
       @DATE_END_2_ = CONVERT(VARCHAR,DATEADD(YEAR,-2000,@DATE_END_),121)

/*�������� ������*/
/*id_reports*/
DECLARE @id_reports_ as varchar(3) 
SET @id_reports_ = (SELECT DISTINCT [id_reports] 
		FROM [ETL_1C_SQL].[dbo].[Table_������_�������] 
		WHERE [name_reports] = ''�������� ���������� �������� �� ��������_����'')
/*������� � ��*/
/*��������� ����������, ������� ������ id �������� �� ������� � ���������� ����� row_number*/
DECLARE @tbl_srv_ TABLE (id_row int, [id_servers] int)
INSERT INTO @tbl_srv_
SELECT DISTINCT 
ROW_NUMBER() OVER(ORDER BY [id_Linked_Servers] asc) AS id_row,
[id_Linked_Servers] AS [id_servers]
FROM  
[ETL_1C_SQL].[dbo].[Table_������_��������] AS F,
[ETL_1C_SQL].[dbo].[Table_������_���������_��������] AS G
WHERE 
F.[id_Linked_Servers] = G.[id_servers] AND  
G.[status] = 1 AND
F.[id_reports] = @id_reports_ 

/*������ ������ ������ ��������*/
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
		             from [ETL_1C_SQL].[dbo].[Table_������_��������] as d 
		             where d.[id_Linked_Servers] = @id_server__ and d.[id_reports] = @id_reports_)
/*����� �������� ��������� ��������*/
print @id_server__
print @sql_insert_text_
		             
EXEC (''BEGIN TRAN;
EXEC sp_executesql N''''
INSERT INTO [ETL_1C_SQL].[dbo].[��_���������������_TEMP] 
EXEC sp_executesql '' + @sql_insert_text_ + '';
COMMIT TRAN;'')
SET @id_row_ = @id_row_ - 1
END;
COMMIT TRAN;
'
/*����������� ������*/
----1. �������� � �������� ������� � ������� ����-�����;
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[��_���������������_TEMP_PREV];
ALTER INDEX ALL ON [ETL_1C_SQL].[dbo].[��_���������������_TEMP_PREV] REBUILD;

----2. ���������� ������ ������ �� ������ ����������� ������ � ������� ��������� ��� ����������� �� ������� ������� � ������� ����-����� 
----3. ����������� ������ �� ������� ������� � ������� ����-�����;
DECLARE @tbl_part TABLE ([id_row] int, [partition_number] int, [rows] int)
INSERT INTO @tbl_part
SELECT ROW_NUMBER() OVER(ORDER BY [partition_number] DESC) AS [id_row], [partition_number], [rows] 
FROM (SELECT DISTINCT [partition_number], [rows] FROM [ETL_1C_SQL].sys.partitions WITH(NOLOCK) 
	  WHERE object_id = object_id (N'��_���������������_TEMP', 'U' ) AND ROWS > 0) AS T;

DECLARE @id_row VARCHAR(3)
SET @id_row = (SELECT DISTINCT MAX(id_row) AS id_row FROM @tbl_part)
  WHILE @id_row > 0
  BEGIN 
  DECLARE @query VARCHAR(4000)
  SET @query = (SELECT 'ALTER TABLE [dbo].[��_���������������] SWITCH PARTITION ' + CONVERT(VARCHAR(3),[partition_number]) + ' TO [dbo].[��_���������������_TEMP_PREV] PARTITION ' + CONVERT(VARCHAR(3),[partition_number])
			   FROM @tbl_part WHERE id_row = @id_row)
  EXEC (@query)
SET @id_row = @id_row - 1
END;

----4. ����������� ������� ����-����� � ������������� ��������, ������� �� ��������� ������ (�������� �� ����� [����������], CONVERT(VARCHAR(7),[������],126));
INSERT INTO [ETL_1C_SQL].[dbo].[��_���������������_TEMP]
SELECT * FROM [ETL_1C_SQL].[dbo].[��_���������������_TEMP_PREV] AS T1
WHERE NOT EXISTS (SELECT * FROM [ETL_1C_SQL].[dbo].[��_���������������_TEMP] AS T2 WHERE T1.[����������] = T2.[����������] AND CONVERT(VARCHAR(7),T1.[������],126) = CONVERT(VARCHAR(7),T2.[������],126));

  /*���������� ����� � ������� ���������������*/
UPDATE [ETL_1C_SQL].[dbo].[��_���������������_TEMP]
SET [����������������] = 0
FROM [ETL_1C_SQL].[dbo].[��_���������������_TEMP]
WHERE [����������������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_���������������_TEMP]
SET [����������������] = 0
FROM [ETL_1C_SQL].[dbo].[��_���������������_TEMP]
WHERE [����������������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_���������������_TEMP]
SET [����������������] = 0
FROM [ETL_1C_SQL].[dbo].[��_���������������_TEMP]
WHERE [����������������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_���������������_TEMP]
SET [���������������] = 0
FROM [ETL_1C_SQL].[dbo].[��_���������������_TEMP]
WHERE [���������������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_���������������_TEMP]
SET [���������������] = 0
FROM [ETL_1C_SQL].[dbo].[��_���������������_TEMP]
WHERE [���������������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_���������������_TEMP]
SET [���������������]= 0
FROM [ETL_1C_SQL].[dbo].[��_���������������_TEMP]
WHERE [���������������] IS NULL;

----5. �������� ������� � ������������� �������;
ALTER INDEX ALL ON [ETL_1C_SQL].[dbo].[��_���������������_TEMP] REBUILD;

----6. ����������� ������ �� ������������� ������� � ������� �������;
SET @id_row = (SELECT DISTINCT MAX(id_row) AS id_row FROM @tbl_part)
  WHILE @id_row > 0
  BEGIN 
  SET @query = (SELECT 'ALTER TABLE [dbo].[��_���������������_TEMP] SWITCH PARTITION ' + CONVERT(VARCHAR(3),[partition_number]) + ' TO [dbo].[��_���������������] PARTITION ' + CONVERT(VARCHAR(3),[partition_number])
			   FROM @tbl_part WHERE id_row = @id_row)
  EXEC (@query)
SET @id_row = @id_row - 1
END;

----7. �������� ������� ����-����� � �������������;
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[��_���������������_TEMP_PREV];
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[��_���������������_TEMP];

----8. �������� ���������� ��� ������� �������;
UPDATE STATISTICS [ETL_1C_SQL].[dbo].[��_���������������];