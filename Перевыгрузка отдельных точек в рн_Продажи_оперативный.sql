select * from sys.partitions where object_id = 
(select Object_id from sys.tables where Name='��_�������_Temp')


USE [ETL_1C_SQL];
/*�������� ���������� ������ �� ������� ���_���*/
/*������ ��� ��������*/
DECLARE @DATE_START DATETIME
DECLARE @DATE_END DATETIME
SELECT 
		@DATE_START = DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0),  
		@DATE_END = (CASE 
						WHEN DATEPART(mm,DATEADD(ss,-1,DATEADD(dd, DATEDIFF(dd,0,CURRENT_TIMESTAMP),0))) < DATEPART(mm, CURRENT_TIMESTAMP) 
						THEN DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0)
						ELSE DATEADD(ss,-1,DATEADD(dd, DATEDIFF(dd,0,CURRENT_TIMESTAMP),0))
					END);

/*���������� ������� � ������ ��������*/
UPDATE [ETL_1C_SQL].[dbo].[Table_������_��������]
SET [SQL_Text] = REPLACE([SQL_Text],
				SUBSTRING([SQL_Text],CHARINDEX('BETWEEN', [SQL_Text]),67),
				'BETWEEN '''''+CONVERT(VARCHAR,@DATE_START,121)+''''' AND '''''+CONVERT(VARCHAR,@DATE_END,121)+'''''')
FROM [ETL_1C_SQL].[dbo].[Table_������_��������] AS Q WITH(NOLOCK)
INNER JOIN [ETL_1C_SQL].[dbo].[Table_������_�������] AS R WITH(NOLOCK) ON R.[id_reports]= Q.[id_reports]
WHERE R.[name_reports] = '�������� ���������� ������ �� ������� ���_���' AND R.[id_reports] = 2 AND Q.[SQL_Text] like '%BETWEEN%';

/*������� ������ �� ������� ���������*/
--TRUNCATE TABLE [ETL_1C_SQL].[dbo].[��_�������_TEMP];

/*��������� ��� ������� � ������� ���������*/
ALTER INDEX ALL ON [ETL_1C_SQL].[dbo].[��_�������_TEMP] DISABLE;

/*��������� ������ � ������� ��������*/
--������� ��������� �������
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
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##����������_'', N''U'') IS NOT NULL
          DROP TABLE ##����������_';
--������� ��������� �������
create table ##OwnOrg_
(
    [����������������] [binary](16) NOT NULL,
	[����������] [nvarchar](100) NULL,
	[��������������] [binary](4) NOT NULL,
	[�������������_������] [binary](16) NOT NULL,
	[�������������] [nvarchar](75) NULL,
	[����������] [INT] NOT NULL
);

create table ##RegSal_
(
	[���������������_������] [binary](16) NOT NULL,
	[������������_������] [binary](16) NOT NULL,
	[����������] [numeric](38, 3) NULL,
	[���������] [numeric](38, 2) NULL,
	[������������������] [numeric](38, 2) NULL,
	[������������������������] [numeric](38, 2) NULL,
	[����������] [INT] NOT NULL
);

create table ##DocLzk_
(
	[�_���_������] [binary](16) NOT NULL,
	[������������_������] [binary](16) NOT NULL,
	[����������] [numeric](38, 3) NULL,
	[���������] [numeric](38, 2) NULL,
	[����������] [INT] NOT NULL
);

create table ##DocPer_
(
	[�����������_������] [binary](16) NOT NULL,
	[������������_������] [binary](16) NOT NULL,
	[����������] [numeric](38, 3) NULL,
	[����������������] [numeric](38, 5) NULL,
	[����������] [INT] NOT NULL
);

create table ##RoznSl_
(
	[�����������������������_������] [binary](16) NOT NULL,
	[������������_������] [binary](16) NOT NULL,
	[����������] [numeric](38, 3) NULL,
	[�����] [numeric](38, 3) NULL,
	[��������������] [numeric](38, 3) NULL,
	[����������] [INT] NOT NULL
);

create table ##CheKKM_
(
	[������_������] [binary](16) NOT NULL,
	[������������_������] [binary](16) NOT NULL,
	[����������] [numeric](38, 3) NULL,
	[�����] [numeric](38, 3) NULL,
	[��������������] [numeric](38, 3) NULL,
	[����������] [INT] NOT NULL
);

create table ##����������_
(
	[������] [datetime] NULL,
	[������������_������] [binary](16) NOT NULL,
	[�����������] [numeric] (9,0) NULL, 
	[����������_������] [binary](16) NULL,
	[��������_������] [binary](16) NULL,
	[�������������_������] [binary](16) NULL,
	[��������_������] [binary](16) NULL,
	[��_��������_������] [binary](16) NULL,
	[��_��������_���_������] [binary](16) NULL,
	[��_����������_������] [binary](16) NULL,
	[��_����������_���_������] [binary](16) NULL,
	[�����������_�����] [nchar](50) NULL,
	[�����������_���] [varchar](150) NULL,
	[���_��������_������] [binary](16) NULL,/*[varchar](50) NULL,*/
	[����������] [numeric](38, 3) NULL,
	[�����������������] [numeric](38, 3) NULL,
	[����������] [INT] NOT NULL
);

/*id_reports*/
declare @id_reports as int
		   set @id_reports = (SELECT distinct [id_reports] 
						      FROM [ETL_1C_SQL].[dbo].[Table_������_�������] 
							  where [name_reports] = '�������� ���������� ������ �� ������� ���_���')
/*������� � ��*/
DECLARE @i_server INT 
SET @i_server = 40 /*(SELECT DISTINCT max([id_Linked_Servers])  
		FROM [ETL_1C_SQL].[dbo].[Table_������_��������] as d
		INNER JOIN [ETL_1C_SQL].[dbo].[Table_������_���������_��������] as t on t.id_servers = d.id_Linked_Servers 
		WHERE [id_reports] = @id_reports and status = 1)*/
WHILE @i_server > 0
	 BEGIN
		DECLARE @id_server varchar(255)
		SET @id_server = (SELECT DISTINCT d.[id_Linked_Servers] 
				FROM [ETL_1C_SQL].[dbo].[Table_������_��������] as d 
				INNER JOIN [ETL_1C_SQL].[dbo].[Table_������_���������_��������] as t on t.id_servers = d.id_Linked_Servers 
				WHERE d.[id_Linked_Servers] = @i_server and status = 1)
	 SET @i_server = @i_server - 1

/*SQL_Text*/
/*��������� ��������� ��� �������*/	 
DECLARE @IdReports INT
DECLARE @IdLinkedServer INT
DECLARE @N INT
DECLARE @SqlText VARCHAR (MAX)
/*��������� ������*/
DECLARE @CURSOR CURSOR
/*��������� ������*/
SET @CURSOR  = CURSOR SCROLL
	FOR SELECT [id_reports], [id_Linked_Servers], [N], [SQL_Text]
		FROM [ETL_1C_SQL].[dbo].[Table_������_��������]
		WHERE [id_Linked_Servers] = @id_server and [id_reports] = @id_reports
		ORDER BY [N] ASC
/*��������� ������*/
OPEN @CURSOR
/*�������� ������ ������*/
FETCH NEXT FROM @CURSOR INTO @IdReports, @IdLinkedServer, @N, @SqlText
/*��������� � ����� ������� �����*/
	WHILE @@FETCH_STATUS = 0
	    BEGIN
	  DECLARE @sql_text varchar(max)
	  SET @sql_text = (select distinct ' BEGIN TRAN; ' + REPLACE(d.[SQL_Text],'��_�������','��_�������_TEMP') + ' COMMIT TRAN; '
					   from [ETL_1C_SQL].[dbo].[Table_������_��������] as d 
					   where d.[id_Linked_Servers] = convert(varchar(3),@IdLinkedServer) and 
					         d.[N] = convert(varchar(3),@N) and
							 d.[id_reports] = convert(varchar(3),@IdReports))
      
      print 'Server: ' + convert(varchar(3),@IdLinkedServer)
      print 'Report: ' + convert(varchar(3),@IdReports) + '    � ' + convert(varchar(3),@N)
      print 'Query: '
      print @sql_text    
      
      EXEC (@sql_text)
/*�������� ��������� ������*/
		FETCH NEXT FROM @CURSOR INTO @IdReports, @IdLinkedServer, @N, @SqlText
	END
CLOSE @CURSOR

TRUNCATE TABLE ##OwnOrg_
TRUNCATE TABLE ##RegSal_
TRUNCATE TABLE ##DocLzk_
TRUNCATE TABLE ##DocPer_
TRUNCATE TABLE ##RoznSl_
TRUNCATE TABLE ##CheKKM_
TRUNCATE TABLE ##����������_

END;

--������� ��������� �������
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
@stmt = N'IF OBJECT_ID(N''tempdb.dbo.##����������_'', N''U'') IS NOT NULL
          DROP TABLE ##����������_';


		  select * from [ETL_1C_SQL].[dbo].[��_�������] where convert(date,������) ='2021-06-30'

/*����������� ������*/
----1. �������� � �������� ������� � ������� ����-�����;
TRUNCATE TABLE [ETL_1C_SQL].[dbo].[��_�������_TEMP_PREV];
ALTER INDEX ALL ON [ETL_1C_SQL].[dbo].[��_�������_TEMP_PREV] REBUILD;

----2. ���������� ������ ������ �� ������ ����������� ������ � ������� ��������� ��� ����������� �� ������� ������� � ������� ����-����� 
----3. ����������� ������ �� ������� ������� � ������� ����-�����;
DECLARE @tbl_part TABLE ([id_row] int, [partition_number] int, [rows] int)
INSERT INTO @tbl_part
SELECT ROW_NUMBER() OVER(ORDER BY [partition_number] DESC) AS [id_row], [partition_number], [rows] 
FROM (SELECT DISTINCT [partition_number], [rows] FROM sys.partitions WITH(NOLOCK) 
	  WHERE object_id = object_id (N'��_�������_TEMP', 'U' ) AND ROWS > 0) AS T;

	--  select * from sys.partitions WITH(NOLOCK) 

DECLARE @id_row VARCHAR(3)
SET @id_row = (SELECT DISTINCT MAX(id_row) AS id_row FROM @tbl_part)
  WHILE @id_row > 0
  BEGIN 
  DECLARE @query VARCHAR(4000)
  SET @query = (SELECT 'ALTER TABLE [dbo].[��_�������] SWITCH PARTITION ' + CONVERT(VARCHAR(3),[partition_number]) + ' TO [dbo].[��_�������_TEMP_PREV] PARTITION ' + CONVERT(VARCHAR(3),[partition_number])
			   FROM @tbl_part WHERE id_row = @id_row)
  EXEC (@query)
SET @id_row = @id_row - 1
END;
select count(1) from [dbo].[��_�������_TEMP]  where convert(date,������) <'2021-06-30'
----4. ����������� ������� ����-����� � ������������� ��������, ������� �� ��������� ������ (�������� �� ����� [����������], CONVERT(VARCHAR(7),[������],126));
INSERT INTO [ETL_1C_SQL].[dbo].[��_�������_TEMP_PREV] 
SELECT *  FROM [ETL_1C_SQL].[dbo].[��_�������_TEMP] AS T1
WHERE not EXISTS (SELECT 1 FROM [ETL_1C_SQL].[dbo].[��_�������_TEMP_PREV]  AS T2 
				  WHERE 
				  CONVERT(VARCHAR(7),T1.[������],126) = CONVERT(VARCHAR(7),T2.[������],126) AND
				  T1.[����������] = T2.[����������] AND 
				  T1.[�������] = T2.[�������] AND 
				  T1.[�����������_�����] = T2.[�����������_�����]);
--select * from ##sales_yet
/*���������� ����� � ������� ������*/
 
--truncate table [dbo].[��_�������_TEMP]
ALTER INDEX ALL on [ETL_1C_SQL].[dbo].[��_�������_TEMP]  disable
INSERT INTO [ETL_1C_SQL].[dbo].[��_�������_TEMP] 
SELECT *  FROM [ETL_1C_SQL].[dbo].[��_�������_TEMP_PREV] 

ALTER TABLE [dbo].[��_�������_TEMP_PREV] SWITCH PARTITION 97 TO [dbo].[��_�������_TEMP] PARTITION 97

UPDATE [ETL_1C_SQL].[dbo].[��_�������_TEMP]
SET [��������_������] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[��_�������_TEMP]
WHERE [��������_������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_�������_TEMP]
SET [����������_������] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[��_�������_TEMP]
WHERE [����������_������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_�������_TEMP]
SET [�������������_������] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[��_�������_TEMP]
WHERE [�������������_������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_�������_TEMP]
SET [��������_������] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[��_�������_TEMP]
WHERE [��������_������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_�������_TEMP]
SET [��_��������_������] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[��_�������_TEMP]
WHERE [��_��������_������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_�������_TEMP]
SET [��_��������_���_������] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[��_�������_TEMP]
WHERE [��_��������_���_������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_�������_TEMP]
SET [��_����������_������] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[��_�������_TEMP]
WHERE [��_����������_������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_�������_TEMP]
SET [��_����������_���_������] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[��_�������_TEMP]
WHERE [��_����������_���_������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_�������_TEMP]
SET [���_��������_������] = 0x00000000000000000000000000000000
FROM [ETL_1C_SQL].[dbo].[��_�������_TEMP]
WHERE [���_��������_������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_�������_TEMP]
SET [������������������] = 0
FROM [ETL_1C_SQL].[dbo].[��_�������_TEMP]
WHERE [������������������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_�������_TEMP]
SET [������������������������] = 0
FROM [ETL_1C_SQL].[dbo].[��_�������_TEMP]
WHERE [������������������������] IS NULL;

UPDATE [ETL_1C_SQL].[dbo].[��_�������_TEMP]
SET [�����������������] = 0
FROM [ETL_1C_SQL].[dbo].[��_�������_TEMP]
WHERE [�����������������] IS NULL;

----5. �������� ������� � ������������� �������;
ALTER INDEX ALL ON [ETL_1C_SQL].[dbo].[��_�������_TEMP] REBUILD;

----6. ����������� ������ �� ������������� ������� � ������� �������;
SET @id_row = (SELECT DISTINCT MAX(id_row) AS id_row FROM @tbl_part)
  WHILE @id_row > 0
  BEGIN 
  SET @query = (SELECT 'ALTER TABLE [dbo].[��_�������_TEMP] SWITCH PARTITION ' + CONVERT(VARCHAR(3),[partition_number]) + ' TO [dbo].[��_�������] PARTITION ' + CONVERT(VARCHAR(3),[partition_number])
			   FROM @tbl_part WHERE id_row = @id_row)
			   print @query
  EXEC (@query)
SET @id_row = @id_row - 1
END;

----7. �������� ������� ����-����� � �������������;
--TRUNCATE TABLE [ETL_1C_SQL].[dbo].[��_�������_TEMP_PREV];
--TRUNCATE TABLE [ETL_1C_SQL].[dbo].[��_�������_TEMP];

----8. �������� ���������� ��� ������� �������;
--UPDATE STATISTICS [ETL_1C_SQL].[dbo].[��_�������];
