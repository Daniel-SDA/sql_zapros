/****** Script for SelectTopNRows command from SSMS  ******/
 
  select * from [dbo].[Table_Список_Связанных_Серверов]
  update [ETL_1C_SQL].[dbo].[Table_Список_Связанных_Серверов]
  set [name_servers]='[10.11.13.6]'
  where id_servers=14
  

  
update ts
   set [SQL_Text]=REPLACE([SQL_Text], '[10.11.13.1]','[10.11.13.6]')
    from [ETL_1C_SQL].[dbo].[Table_Список_Запросов] ts
  where [id_Linked_Servers]=14



  
  /*
 
  
update ts
   set [SQL_Text]=REPLACE([SQL_Text], '] * 1.18               when (select distinct [НеВключатьНДСВСтоимостьПартий]','] * 1.20               when (select distinct [НеВключатьНДСВСтоимостьПартий]')
    from [ETL_1C_SQL].[dbo].[Table_Список_Запросов] ts
where ts.id_reports in (1,2)*/