/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [id_servers]
      ,[name_servers]
      ,[name_catalog_db]
      ,[name_location_db]
      ,[name_region]
      ,[name_city_magazin]
      ,[status]
      ,[time_server_polled]
  FROM [ETL_1C_SQL].[dbo].[Table_Список_Связанных_Серверов]