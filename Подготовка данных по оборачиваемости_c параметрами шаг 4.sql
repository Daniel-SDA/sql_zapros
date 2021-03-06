/****** Script for SelectTopNRows command from SSMS  ******/
--drop table select top 1000 * from [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_посл]
declare @DtF as date
set @DtF=DATEADD(month, DATEDIFF(month, -1, getdate()) - 2, 0)

drop table  [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_посл]  
SELECT *
  into  [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_посл]   
  FROM [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_всп]
  where [Дата отчета]= @DtF
  
  /*
DATEADD(month, - 1, CONVERT(date, CONVERT(varchar(7), 
                      CONVERT(date, GETDATE())) + '-01'))
					  */
drop table [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_посл2]

SELECT *
  into  [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_посл2]   
  FROM [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_всп2]
  where [Дата отчета]=@DtF/*DATEADD(month, - 1, CONVERT(date, CONVERT(varchar(7), 
                      CONVERT(date, GETDATE())) + '-01'))     */ 

drop table [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_посл3]

SELECT * 
  into  [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_посл3]   
 FROM [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_всп1]
  where [Дата отчета]=@DtF
  
 -- select * from  [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_посл3] 

					  
					  /*
					  select * from [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_посл]             
					    select * from [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_посл2]   
						   select * from [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_всп2]   
						select * from [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_посл3]   */
					--	  select * FROM [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_всп1]