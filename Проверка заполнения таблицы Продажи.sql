/****** Script for SelectTopNRows command from SSMS  ******/
use [ETL_1C_SQL]
SELECT [БазаДанных]
      ,count([Артикул]) as ЧислоСтрок
      ,sum([Количество]) as Количество
      ,sum([Стоимость]) as Стоимость
	   

  FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP] where Регистратор_Тип like 'Реализация%'
  group by [БазаДанных]
  order by [БазаДанных]

  --не было продаж по данным базам
  SELECT DISTINCT d.[id_Linked_Servers] 
			select *	FROM [ETL_1C_SQL].[dbo].[Table_Список_Запросов] as d 
				INNER JOIN [ETL_1C_SQL].[dbo].[Table_Список_Связанных_Серверов] as t on t.id_servers = d.id_Linked_Servers 
				and d.[id_Linked_Servers] not in (select distinct БазаДанных from [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP])
				WHERE  status = 1
				order by 1

select distinct t2.Наименование, t3.Наименование,t3.БазаДанных	
from [ETL_1C_SQL].[dbo].[рн_ОборотыОстатков] as t
left outer join [ETL_1C_SQL].[dbo].[Table_Справочник_Склады] as t2  on t2.БазаДанных = t.БазаДанных and t2.Ссылка_ID = t.Склад_Ссылка
left outer join [ETL_1C_SQL].[dbo].[Table_Справочник_Подразделения] as t3  on t3.БазаДанных = t2.БазаДанных and t2.Подразделение_Ссылка = t3.Ссылка_ID
where t2.м_СкладВнешний=0x00 and convert(date,t.Период) >'2021-06-01' and t.БазаДанных in (11,58)
select  l.Ссылка_ID,
 sum(p.Количество),
 l.[Код], l.БазаДанных


from [ETL_1C_SQL].[dbo].[рн_Продажи] p 
join  [ETL_1C_SQL].[dbo].[Table_Справочник_Подразделения] l on p.ЛП_Отгрузки_Ссылка=l.Ссылка_ID 
join [ETL_1C_SQL].[dbo].[Table_Справочник_Контрагенты] K on p.Контрагент_ссылка=k.Ссылка_ID  
where l.БазаДанных in (11,58) and  l.ПометкаУдаления=0х00  and
convert(date,p.Период) >'2021-04-01' 
group by  l.[Код],  l.Ссылка_ID, l.БазаДанных
order by 2 ,3
select * from [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP] p
select * from [dbo].[Table_Справочник_Подразделения] s  where БазаДанных in (11,58)

--[dbo].[Table_Справочник_Организации_TEMP]
select * from [dbo].[Table_Список_Подразделения] where БазаДанных in (17)

select *  from [dbo].[Table_Перечисления_м_ТипОтгрузки]
  SELECT Номенклатура_ссылка
   select k.Ссылка_ID, k.Наименование from [ETL_1C_SQL].[dbo].[Table_Справочник_Контрагенты] K -- on Контрагент_ссылка=k.Ссылка_ID  
 -- delete    FROM [ETL_1C_SQL].[dbo].[рн_Продажи_TEMP] Where Период > convert(datetime,'2021-06-30 23:59:59',121)

 SELECT top 1000  t1.[БазаДанных]
      ,[Период]
      ,[Номенклатура_Ссылка]
      ,[Склад_Ссылка]
      ,[Количество]
      ,[Стоимость]
      ,[Дата_Актуальности]

	  sum (Количество) over (partition by Склад_ссылка)
	  sum  (Стоимость) over (partition by Склад_ссылка)
  FROM [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам] as t1
  left join ETL_1C_SQL.dbo.Table_Справочник_Номенклатура as t
on t1.Номенклатура_Ссылка = t.Ссылка_ID and t
where t.Артикул = '0042904338'
and convert (date, t1.Период)='2021-02-01' 
and t1.БазаДанных in (11,58)

