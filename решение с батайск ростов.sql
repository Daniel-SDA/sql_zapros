/****** Сценарий для команды SelectTopNRows среды SSMS  ******/
select * from [dbo].[рн_Остатки_По_Месяцам_TEMP] as t1

--insert into [dbo].[рн_Остатки_По_Месяцам_TEMP]
SELECT distinct skl.Наименование, skl.БазаДанных, t1.БазаДанных, skl.ВидСклада_Ссылка, skl.Код
   /*   ,[Период]
 
      ,[Склад_Ссылка]
 
      ,[Дата_Актуальности]
	  */

  FROM [ETL_1C_SQL].[dbo].[рн_Остатки_По_Месяцам] as t1

  left join ETL_1C_SQL.[dbo].[Table_Справочник_Склады]  skl
  on skl.Ссылка_ID = t1.Склад_Ссылка
   and skl.БазаДанных = t1.БазаДанных

where t1.БазаДанных in (11)
and convert (date, t1.Период)> '2020-12-01' 
and t1.Количество>0
and skl.Подразделение_Ссылка = '0b88e451-bffa-11e8-ae7d-0025906a9547' /*L544*/



 
drop table #wrong_base 
select   distinct skl.Ссылка_ID, skl.БазаДанных, skl.Наименование, podr.Код --into #wrong_base 
from [dbo].[Table_Справочник_Подразделения] podr 
left join ETL_1C_SQL.[dbo].[Table_Справочник_Склады]  skl
  on skl.Подразделение_Ссылка=podr.Ссылка_ID
where skl.БазаДанных in (11) and [м_СкладОстатков]=0x01 and
podr.м_ЛогистическоеПодразделение = 0x01 and  podr.Наименование like 'л544 %'
 insert into  #wrong_base
select   distinct skl.Ссылка_ID, skl.БазаДанных, skl.Наименование, podr.Код  from [dbo].[Table_Справочник_Подразделения] podr
left join ETL_1C_SQL.[dbo].[Table_Справочник_Склады]  skl
  on skl.Подразделение_Ссылка=podr.Ссылка_ID
where skl.БазаДанных in (58) and  [м_СкладОстатков]=0x01 and
podr.м_ЛогистическоеПодразделение = 0x01 and ( podr.Наименование  like 'л500%' or podr.Наименование  like 'л521%' or podr.Наименование  like 'л527%' or podr.Наименование  like 'л528 %'  or podr.Наименование  like 'л532 %' or podr.Наименование  like 'л535 %'  or podr.Наименование  like 'л539 %' or podr.Наименование  like 'л542 %' or podr.Наименование  like 'л544 %') 
select * from #wrong_base
 
update ost set Количество = 0 , Стоимость =0
   from  [dbo].[рн_Остатки_По_Месяцам] ost 
  join #wrong_base wrb
   on ost.БазаДанных=wrb.БазаДанных and 
   ost.Склад_ссылка=wrb.Ссылка_ID
  where convert(date,Период) = '2021-01-01' and
  ost.БазаДанных in (11,58)
  declare @DATE_START datetime
  select @DATE_START = '4021-01-01 00:00:00.000'
  -----------DATEADD(YEAR,+2000,CONVERT(DATETIME,DATEADD(mm,-1,DATEADD(mm, DATEDIFF(mm,0,'2022-01-01'),0))))
  select @DATE_START

@DATE_START_ = select DATEADD(YEAR,+2000,CONVERT(DATETIME,DATEADD(mm,-1,DATEADD(mm, DATEDIFF(mm,0,'2021-01-01'),0))))
@DATE_END_ = 

select DATEADD(YEAR,+2000,DATEADD(MILLISECOND,-003,DATEADD(dd,0,DATEADD(mm, DATEDIFF(mm,0,CURRENT_TIMESTAMP),0))))



  select top 100 * from [dbo].[рн_ОборотыОстатков] where convert(date,Период)='2021-03-01'

  select 
  * from    [dbo].[рн_Остатки_По_Месяцам] ost 
  /*join #wrong_base wrb
   on ost.БазаДанных=wrb.БазаДанных and 
   ost.Склад_ссылка=wrb.Ссылка_ID*/
  where convert(date,Период) = '2021-01-01' and
  ost.БазаДанных in (11,58)


order by 2, 3
and [Склад_Ссылка] in (select Ссылка_Id from [dbo].[Table_Справочник_Склады] 
where /*[Подразделение_Ссылка] in  (select Ссылка_Id from [dbo].[Table_Список_Подразделения]  where БазаДанных in (58))  and*/ БазаДанных=58 and м_СкладВнешний =0x00 and  м_СкладОстатков =0x01)



select t58.* from
(select *  from [dbo].[Table_Справочник_Склады] 
where БазаДанных=58) t58
join (select Ссылка_Id from [dbo].[Table_Справочник_Склады] 
where БазаДанных=11) t11
 on t58.Ссылка_ID = t11.Ссылка_ID

join 
select Ссылка_Id from [dbo].[Table_Список_Подразделения] join where БазаДанных in (11)

select * from [dbo].[Table_Справочник_Склады] 
where [Подразделение_Ссылка] in  (select Ссылка_Id from [dbo].[Table_Список_Подразделения]  where БазаДанных in (11))  and БазаДанных=58

select sklad.БазаДанных, sklad.Наименование, spodr.м_Идентификатор, sklad.Ссылка_ID from [dbo].[Table_Справочник_Подразделения] as spodr 
join [dbo].[Table_Справочник_Склады]  sklad
 on sklad.Подразделение_Ссылка=spodr.Ссылка_ID where sklad.БазаДанных in (11,58) and spodr.м_ЛогистическоеПодразделение=0x01
 group by sklad.БазаДанных, sklad.Наименование, spodr.м_Идентификатор, sklad.Ссылка_ID

 select top 1000 * from [dbo].[рн_ОборотыОстатков]  where convert(date,Период) = '2021-06-05'
 select top 1000 * from [dbo].[рн_Продажи]  where convert(date,Период) = '2021-06-05'
 
  
 select sum(Количество), БазаДанных from рн_Остатки_По_Месяцам where convert(date,Период)= '2021-01-01' and БазаДанных in(58,11) group by БазаДанных
 select sum(Количество), БазаДанных from [dbo].[рн_Остатки_По_Месяцам_bak210604] where convert(date,Период)= '2021-01-01' and БазаДанных in(58,11) group by БазаДанных


 
 EXEC msdb.dbo.sp_update_job @job_name='Сбор_Данных_Продажи_Оперативный_Период',@enabled = 1