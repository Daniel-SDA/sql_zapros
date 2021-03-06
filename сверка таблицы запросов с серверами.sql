/****** Script for SelectTopNRows command from SSMS  ******/


select tserv.name_catalog_db, tserv.name_servers, [id_reports]
      ,[id_Linked_Servers]
      ,[N], SUBSTRING(t1.SQL_Text,CHARINDEX('openquery',t1.SQL_Text),30) as query
 from [dbo].[Table_Список_Запросов] t1 join [dbo].[Table_Список_Связанных_Серверов] tserv on tserv.id_servers=t1.id_Linked_Servers  where CHARINDEX('openquery',t1.SQL_Text) >0 
  and id_reports=1 and N=1 order by [id_Linked_Servers] 





select t5.[id_Linked_Servers], t5.query, tbak.query, t5.id_reports from

(select [id_reports]
      ,[id_Linked_Servers]
      ,[N], SUBSTRING(t1.SQL_Text,CHARINDEX('openquery',t1.SQL_Text),30) as query
 from [dbo].[Table_Список_Запросов] t1 where CHARINDEX('openquery',t1.SQL_Text) >0 
  and id_reports=1 and N=1 order by [id_Linked_Servers]) t5
 
 join 
 (select [id_reports]
      ,[id_Linked_Servers]
      ,[N],  SUBSTRING(t1.SQL_Text,CHARINDEX('openquery',t1.SQL_Text),30) as query
 from [dbo].[Table_Список_Запросов7772021] t1 where CHARINDEX('openquery',t1.SQL_Text) >0  ) tbak
 on  t5.[id_reports]= tbak.[id_reports]
 and t5.[id_Linked_Servers] =tbak.[id_Linked_Servers]
 and t5.[N] =tbak.[N] and t5.[query]<>tbak.[query]
 
 

 --delete from [dbo].[Table_Список_Запросов]
 --insert into [dbo].[Table_Список_Запросов]

select [id_reports]
      ,[id_Linked_Servers]
      ,[N]
      ,[SQL_Text]
      ,[Date_Actual] from (SELECT ROW_NUMBER() over( partition by [id_Linked_Servers], id_reports,N  order by id_reports,N 
       ) as vers , [id_reports]
      ,[id_Linked_Servers]
      ,[N]
      ,[SQL_Text]
      ,[Date_Actual]
  FROM [ETL_1C_SQL].[dbo].[Table_Список_Запросов7772021]) t where vers=2



  SELECT SUBSTRING('SQL Tutorial', CHARINDEX('tut','SQL Tutorial') , 3) AS ExtractString;


  BETWEEN '2021-08-01'' AND ''+CONVERT(VARCHAR,@DATE_END,121)+''RRef = T1.RecorderRRef AND T1.RecorderTRef = 0x000023ED THEN CONVERT(VARCHAR(MAX),'Ввод контрольных и складских нормативов') WHEN T7._IDRRef = T1.RecorderRRef AND T1.RecorderTRef = 0x000000A4 THEN CONVERT(VARCHAR(MAX),'Корректировка записей регистров') END as Регистратор_Наименование , CASE WHEN T8._IDRRef = T1.RecorderRRef AND T1.RecorderTRef = 0x000023ED THEN T8._Number WHEN T7._IDRRef = T1.RecorderRRef AND T1.RecorderTRef = 0x000000A4 THEN T7._Number END as Регистратор_Номер , CASE WHEN T8._IDRRef = T1.RecorderRRef AND T1.RecorderTRef = 0x000023ED THEN CONVERT(VARCHAR,DATEADD(YEAR,-2000,T8._Date_Time), 121) WHEN T7._IDRRef = T1.RecorderRRef AND T1.RecorderTRef = 0x000000A4 THEN CONVERT(VARCHAR,DATEADD(YEAR,-2000,T7._Date_Time), 121) END as Регистратор_Дата , T1.LineNo_ as НомерСтроки, CASE WHEN T1.Active_ = 0x01 THEN 'Да' ELSE 'Нет' END as Активность,  ISNULL(T9._IDRRef, 0x00000000000000000000000000000000) as Подразделение_Ссылка, ISNULL(T10._IDRRef, 0x00000000000000000000000000000000) as Склад_Ссылка, CASE WHEN(T1.Fld12344_TYPE = 0x08 AND T1.Fld12344_RTRef = 0x00002217) THEN T11._IDRRef WHEN (T1.Fld12344_TYPE = 0x08 AND T1.Fld12344_RTRef = 0x0000003E) THEN T12._IDRRef END as [Номенклатура/НоменклатураВЗТ_Ссылка], T1.Fld12345_ as УровеньДефицита, T1.Fld12346_ as УровеньСверхнорматива, T1.Fld12347_ as УровеньДефицитаВДнях, T1.Fld12348_ as УровеньСверхнормативаВДнях, T13._IDRRef as Автор_Ссылка, 27 as БазаДанных  FROM  (SELECT T6._Period AS Period_, T6._RecorderTRef AS RecorderTRef, T6._RecorderRRef AS RecorderRRef, T6._LineNo AS LineNo_, T6._Active AS Active_, T6._Fld12342RRef AS Fld12342RRef, T6._Fld12343RRef AS Fld12343RRef, T6._Fld12344_TYPE AS Fld12344_TYPE, T6._Fld12344_RTRef AS Fld12344_RTRef, T6._Fld12344_RRRef AS Fld12344_RRRef, T6._Fld12345 AS Fld12345_, T6._Fld12346 AS Fld12346_, T6._Fld12347 AS Fld12347_, T6._Fld12348 AS Fld12348_, T6._Fld12349RRef AS Fld12349RRef FROM  (SELECT T3.Fld12342RRef AS Fld12342RRef, T3.Fld12343RRef AS Fld12343RRef, T3.Fld12344_TYPE AS Fld12344_TYPE, T3.Fld12344_RTRef AS Fld12344_RTRef, T3.Fld12344_RRRef AS Fld12344_RRRef, T3.MAXPERIOD_ AS MAXPERIOD_,SUBSTRING(MAX(T5._RecorderTRef + T5._RecorderRRef),1,4) AS  MAXRECORDERTRef ,SUBSTRING(MAX(T5._RecorderTRef + T5._RecorderRRef),5,16) AS  MAXRECORDERRRef  FROM ( SELECT T4._Fld12342RRef AS Fld12342RRef, T4._Fld12343RRef AS Fld12343RRef, T4._Fld12344_TYPE AS Fld12344_TYPE, T4._Fld12344_RTRef AS Fld12344_RTRef, T4._Fld12344_RRRef AS Fld12344_RRRef, MAX(T4._Period) AS MAXPERIOD_  FROM [trade].dbo._InfoRg12341 T4 WITH(NOLOCK)  WHERE T4._Active = 0x01 GROUP BY  T4._Fld12342RRef, T4._Fld12343RRef, T4._Fld12344_TYPE, T4._Fld12344_RTRef, T4._Fld12344_RRRef) T3 INNER JOIN [trade].dbo._InfoRg12341 T5 WITH(NOLOCK) ON T3.Fld12342RRef = T5._Fld12342RRef AND T3.Fld12343RRef = T5._Fld12343RRef AND T3.Fld12344_TYPE = T5._Fld12344_TYPE AND T3.Fld12344_RTRef = T5._Fld12344_RTRef AND T3.Fld12344_RRRef = T5._Fld12344_RRRef AND T3.MAXPERIOD_ =  T5._Period WHERE  T5._Active = 0x01  GROUP BY  T3.Fld12342RRef, T3.Fld12343RRef, T3.Fld12344_TYPE, T3.Fld12344_RTRef, T3.Fld12344_RRRef, T3.MAXPERIOD_) T2 INNER JOIN [trade].dbo._InfoRg12341 T6 WITH(NOLOCK) ON T2.Fld12342RRef = T6._Fld12342RRef AND T2.Fld12343RRef = T6._Fld12343RRef AND T2.Fld12344_TYPE = T6._Fld12344_TYPE AND T2.Fld12344_RTRef = T6._Fld12344_RTRef AND T2.Fld12344_RRRef = T6._Fld12344_RRRef AND T2.MAXPERIOD_ =  T6._Period AND T2.MAXRECORDERTRef =  T6._RecorderTRef AND T2.MAXRECORDERRRef =  T6._RecorderRRef) T1 LEFT OUTER JOIN [trade].dbo._Document164 T7 WITH(NOLOCK) ON T1.RecorderTRef = 0x000000A4 AND T1.RecorderRRef = T7._IDRRef LEFT OUTER JOIN [trade].dbo._Document9197 T8 WITH(NOLOCK) ON T1.RecorderTRef = 0x000023ED AND T1.RecorderRRef = T8._IDRRef LEFT OUTER JOIN [trade].dbo._Reference68 T9 WITH(NOLOCK) ON T1.Fld12342RRef = T9._IDRRef LEFT OUTER JOIN [trade].dbo._Reference80 T10 WITH(NOLOCK) ON T1.Fld12343RRef = T10._IDRRef LEFT OUTER JOIN [trade].dbo._Reference8727 T11 WITH(NOLOCK) ON T1.Fld12344_TYPE = 0x08 AND T1.Fld12344_RTRef = 0x00002217 AND T1.Fld12344_RRRef = T11._IDRRef LEFT OUTER JOIN [trade].dbo._Reference62 T12 WITH(NOLOCK) ON T1.Fld12344_TYPE = 0x08 AND T1.Fld12344_RTRef = 0x0000003E AND T1.Fld12344_RRRef = T12._IDRRef LEFT OUTER JOIN [trade].dbo._Reference69 T13 WITH(NOLOCK) ON T1.Fld12349RRef = T13._IDRRef