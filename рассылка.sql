  
declare @subject varchar(150)
DECLARE @DAY INT
SET @DAY = DATEPART(day,GETDATE()) 
IF (@DAY NOT IN (1,10))
set @subject = ' OLAP обновлен на '+convert(varchar(10), DATEADD(day,-1,GETDATE()),126)
ELSE 
set @subject = ' OLAP ЗА ПРОШЛЫЙ МЕСЯЦ обновлен на'+convert(varchar(10), DATEADD(day,-1,GETDATE()),126)

declare @body nvarchar(MAX) 
set @body =  '<H3>Уважаемые Коллеги, добрый день.</H3> <p>'+ @subject + '</p>' +
'<p> С уважением, <br>'+
'Сдержиков Даниил Александрович <br>'+
'Программист SQL <br>'+
'Компания «Минимакс» <br>'+
'г. Санкт-Петербург, Лиговский пр., д. 260 <br>'+
'Тел.: (812) 244-00-22  (доб. 5671) <br>'+
'E-mail: sderjikov@minimaks.ru </p>';


EXEC msdb.dbo.sp_send_dbmail  
		 @profile_name = 'ANALITIK_SQL', 
        @recipients ='havinson@MINIMAKS.RU;Rygkov@MINIMAKS.RU;samsonova@minimaks.ru;asmirnova@MINIMAKS.RU;Velieva@minimaks.ru;moiseev@MINIMAKS.RU;yadrenov@minimaks.ru',
 	   @copy_recipients ='sderjikov@minimaks.ru;torosyan@minimaks.ru',
		@body =  @body,  
		@body_format =  'HTML',
		@subject = @subject ;  