/*
ALTER DATABASE [MINIMAKS_SQL]
SET SINGLE_USER
WITH ROLLBACK IMMEDIATE;
*/
use MINIMAKS_SQL

declare @sql as nvarchar(max), @sql1 as nvarchar(max)
declare @Date as nvarchar(20), @ReportDate as nvarchar(255)
 /*SELECT DATEADD(month, DATEDIFF(month, -1, getdate()) - 2, 0) as FirtDayPreviousMonthWithTimeStamp,
    DATEADD(ss, -1, DATEADD(month, DATEDIFF(month, 0, getdate()), 0)) as LastDayPreviousMonthWithTimeStamp
	*/

set @Date = convert(nvarchar(10),DATEADD(month, DATEDIFF(month, -1, getdate()) - 2, 0),121)

select @date
set @ReportDate='[Dim_Календарь].[Дата].&[' + @Date + 'T00:00:00]'

delete from [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_всп2]
where [Дата отчета]=convert(date,@Date)




set @sql= 'insert into [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_всп2] ([Дата отчета],[ЛогПодразделение],[Артикул],[Код ВЗТ],[РА],[Сверхнорматив ВЗТ],[Сверхнорматив],[Максимальный остаток]) ' +
'SELECT ' + 
       'convert(date, convert(nvarchar(20),[[Dim_Календарь]].[Дата]].[Дата]].[MEMBER_CAPTION]]])) as [Дата отчета], '+
       'convert(nvarchar(255),[[Dim_Предприятие]].[Предприятие_Подразделение_Наименование]].[Предприятие_Подразделение_Наименование]].[MEMBER_CAPTION]]]) as ЛогПодразделение, ' +
       'convert(nvarchar(20),[[Dim_Номенклатура]].[Номенклатура_Артикул]].[Номенклатура_Артикул]].[MEMBER_CAPTION]]]) as Артикул, '+
       'convert(nvarchar(20),[[Dim_Номенклатура]].[Номенклатура_ВЗТ_Код]].[Номенклатура_ВЗТ_Код]].[MEMBER_CAPTION]]]) as [Код ВЗТ], '+
       'convert(int, [[Measures]].[РА_Количество_Ежедневный]]]) as РА, ' +
       'convert(float, [[Measures]].[КН_ВЗТ_Уровень_Сверхнорматива_Ежедневный]]]) as [Сверхнорматив ВЗТ], '+
       'convert(float, [[Measures]].[КН_Номенклатура_Уровень_Сверхнорматива_Ежедневный]]]) as [Сверхнорматив], '+
       'convert(float, [[Measures]].[РН_Номенклатура_Максимальный_Остаток_Ежедневный]]]) as [Максимальный остаток] '  

set @sql1= 'FROM openquery(SSAS_DATA_ANALYSIS, ''' +
' SELECT ' +
       'NON EMPTY { '+
                   '[Measures].[РА_Количество_Ежедневный], '+
                   '[Measures].[КН_ВЗТ_Уровень_Сверхнорматива_Ежедневный], ' +
                   '[Measures].[КН_Номенклатура_Уровень_Сверхнорматива_Ежедневный], ' + 
                   '[Measures].[РН_Номенклатура_Максимальный_Остаток_Ежедневный] ' +
                   '} ON COLUMNS, ' +
      'NON EMPTY { ({' + @ReportDate + 
                    '},[Dim_Предприятие].[Предприятие_Подразделение_Наименование].[Предприятие_Подразделение_Наименование].ALLMEMBERS * ' +
                   '[Dim_Номенклатура].[Номенклатура_Артикул].[Номенклатура_Артикул].ALLMEMBERS * ' +
                   '[Dim_Номенклатура].[Номенклатура_ВЗТ_Код].[Номенклатура_ВЗТ_Код].ALLMEMBERS ) } having [Measures].[РА_Количество_Ежедневный]>0 ON ROWS ' +
 'FROM ( SELECT ( { [Dim_Рабочий_Ассортимент_Признак].[Рабочий_Ассортимент_Признак].&[РА] } ) ON COLUMNS ' +
        'FROM ( SELECT ( { [Dim_Предприятие].[Предприятие_Подразделение_Признак_Логистическое].&[Да] } ) ON COLUMNS ' +
               'FROM ( SELECT ( { ' + @ReportDate + 
                    '} ) ON COLUMNS ' +
                      'FROM [DATA_ANALYSIS]))) ' +
 'WHERE (  ' +
         '[Dim_Предприятие].[Предприятие_Подразделение_Признак_Логистическое].&[Да], ' +
         '[Dim_Рабочий_Ассортимент_Признак].[Рабочий_Ассортимент_Признак].&[РА] ' +
         ') '')'
         
select (@sql+@sql1)

exec(@sql+@sql1)     

--delete from [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_всп2] where [Дата отчета]=CONVERT(date, @Date)

-- обновляем уровень сверхнорматива
update [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_всп2]
      set [Уровень сверхнорматива]=
      
      case when [Код ВЗТ]<>'' and [Код ВЗТ] is not null  
             then 
                 case when [Сверхнорматив ВЗТ]> round([Максимальный остаток]*1.2,0)
                        then [Сверхнорматив ВЗТ]
                      else round([Максимальный остаток]*1.2,0)
                 end       
             else case when [Сверхнорматив]> round([Максимальный остаток]*1.2,0)
                        then [Сверхнорматив]
                      else round([Максимальный остаток]*1.2,0)
                 end 
      end
where [Дата отчета]=CONVERT(date, @Date)    

--select top 1000 * from [MINIMAKS_SQL].[dbo].[ОтчетОборачиваемостьНов_всп2]  where [Дата отчета]='2021-06-01'