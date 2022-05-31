USE ETL_1C_SQL;
BEGIN TRAN;
--------------------------------------------------------
---------------ЗАПРОС ABC&FMR - АНАЛИЗ------------------
--------------------------------------------------------
/*
TRUNCATE TABLE ##PRODAGY_PODGRUPPA;
TRUNCATE TABLE ##PRODAGY_CATEGORY;
TRUNCATE TABLE ##CHASTOTA_PODGRUPPA;
TRUNCATE TABLE ##CHASTOTA_CATEGORY;*/

DELETE FROM [Table_Отчет_ABC&FMR];

/*
SELECT TOP 1 * FROM ##PRODAGY_GRUPPA;
SELECT TOP 1 * FROM ##PRODAGY_PODGRUPPA;
SELECT TOP 1 * FROM ##PRODAGY_CATEGORY;
SELECT TOP 1 * FROM ##PRODAGY_BARCODE;
SELECT TOP 1 * FROM ##PRODAGY_BARCODE_TN_GRUPPA;
SELECT TOP 1 * FROM ##PRODAGY_BARCODE_TN_PODGRUPPA;
SELECT TOP 1 * FROM ##PRODAGY_BARCODE_TN_CATEGORY;

SELECT TOP 1 * FROM ##CHASTOTA_GRUPPA;
SELECT TOP 1 * FROM ##CHASTOTA_PODGRUPPA;
SELECT TOP 1 * FROM ##CHASTOTA_CATEGORY;
SELECT TOP 1 * FROM ##CHASTOTA_BARCODE;
SELECT TOP 1 * FROM ##CHASTOTA_BARCODE_TN_GRUPPA;
SELECT TOP 1 * FROM ##CHASTOTA_BARCODE_TN_PODGRUPPA;
SELECT TOP 1 * FROM ##CHASTOTA_BARCODE_TN_CATEGORY;
*/
DECLARE @YEAR_MM_A VARCHAR(7)
DECLARE @YEAR_MM_B VARCHAR(7)
DECLARE @YEARMM_1 VARCHAR(7)
DECLARE @YEARMM_2 VARCHAR(7)
DECLARE @YEARMM_3 VARCHAR(7)
DECLARE @YEARMM_4 VARCHAR(7)

-- ПРОДАЖИ ГОД.МЕСЯЦ НАЧАЛЬНАЯ ДАТА - ГОДОВЫЕ
SET @YEAR_MM_A = (SELECT TOP 1 REPLACE(CONVERT(VARCHAR(7),DATEADD(dd,-1,DATEADD(mm, DATEDIFF(mm,0,DATEADD(MM,-1,DATEADD(YY,-1,GETDATE())))+1,0)), 126), '-', '.'))
-- ПРОДАЖИ ГОД.МЕСЯЦ КОНЕЧНАЯ ДАТА - ГОДОВЫЕ
SET @YEAR_MM_B = (SELECT TOP 1 REPLACE(CONVERT(VARCHAR(7),DATEADD(dd,-1,DATEADD(mm, DATEDIFF(mm,0,DATEADD(MM,-1,GETDATE()))+1,0)), 126), '-', '.'))
-- ПРОДАЖИ ГОД.МЕСЯЦ - КВАРТАЛЬНЫЕ - ПОЛУГОДОВЫЕ
SET @YEARMM_1 = (SELECT TOP 1 REPLACE(CONVERT(VARCHAR(7),DATEADD(dd,-1,DATEADD(mm, DATEDIFF(mm,0,DATEADD(MM,-6,GETDATE()))+1,0)), 126), '-', '.'))
SET @YEARMM_2 = (SELECT TOP 1 REPLACE(CONVERT(VARCHAR(7),DATEADD(dd,-1,DATEADD(mm, DATEDIFF(mm,0,DATEADD(MM,-4,GETDATE()))+1,0)), 126), '-', '.'))
SET @YEARMM_3 = (SELECT TOP 1 REPLACE(CONVERT(VARCHAR(7),DATEADD(dd,-1,DATEADD(mm, DATEDIFF(mm,0,DATEADD(MM,-3,GETDATE()))+1,0)), 126), '-', '.'))
SET @YEARMM_4 = (SELECT TOP 1 REPLACE(CONVERT(VARCHAR(7),DATEADD(dd,-1,DATEADD(mm, DATEDIFF(mm,0,DATEADD(MM,-1,GETDATE()))+1,0)), 126), '-', '.'));

IF OBJECT_ID('[tempdb]..##PRODAGY_TN', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_TN;
IF OBJECT_ID('[tempdb]..##PRODAGY_GRUPPA', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_GRUPPA;
IF OBJECT_ID('[tempdb]..##PRODAGY_PODGRUPPA', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_PODGRUPPA;
IF OBJECT_ID('[tempdb]..##PRODAGY_CATEGORY', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_CATEGORY;
IF OBJECT_ID('[tempdb]..##PRODAGY_BARCODE', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_BARCODE;
IF OBJECT_ID('[tempdb]..[tempdb]..##PRODAGY_BARCODE_TN', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_BARCODE_TN;
IF OBJECT_ID('[tempdb]..##PRODAGY_BARCODE_TN_GRUPPA', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_BARCODE_TN_GRUPPA;
IF OBJECT_ID('[tempdb]..##PRODAGY_BARCODE_TN_PODGRUPPA', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_BARCODE_TN_PODGRUPPA;
IF OBJECT_ID('[tempdb]..##PRODAGY_BARCODE_TN_CATEGORY', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_BARCODE_TN_CATEGORY;
IF OBJECT_ID('[tempdb]..##CHASTOTA_TN', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_TN;
IF OBJECT_ID('[tempdb]..##CHASTOTA_GRUPPA', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_GRUPPA;
IF OBJECT_ID('[tempdb]..##CHASTOTA_PODGRUPPA', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_PODGRUPPA;
IF OBJECT_ID('[tempdb]..##CHASTOTA_CATEGORY', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_CATEGORY;
IF OBJECT_ID('[tempdb]..##CHASTOTA_BARCODE', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_BARCODE;
IF OBJECT_ID('[tempdb]..##CHASTOTA_BARCODE_TN', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_BARCODE_TN;
IF OBJECT_ID('[tempdb]..##CHASTOTA_BARCODE_TN_GRUPPA', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_BARCODE_TN_GRUPPA;
IF OBJECT_ID('[tempdb]..##CHASTOTA_BARCODE_TN_PODGRUPPA', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_BARCODE_TN_PODGRUPPA;
IF OBJECT_ID('[tempdb]..##CHASTOTA_BARCODE_TN_CATEGORY', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_BARCODE_TN_CATEGORY;
/*IF OBJECT_ID('ABC&FMR', 'U') IS NOT NULL
DROP TABLE [ABC&FMR];*/
IF OBJECT_ID('[tempdb]..##NOMENKLATURA', 'U') IS NOT NULL
DROP TABLE ##NOMENKLATURA;
IF OBJECT_ID('[tempdb]..##PRODAGY_III_QUARTER', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_III_QUARTER;
IF OBJECT_ID('[tempdb]..##PRODAGY_IV_QUARTER', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_IV_QUARTER;
IF OBJECT_ID('[tempdb]..##NOT_PRODAGY_THREE_MOUNTH', 'U') IS NOT NULL
DROP TABLE ##NOT_PRODAGY_THREE_MOUNTH;
IF OBJECT_ID('[tempdb]..##SREDNEVZVPRICE', 'U') IS NOT NULL
DROP TABLE ##SREDNEVZVPRICE;
IF OBJECT_ID('[tempdb]..##OSTATKI_SKLAD', 'U') IS NOT NULL
DROP TABLE ##OSTATKI_SKLAD;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_TN_BARCODE', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_TN_BARCODE;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_GRUPPA_BARCODE', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_GRUPPA_BARCODE;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_PODGRUPPA_BARCODE', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_PODGRUPPA_BARCODE;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_CATEGORY_BARCODE', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_CATEGORY_BARCODE;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_BARCODE', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_BARCODE;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_TN', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_TN;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_GRUPPA', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_GRUPPA;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_PODGRUPPA', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_PODGRUPPA;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_CATEGORY', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_CATEGORY;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_TN_BARCODE', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_TN_BARCODE;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_GRUPPA_BARCODE', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_GRUPPA_BARCODE;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_PODGRUPPA_BARCODE', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_PODGRUPPA_BARCODE;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_CATEGORY_BARCODE', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_CATEGORY_BARCODE;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_BARCODE', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_BARCODE;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_TN', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_TN;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_GRUPPA', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_GRUPPA;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_PODGRUPPA', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_PODGRUPPA;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_CATEGORY', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_CATEGORY;
IF OBJECT_ID('[tempdb]..##FUNCTIONAL_RA_TP', 'U') IS NOT NULL
DROP TABLE ##FUNCTIONAL_RA_TP;
IF OBJECT_ID('[tempdb]..##PRIHOD_DATA', 'U') IS NOT NULL
DROP TABLE ##PRIHOD_DATA;
IF OBJECT_ID('[tempdb]..##MINIMUMSTANDARTFILLING', 'U') IS NOT NULL
DROP TABLE ##MINIMUMSTANDARTFILLING;
----------------------------------------------------------
--														--
-------------/*ДАННЫЕ ДЛЯ ABC - АНАЛИЗА*/-----------------
--														--
----------------------------------------------------------

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ ABC - АНАЛИЗА ПО ПРОДАЖАМ В ГРУППЕРОВКЕ ПО БАРКОДУ В ГРУППЕ*/
SELECT 
T2.ТоварноеНаправление,
T1.НоменклатураАртикул,
SUM(T3.[Сумма приведен продаж]) AS [Сумма продаж в ценах закупки с НДС]
INTO ##PRODAGY_BARCODE_TN
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B)
GROUP BY 
T2.ТоварноеНаправление,
T1.НоменклатураАртикул;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ ABC - АНАЛИЗА ПО ПРОДАЖАМ В ГРУППЕРОВКЕ ПО БАРКОДУ В ГРУППЕ*/
SELECT 
T2.Группа,
T1.НоменклатураАртикул,
SUM(T3.[Сумма приведен продаж]) AS [Сумма продаж в ценах закупки с НДС]
INTO ##PRODAGY_BARCODE_TN_GRUPPA
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B)
GROUP BY 
T2.Группа,
T1.НоменклатураАртикул;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ ABC - АНАЛИЗА ПО ПРОДАЖАМ В ГРУППЕРОВКЕ ПО БАРКОДУ В ПОДГРУППЕ*/
SELECT 
T2.Подгруппа,
T1.НоменклатураАртикул,
SUM(T3.[Сумма приведен продаж]) AS [Сумма продаж в ценах закупки с НДС]
INTO ##PRODAGY_BARCODE_TN_PODGRUPPA
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B)
GROUP BY 
T2.Подгруппа,
T1.НоменклатураАртикул;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ ABC - АНАЛИЗА ПО ПРОДАЖАМ В ГРУППЕРОВКЕ ПО БАРКОДУ В КАТЕГОРИИ*/
SELECT 
T2.Категория,
T1.НоменклатураАртикул,
SUM(T3.[Сумма приведен продаж]) AS [Сумма продаж в ценах закупки с НДС]
INTO ##PRODAGY_BARCODE_TN_CATEGORY
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B)
GROUP BY 
T2.Категория,
T1.НоменклатураАртикул;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ ABC - АНАЛИЗА ПО ПРОДАЖАМ В ГРУППЕРОВКЕ ПО БАРКОДУ*/
SELECT 
T1.НоменклатураАртикул,
SUM(T3.[Сумма приведен продаж]) AS [Сумма продаж в ценах закупки с НДС]
INTO ##PRODAGY_BARCODE
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
--T2.ТоварноеНаправление IN ('1 ЭЛО','2 КПП','3 НВО','4 СВТ','Прочее','Прочие') AND 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B)
GROUP BY T1.НоменклатураАртикул;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ ABC - АНАЛИЗА ПО ПРОДАЖАМ В ГРУППЕРОВКЕ ПО ТН*/
SELECT 
T2.ТоварноеНаправление,
SUM(T3.[Сумма приведен продаж]) AS [Сумма продаж в ценах закупки с НДС]
INTO ##PRODAGY_TN
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
--T2.ТоварноеНаправление IN ('1 ЭЛО','2 КПП','3 НВО','4 СВТ','Прочее','Прочие') AND 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B) 
GROUP BY T2.ТоварноеНаправление;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ ABC - АНАЛИЗА ПО ПРОДАЖАМ В ГРУППЕРОВКЕ ПО ГРУППЕ*/
SELECT 
T2.Группа,
SUM(T3.[Сумма приведен продаж]) AS [Сумма продаж в ценах закупки с НДС]
INTO ##PRODAGY_GRUPPA
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
--T2.ТоварноеНаправление IN ('1 ЭЛО','2 КПП','3 НВО','4 СВТ','Прочее','Прочие') AND 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B) 
GROUP BY T2.Группа;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ ABC - АНАЛИЗА ПО ПРОДАЖАМ В ГРУППЕРОВКЕ ПО ПОДГРУППЕ*/
SELECT 
T2.Подгруппа,
SUM(T3.[Сумма приведен продаж]) AS [Сумма продаж в ценах закупки с НДС]
INTO ##PRODAGY_PODGRUPPA
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
--T2.ТоварноеНаправление IN ('1 ЭЛО','2 КПП','3 НВО','4 СВТ','Прочее','Прочие') AND 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B) 
GROUP BY T2.Подгруппа;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ ABC - АНАЛИЗА ПО ПРОДАЖАМ В ГРУППЕРОВКЕ ПО КАТЕГОРИИ*/
SELECT 
T2.Категория,
SUM(T3.[Сумма приведен продаж]) AS [Сумма продаж в ценах закупки с НДС]
INTO ##PRODAGY_CATEGORY
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK)  ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
--T2.ТоварноеНаправление IN ('1 ЭЛО','2 КПП','3 НВО','4 СВТ','Прочее','Прочие') AND 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B) 
GROUP BY T2.Категория;
----------------------------------------------------------
--														--
-------------/*ДАННЫЕ ДЛЯ FMR - АНАЛИЗА*/-----------------
--														--
----------------------------------------------------------

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ FMR - АНАЛИЗА ПО ЧАСТОТЕ В ГРУППЕРОВКЕ ПО БАРКОДУ В ГРУППЕ*/
SELECT 
T2.ТоварноеНаправление,
T1.НоменклатураАртикул,
sum(T3.Частота) AS [Частота продаж]
INTO ##CHASTOTA_BARCODE_TN
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
--T2.ТоварноеНаправление IN ('1 ЭЛО','2 КПП','3 НВО','4 СВТ','Прочее','Прочие') AND 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B)
GROUP BY 
T2.ТоварноеНаправление,
T1.НоменклатураАртикул;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ FMR - АНАЛИЗА ПО ЧАСТОТЕ В ГРУППЕРОВКЕ ПО БАРКОДУ В ГРУППЕ*/
SELECT 
T2.Группа,
T1.НоменклатураАртикул,
sum(T3.Частота) AS [Частота продаж]
INTO ##CHASTOTA_BARCODE_TN_GRUPPA
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
--T2.ТоварноеНаправление IN ('1 ЭЛО','2 КПП','3 НВО','4 СВТ','Прочее','Прочие') AND 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B)
GROUP BY 
T2.Группа,
T1.НоменклатураАртикул;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ FMR - АНАЛИЗА ПО ЧАСТОТЕ В ГРУППЕРОВКЕ ПО БАРКОДУ В ПОДГРУППЕ*/
SELECT 
T2.Подгруппа,
T1.НоменклатураАртикул,
sum(T3.Частота) AS [Частота продаж]
INTO ##CHASTOTA_BARCODE_TN_PODGRUPPA
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
--T2.ТоварноеНаправление IN ('1 ЭЛО','2 КПП','3 НВО','4 СВТ','Прочее','Прочие') AND 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B)
GROUP BY 
T2.Подгруппа,
T1.НоменклатураАртикул;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ FMR - АНАЛИЗА ПО ЧАСТОТЕ В ГРУППЕРОВКЕ ПО БАРКОДУ В КАТЕГОРИИ*/
SELECT 
T2.Категория,
T1.НоменклатураАртикул,
sum(T3.Частота) AS [Частота продаж]
INTO ##CHASTOTA_BARCODE_TN_CATEGORY
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
--T2.ТоварноеНаправление IN ('1 ЭЛО','2 КПП','3 НВО','4 СВТ','Прочее','Прочие') AND 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B)
GROUP BY 
T2.Категория,
T1.НоменклатураАртикул;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ FMR - АНАЛИЗА ПО ЧАСТОТЕ В ГРУППЕРОВКЕ ПО БАРКОДУ*/
SELECT 
T1.НоменклатураАртикул,
sum(T3.Частота) AS [Частота продаж]
INTO ##CHASTOTA_BARCODE
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
--T2.ТоварноеНаправление IN ('1 ЭЛО','2 КПП','3 НВО','4 СВТ','Прочее','Прочие') AND 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B)
GROUP BY T1.НоменклатураАртикул;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ FMR - АНАЛИЗА ПО ЧАСТОТЕ В ГРУППЕРОВКЕ ПО ТН*/
SELECT 
T2.ТоварноеНаправление,
sum(T3.Частота) AS [Частота продаж]
INTO ##CHASTOTA_TN
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
--T2.ТоварноеНаправление IN ('1 ЭЛО','2 КПП','3 НВО','4 СВТ','Прочее','Прочие') AND 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B) 
GROUP BY T2.ТоварноеНаправление;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ FMR - АНАЛИЗА ПО ЧАСТОТЕ В ГРУППЕРОВКЕ ПО ГРУППЕ*/
SELECT 
T2.Группа,
sum(T3.Частота) AS [Частота продаж]
INTO ##CHASTOTA_GRUPPA
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
--T2.ТоварноеНаправление IN ('1 ЭЛО','2 КПП','3 НВО','4 СВТ','Прочее','Прочие') AND 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B) 
GROUP BY T2.Группа;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ FMR - АНАЛИЗА ПО ЧАСТОТЕ В ГРУППЕРОВКЕ ПО ПОДГРУППЕ*/
SELECT 
T2.Подгруппа,
sum(T3.Частота) AS [Частота продаж]
INTO ##CHASTOTA_PODGRUPPA
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
--T2.ТоварноеНаправление IN ('1 ЭЛО','2 КПП','3 НВО','4 СВТ','Прочее','Прочие') AND 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B) 
GROUP BY T2.Подгруппа;

/*ЗАПРОС НА ВЫБОРКУ ДАННЫХ ДЛЯ FMR - АНАЛИЗА ПО ЧАСТОТЕ В ГРУППЕРОВКЕ ПО КАТЕГОРИИ*/
SELECT 
T2.Категория,
sum(T3.Частота) AS [Частота продаж]
INTO ##CHASTOTA_CATEGORY
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE 
--T2.ТоварноеНаправление IN ('1 ЭЛО','2 КПП','3 НВО','4 СВТ','Прочее','Прочие') AND 
(T3.Period BETWEEN @YEAR_MM_A AND @YEAR_MM_B)
GROUP BY T2.Категория;

-------------------------------------------------
/*ОСНОВНОЙ ЗАПРОС*/
-------------------------------------------------
/*ЗАПРОС ДАННЫЕ ПО НОМЕКЛАТУРАМ И ГРУППАМ*/
SELECT
T2.ТоварноеНаправление, 
T2.Группа, 
T2.Подгруппа, 
T2.Категория, 
T1.Номенклатура, 
T1.НоменклатураВЗТНаименование AS [Наименование группы ВЗТ], 
T1.[НоменклатураОсновной статус товара], 
T1.[НоменклатураВЗТОсновной статус товара], 
T1.НоменклатураАртикул AS Баркод,
T1.[НоменклатураМ ЖЦ] AS ЖЦ,
T1.НаРеализации AS Реализация,
T1.КратностьУпаковки AS [Кратность Упаковки],
T1.[НоменклатураБазовая единица измерения] AS [Базовая единица измерения]
INTO ##NOMENKLATURA
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код;
     
/*Нормативы Минимального Наполнения*/
SELECT DISTINCT 
  [Артикул]
 ,[ТочкаЗакза]
 ,[Максимум]
INTO ##MINIMUMSTANDARTFILLING 
FROM [MINIMAKS_SQL].[dbo].[рег_НормТЗ_Нормативы_Минимального_Наполнения];

/*ПЕРВАЯ И ПОСЛЕДНЯЯ ДАТА ПРИХОДА В РАЗРЕЗЕ БАРКОДА*/
SELECT DISTINCT 
	   [БАРКОД],
	   MIN(CAST([ДАТА_ПРИХО] AS DATE)) AS [ПЕРВАЯ ДАТА ПОСТУПЛЕНИЯ],
	   MAX(CAST([ДАТА_ПРИХО] AS DATE)) AS [ПОСЛЕДНЯЯ ДАТА ПОСТУПЛЕНИЯ]
INTO ##PRIHOD_DATA
FROM [MINIMAKS_SQL].[dbo].[Prihod]
GROUP BY [БАРКОД];

/*НАЛИЧИЕ БАРКОДА В РА*/
SELECT DISTINCT 
T.Баркод,
SUM(CASE WHEN T.[Филиал] IN ('Актобе') THEN 1 ELSE 0 END) AS [ПФО_Актобе],
SUM(CASE WHEN T.[Филиал] IN ('Казань') THEN 1 ELSE 0 END) AS [ПФО_Казань],
SUM(CASE WHEN T.[Филиал] IN ('Н.Новгород') THEN 1 ELSE 0 END) AS [ПФО_Н.Новгород],
SUM(CASE WHEN T.[Филиал] IN ('Н.Новгород Веденяпина') THEN 1 ELSE 0 END) AS [ПФО_Н.Новгород Веденяпина],
SUM(CASE WHEN T.[Филиал] IN ('Оренбург') THEN 1 ELSE 0 END) AS [ПФО_Оренбург],
SUM(CASE WHEN T.[Филиал] IN ('Оренбург_авт') THEN 1 ELSE 0 END) AS [ПФО_Оренбург_авт],
SUM(CASE WHEN T.[Филиал] IN ('Пенза') THEN 1 ELSE 0 END) AS [ПФО_Пенза],
SUM(CASE WHEN T.[Филиал] IN ('Самара') THEN 1 ELSE 0 END) AS [ПФО_Самара],
SUM(CASE WHEN T.[Филиал] IN ('Самара_Кир') THEN 1 ELSE 0 END) AS [ПФО_Самара_Кир],
SUM(CASE WHEN T.[Филиал] IN ('Самара_Красн') THEN 1 ELSE 0 END) AS [ПФО_Самара_Красн],
SUM(CASE WHEN T.[Филиал] IN ('Самара_УТ') THEN 1 ELSE 0 END) AS [ПФО_Самара_УТ],
SUM(CASE WHEN T.[Филиал] IN ('Саратов') THEN 1 ELSE 0 END) AS [ПФО_Саратов],
SUM(CASE WHEN T.[Филиал] IN ('Тольятти') THEN 1 ELSE 0 END) AS [ПФО_Тольятти],
SUM(CASE WHEN T.[Филиал] IN ('Тольятти_Громова') THEN 1 ELSE 0 END) AS [ПФО_Тольятти_Громова],
SUM(CASE WHEN T.[Филиал] IN ('Ульяновск') THEN 1 ELSE 0 END) AS [ПФО_Ульяновск],
SUM(CASE WHEN T.[Филиал] IN ('Уфа') THEN 1 ELSE 0 END) AS [ПФО_Уфа],
SUM(CASE WHEN T.[Филиал] IN ('Уфа Кавказская') THEN 1 ELSE 0 END) AS [ПФО_Уфа Кавказская],
SUM(CASE WHEN T.[Филиал] IN ('Чебоксары') THEN 1 ELSE 0 END) AS [ПФО_Чебоксары],
SUM(CASE WHEN T.[Филиал] IN ('Энгельс') THEN 1 ELSE 0 END) AS [ПФО_Энгельс],
SUM(CASE WHEN T.[Филиал] IN ('LVS Горелово-16 (с 18.04)') THEN 1 ELSE 0 END) AS [СЗФО_LVS Горелово-16 (с 18.04)],
SUM(CASE WHEN T.[Филиал] IN ('Архангельск') THEN 1 ELSE 0 END) AS [СЗФО_Архангельск],
SUM(CASE WHEN T.[Филиал] IN ('Б.Сампсониевский') THEN 1 ELSE 0 END) AS [СЗФО_Б.Сампсониевский],
SUM(CASE WHEN T.[Филиал] IN ('Бабушкина') THEN 1 ELSE 0 END) AS [СЗФО_Бабушкина],
SUM(CASE WHEN T.[Филиал] IN ('Большевиков') THEN 1 ELSE 0 END) AS [СЗФО_Большевиков],
SUM(CASE WHEN T.[Филиал] IN ('Будапештская') THEN 1 ELSE 0 END) AS [СЗФО_Будапештская],
SUM(CASE WHEN T.[Филиал] IN ('Великие Луки') THEN 1 ELSE 0 END) AS [СЗФО_Великие Луки],
SUM(CASE WHEN T.[Филиал] IN ('ВО') THEN 1 ELSE 0 END) AS [СЗФО_ВО],
SUM(CASE WHEN T.[Филиал] IN ('Вологда') THEN 1 ELSE 0 END) AS [СЗФО_Вологда],
SUM(CASE WHEN T.[Филиал] IN ('Гатчина') THEN 1 ELSE 0 END) AS [СЗФО_Гатчина],
SUM(CASE WHEN T.[Филиал] IN ('Е12') THEN 1 ELSE 0 END) AS [СЗФО_Е12],
SUM(CASE WHEN T.[Филиал] IN ('Кингисепп') THEN 1 ELSE 0 END) AS [СЗФО_Кингисепп],
SUM(CASE WHEN T.[Филиал] IN ('Колпино') THEN 1 ELSE 0 END) AS [СЗФО_Колпино],
SUM(CASE WHEN T.[Филиал] IN ('Коми') THEN 1 ELSE 0 END) AS [СЗФО_Коми],
SUM(CASE WHEN T.[Филиал] IN ('Косыгина') THEN 1 ELSE 0 END) AS [СЗФО_Косыгина],
SUM(CASE WHEN T.[Филиал] IN ('Котлас') THEN 1 ELSE 0 END) AS [СЗФО_Котлас],
SUM(CASE WHEN T.[Филиал] IN ('Л260') THEN 1 ELSE 0 END) AS [СЗФО_Л260],
SUM(CASE WHEN T.[Филиал] IN ('Ленинский') THEN 1 ELSE 0 END) AS [СЗФО_Ленинский],
SUM(CASE WHEN T.[Филиал] IN ('Мурманск') THEN 1 ELSE 0 END) AS [СЗФО_Мурманск],
SUM(CASE WHEN T.[Филиал] IN ('Науки') THEN 1 ELSE 0 END) AS [СЗФО_Науки],
SUM(CASE WHEN T.[Филиал] IN ('Новгород') THEN 1 ELSE 0 END) AS [СЗФО_Новгород],
SUM(CASE WHEN T.[Филиал] IN ('Обводный') THEN 1 ELSE 0 END) AS [СЗФО_Обводный],
SUM(CASE WHEN T.[Филиал] IN ('П59') THEN 1 ELSE 0 END) AS [СЗФО_П59],
SUM(CASE WHEN T.[Филиал] IN ('Петрозаводск') THEN 1 ELSE 0 END) AS [СЗФО_Петрозаводск],
SUM(CASE WHEN T.[Филиал] IN ('Псков') THEN 1 ELSE 0 END) AS [СЗФО_Псков],
SUM(CASE WHEN T.[Филиал] IN ('Рижский') THEN 1 ELSE 0 END) AS [СЗФО_Рижский],
SUM(CASE WHEN T.[Филиал] IN ('СБ') THEN 1 ELSE 0 END) AS [СЗФО_СБ],
SUM(CASE WHEN T.[Филиал] IN ('Среднеохтинский') THEN 1 ELSE 0 END) AS [СЗФО_Среднеохтинский],
SUM(CASE WHEN T.[Филиал] IN ('Стачек') THEN 1 ELSE 0 END) AS [СЗФО_Стачек],
SUM(CASE WHEN T.[Филиал] IN ('Сытный') THEN 1 ELSE 0 END) AS [СЗФО_Сытный],
SUM(CASE WHEN T.[Филиал] IN ('Чкаловский') THEN 1 ELSE 0 END) AS [СЗФО_Чкаловский],
SUM(CASE WHEN T.[Филиал] IN ('Школьная') THEN 1 ELSE 0 END) AS [СЗФО_Школьная],
SUM(CASE WHEN T.[Филиал] IN ('Электролайт') THEN 1 ELSE 0 END) AS [СЗФО_Электролайт],
SUM(CASE WHEN T.[Филиал] IN ('Энгельса') THEN 1 ELSE 0 END) AS [СЗФО_Энгельса],
SUM(CASE WHEN T.[Филиал] IN ('Кемерово') THEN 1 ELSE 0 END) AS [СФО_Кемерово],
SUM(CASE WHEN T.[Филиал] IN ('Новосибирск') THEN 1 ELSE 0 END) AS [СФО_Новосибирск],
SUM(CASE WHEN T.[Филиал] IN ('Новосибирск_Ватутина') THEN 1 ELSE 0 END) AS [СФО_Новосибирск_Ватутина],
SUM(CASE WHEN T.[Филиал] IN ('Новосибирск_Зыряновское') THEN 1 ELSE 0 END) AS [СФО_Новосибирск_Зыряновское],
SUM(CASE WHEN T.[Филиал] IN ('Новосибирск_Сибиряков') THEN 1 ELSE 0 END) AS [СФО_Новосибирск_Сибиряков],
SUM(CASE WHEN T.[Филиал] IN ('Екатеринбург_Блюхера') THEN 1 ELSE 0 END) AS [УФО_Екатеринбург_Блюхера],
SUM(CASE WHEN T.[Филиал] IN ('Екатеринбург_Мет') THEN 1 ELSE 0 END) AS [УФО_Екатеринбург_Мет],
SUM(CASE WHEN T.[Филиал] IN ('Екатеринбург_Тит') THEN 1 ELSE 0 END) AS [УФО_Екатеринбург_Тит],
SUM(CASE WHEN T.[Филиал] IN ('Екатеринбург_Шаумяна') THEN 1 ELSE 0 END) AS [УФО_Екатеринбург_Шаумяна],
SUM(CASE WHEN T.[Филиал] IN ('Магнитогорск') THEN 1 ELSE 0 END) AS [УФО_Магнитогорск],
SUM(CASE WHEN T.[Филиал] IN ('Миасс') THEN 1 ELSE 0 END) AS [УФО_Миасс],
SUM(CASE WHEN T.[Филиал] IN ('Пермь') THEN 1 ELSE 0 END) AS [УФО_Пермь],
SUM(CASE WHEN T.[Филиал] IN ('Тюмень') THEN 1 ELSE 0 END) AS [УФО_Тюмень],
SUM(CASE WHEN T.[Филиал] IN ('Челябинск') THEN 1 ELSE 0 END) AS [УФО_Челябинск],
SUM(CASE WHEN T.[Филиал] IN ('Челябинск_Кулиб') THEN 1 ELSE 0 END) AS [УФО_Челябинск_Кулиб],
SUM(CASE WHEN T.[Филиал] IN ('Челябинск_Поб') THEN 1 ELSE 0 END) AS [УФО_Челябинск_Поб],
SUM(CASE WHEN T.[Филиал] IN ('Челябинск_Свердловский') THEN 1 ELSE 0 END) AS [УФО_Челябинск_Свердловский],
SUM(CASE WHEN T.[Филиал] IN ('Армавир') THEN 1 ELSE 0 END) AS [ЮФО_Армавир],
SUM(CASE WHEN T.[Филиал] IN ('Астрахань') THEN 1 ELSE 0 END) AS [ЮФО_Астрахань],
SUM(CASE WHEN T.[Филиал] IN ('Волгоград') THEN 1 ELSE 0 END) AS [ЮФО_Волгоград],
SUM(CASE WHEN T.[Филиал] IN ('Волгодонск') THEN 1 ELSE 0 END) AS [ЮФО_Волгодонск],
SUM(CASE WHEN T.[Филиал] IN ('Воронеж') THEN 1 ELSE 0 END) AS [ЮФО_Воронеж],
SUM(CASE WHEN T.[Филиал] IN ('Краснодар') THEN 1 ELSE 0 END) AS [ЮФО_Краснодар],
SUM(CASE WHEN T.[Филиал] IN ('Краснодар_Рос') THEN 1 ELSE 0 END) AS [ЮФО_Краснодар_Рос],
SUM(CASE WHEN T.[Филиал] IN ('Липецк') THEN 1 ELSE 0 END) AS [ЮФО_Липецк],
SUM(CASE WHEN T.[Филиал] IN ('Пятигорск') THEN 1 ELSE 0 END) AS [ЮФО_Пятигорск],
SUM(CASE WHEN T.[Филиал] IN ('Ростов') THEN 1 ELSE 0 END) AS [ЮФО_Ростов],
SUM(CASE WHEN T.[Филиал] IN ('Ростов_Вят') THEN 1 ELSE 0 END) AS [ЮФО_Ростов_Вят],
SUM(CASE WHEN T.[Филиал] IN ('Сочи') THEN 1 ELSE 0 END) AS [ЮФО_Сочи]
INTO ##FUNCTIONAL_RA_TP
FROM [MINIMAKS_SQL].[dbo].[ФункционалРА] AS T
LEFT JOIN [MINIMAKS_SQL].[dbo].Подразделения AS T2 ON (T2.Филиал = T.Филиал) 
GROUP BY T.Баркод;

/*СРЕДНЕВЗВЕШЕННАЯ ЦЕНА*/
SELECT DISTINCT
T1.Баркод,
T1.Период AS ДАТА_СРЕД_ВЗВ_ЦЕНЫ,
MAX(T1.СрВзвЦена) AS СРЕД_ВЗВ_ЦЕНА
INTO ##SREDNEVZVPRICE
FROM MINIMAKS_SQL.dbo.SredVzvPrice AS T1 WITH (NOLOCK)
WHERE T1.Период = (SELECT DISTINCT MAX(T2.Период) 
                   FROM MINIMAKS_SQL.dbo.SredVzvPrice AS T2 WITH (NOLOCK) 
                   WHERE T1.Баркод = T2.Баркод GROUP BY T2.Баркод)
GROUP BY 
T1.Баркод,
T1.Период;

/*ТОВАРЫ БЕЗ ДВИЖЕНИЯ БОЛЕЕ 3 МЕСЯЦЕВ*/
SELECT 
T1.Баркод,
SUM(T1.Количество) AS [Сумма_кол-во_без_движения_3_месяца]
INTO ##NOT_PRODAGY_THREE_MOUNTH
FROM MINIMAKS_SQL.dbo.Товар_без_движения_3_месяца AS T1 WITH (NOLOCK)
WHERE T1.Дата_занесения = (SELECT MAX(T1.Дата_занесения) FROM MINIMAKS_SQL.dbo.Товар_без_движения_3_месяца AS T1 WITH (NOLOCK))
GROUP BY T1.Баркод;

/*ОСТАТКИ НА СКЛАДАХ*/
SELECT T1.Баркод, 
MAX(T1.ДАТА) AS ДАТА, 
SUM(T1.Количество) AS Количество
INTO ##OSTATKI_SKLAD
FROM MINIMAKS_SQL.dbo.OstatkiFil AS T1 WITH (NOLOCK) 
WHERE T1.ДАТА = (SELECT MAX(T1.ДАТА) FROM MINIMAKS_SQL.dbo.OstatkiFil AS T1 WITH (NOLOCK) )
GROUP BY T1.Баркод;

/*ПРОДАЖИ ПО КВАРТАЛЬНО*/
SELECT 
T1.НоменклатураАртикул AS Баркод, 
SUM(T3.[Сумма приведен продаж]) AS [Сумма продаж в ценах закупки с НДС_III_квартал]
INTO ##PRODAGY_III_QUARTER
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE (T3.Period BETWEEN @YEARMM_1 AND @YEARMM_2)
GROUP BY T1.НоменклатураАртикул;

/*ПРОДАЖИ ПО КВАРТАЛЬНО*/
SELECT 
T1.НоменклатураАртикул AS Баркод, 
SUM(T3.[Сумма приведен продаж]) AS [Сумма продаж в ценах закупки с НДС_IV_квартал]
INTO ##PRODAGY_IV_QUARTER
FROM MINIMAKS_SQL.dbo.Функционал_УТЗ_Маркетинг AS T1 WITH (NOLOCK) LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Измерение_НоменклатурныеГруппыМаркетинг AS T2 WITH (NOLOCK) ON T1.КодРодителя = T2.Код LEFT OUTER JOIN
     MINIMAKS_SQL.dbo.Таблица_исходных_данных AS T3 WITH (NOLOCK) ON T1.НоменклатураАртикул = T3.БАРКОД
WHERE (T3.Period BETWEEN @YEARMM_3 AND @YEARMM_4)
GROUP BY T1.НоменклатураАртикул;

----------------------------------------------------------
--														--
---------------------/*ABC - АНАЛИЗ*/---------------------
--														--
----------------------------------------------------------

/*АНАЛИЗ - ABC, ПРОДАЖИ, ГРУППА, БАРКОД*/
SELECT DISTINCT 
                PRODAGY_BARCODE.ТоварноеНаправление AS ТоварноеНаправление
               ,PRODAGY_BARCODE.НоменклатураАртикул AS Баркод
               ,PRODAGY_BARCODE.[Сумма продаж в ценах закупки с НДС]
               ,CAST(PRODAGY_BARCODE.[Сумма продаж в ценах закупки с НДС]*1./a*100 as decimal(10,2)) AS PERSENT
               ,CAST(b*1./a*100 as decimal(10,2)) AS [PERSENT_SUM] 
,case 
when cast(b*1./a*100 as decimal(10,2)) between 0 and 80 then 'A'
when cast(b*1./a*100 as decimal(10,2)) between 80 and 95 then 'B'
when cast(b*1./a*100 as decimal(10,2)) between 95 and 99 then 'C'
when cast(b*1./a*100 as decimal(10,2)) between 99 and 100 then 'D'
else 'X'
end [ABC]
INTO ##ABC_PRODAGY_TN_BARCODE
from ##PRODAGY_BARCODE_TN AS PRODAGY_BARCODE WITH (NOLOCK)
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) a from ##PRODAGY_BARCODE_TN WITH (NOLOCK) 
             WHERE ТоварноеНаправление = PRODAGY_BARCODE.ТоварноеНаправление) t1
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) b from ##PRODAGY_BARCODE_TN WITH (NOLOCK) 
             where [Сумма продаж в ценах закупки с НДС] >= PRODAGY_BARCODE.[Сумма продаж в ценах закупки с НДС] 
              AND ТоварноеНаправление = PRODAGY_BARCODE.ТоварноеНаправление) t2
where a <> 0 and b <> 0 and [Сумма продаж в ценах закупки с НДС] <> 0;

/*АНАЛИЗ - ABC, ПРОДАЖИ, ГРУППА, БАРКОД*/
SELECT DISTINCT 
                PRODAGY_BARCODE.ГРУППА AS ГРУППА
               ,PRODAGY_BARCODE.НоменклатураАртикул AS Баркод
               ,PRODAGY_BARCODE.[Сумма продаж в ценах закупки с НДС]
               ,CAST(PRODAGY_BARCODE.[Сумма продаж в ценах закупки с НДС]*1./a*100 as decimal(10,2)) AS PERSENT
               ,CAST(b*1./a*100 as decimal(10,2)) AS [PERSENT_SUM] 
,case 
when cast(b*1./a*100 as decimal(10,2)) between 0 and 80 then 'A'
when cast(b*1./a*100 as decimal(10,2)) between 80 and 95 then 'B'
when cast(b*1./a*100 as decimal(10,2)) between 95 and 99 then 'C'
when cast(b*1./a*100 as decimal(10,2)) between 99 and 100 then 'D'
else 'X'
end [ABC]
INTO ##ABC_PRODAGY_GRUPPA_BARCODE
from ##PRODAGY_BARCODE_TN_GRUPPA AS PRODAGY_BARCODE WITH (NOLOCK)
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) a from ##PRODAGY_BARCODE_TN_GRUPPA WITH (NOLOCK) 
             WHERE ГРУППА = PRODAGY_BARCODE.ГРУППА) t1
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) b from ##PRODAGY_BARCODE_TN_GRUPPA WITH (NOLOCK) 
             where [Сумма продаж в ценах закупки с НДС] >= PRODAGY_BARCODE.[Сумма продаж в ценах закупки с НДС] 
              AND ГРУППА = PRODAGY_BARCODE.ГРУППА) t2
where a <> 0 and b <> 0 and [Сумма продаж в ценах закупки с НДС] <> 0;

/*АНАЛИЗ - ABC, ПРОДАЖИ, ГРУППА, ПОДГРУППА, БАРКОД*/
SELECT DISTINCT 
			    PRODAGY_BARCODE.ПОДГРУППА AS ПОДГРУППА
               ,PRODAGY_BARCODE.НоменклатураАртикул AS Баркод
               ,PRODAGY_BARCODE.[Сумма продаж в ценах закупки с НДС]
               ,CAST(PRODAGY_BARCODE.[Сумма продаж в ценах закупки с НДС]*1./a * 100 as decimal(10,2)) AS PERSENT
               ,CAST( b*1./a * 100 as decimal(10,2)) AS [PERSENT_SUM] 
,case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'A'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'B'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'C'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'D'
else 'X'
end [ABC]
INTO ##ABC_PRODAGY_PODGRUPPA_BARCODE
from ##PRODAGY_BARCODE_TN_PODGRUPPA AS PRODAGY_BARCODE WITH (NOLOCK)
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) a from ##PRODAGY_BARCODE_TN_PODGRUPPA WITH (NOLOCK) 
                        WHERE ПОДГРУППА = PRODAGY_BARCODE.ПОДГРУППА 
                        HAVING sum([Сумма продаж в ценах закупки с НДС]) > 0) t1
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) b from ##PRODAGY_BARCODE_TN_PODGRUPPA WITH (NOLOCK) 
                        WHERE [Сумма продаж в ценах закупки с НДС] >= 
                        PRODAGY_BARCODE.[Сумма продаж в ценах закупки с НДС] AND 
			            ПОДГРУППА = PRODAGY_BARCODE.ПОДГРУППА
			            /*HAVING sum([Сумма продаж в ценах закупки с НДС]) > 0*/) t2
where a <> 0 and b <> 0 and [Сумма продаж в ценах закупки с НДС] <> 0;

/*АНАЛИЗ - ABC, ПРОДАЖИ, ГРУППА, ПОДГРУППА, КАТЕГОРИЯ, БАРКОД*/
SELECT DISTINCT 
 			    PRODAGY_BARCODE.КАТЕГОРИЯ AS КАТЕГОРИЯ
               ,PRODAGY_BARCODE.НоменклатураАртикул AS Баркод
               ,PRODAGY_BARCODE.[Сумма продаж в ценах закупки с НДС]
               ,CAST(PRODAGY_BARCODE.[Сумма продаж в ценах закупки с НДС]*1./a * 100 as decimal(10,2)) AS PERSENT
               ,CAST( b*1./a * 100 as decimal(10,2)) AS [PERSENT_SUM] 
,case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'A'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'B'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'C'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'D'
else 'X'
end [ABC]
INTO ##ABC_PRODAGY_CATEGORY_BARCODE
from ##PRODAGY_BARCODE_TN_CATEGORY AS PRODAGY_BARCODE WITH (NOLOCK)
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) a from ##PRODAGY_BARCODE_TN_CATEGORY WITH (NOLOCK) 
                        WHERE КАТЕГОРИЯ = PRODAGY_BARCODE.КАТЕГОРИЯ
                        HAVING sum([Сумма продаж в ценах закупки с НДС]) > 0) t1
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) b from ##PRODAGY_BARCODE_TN_CATEGORY WITH (NOLOCK) where 
			            [Сумма продаж в ценах закупки с НДС] >= 
			            PRODAGY_BARCODE.[Сумма продаж в ценах закупки с НДС]
			            AND КАТЕГОРИЯ = PRODAGY_BARCODE.КАТЕГОРИЯ 
			            /*HAVING sum([Сумма продаж в ценах закупки с НДС]) > 0*/) t2
where a <> 0 and b <> 0 and [Сумма продаж в ценах закупки с НДС] <> 0;

/*АНАЛИЗ - ABC, ПРОДАЖИ, БАРКОД*/
SELECT  PRODAGY_BARCODE.НоменклатураАртикул AS Баркод
       ,PRODAGY_BARCODE.[Сумма продаж в ценах закупки с НДС]
       ,CAST(PRODAGY_BARCODE.[Сумма продаж в ценах закупки с НДС]*1./a * 100 as decimal(10,2)) AS PERSENT
       ,CAST( b*1./a * 100 as decimal(10,2)) AS [PERSENT_SUM], 
case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'A'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'B'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'C'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'D'
else 'X'
end [ABC]
INTO ##ABC_PRODAGY_BARCODE
from ##PRODAGY_BARCODE AS PRODAGY_BARCODE WITH (NOLOCK)
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) a from ##PRODAGY_BARCODE WITH (NOLOCK)) t1
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) b from ##PRODAGY_BARCODE WITH (NOLOCK) where 
			            [Сумма продаж в ценах закупки с НДС] >= 
			            PRODAGY_BARCODE.[Сумма продаж в ценах закупки с НДС]) t2
where a <> 0 and b <> 0 and [Сумма продаж в ценах закупки с НДС] <> 0;
/*АНАЛИЗ - ABC, ПРОДАЖИ, ТН*/
SELECT  PRODAGY_TN.ТОВАРНОЕНАПРАВЛЕНИЕ AS ТОВАРНОЕНАПРАВЛЕНИЕ
       ,PRODAGY_TN.[Сумма продаж в ценах закупки с НДС]
       ,CAST(PRODAGY_TN.[Сумма продаж в ценах закупки с НДС]*1./a * 100 as decimal(10,2)) AS PERSENT
       ,CAST( b*1./a * 100 as decimal(10,2)) AS [PERSENT_SUM], 
case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'A'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'B'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'C'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'D'
else 'X'
end [ABC]
INTO ##ABC_PRODAGY_TN
from ##PRODAGY_TN AS PRODAGY_TN WITH (NOLOCK)
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) a from ##PRODAGY_TN WITH (NOLOCK)) t1
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) b from ##PRODAGY_TN WITH (NOLOCK) where 
			            [Сумма продаж в ценах закупки с НДС] >= 
			            PRODAGY_TN.[Сумма продаж в ценах закупки с НДС]) t2
where a <> 0 and b <> 0 and [Сумма продаж в ценах закупки с НДС] <> 0;

/*АНАЛИЗ - ABC, ПРОДАЖИ, ГРУППА*/
SELECT  PRODAGY_GRUPPA.Группа AS Группа
       ,PRODAGY_GRUPPA.[Сумма продаж в ценах закупки с НДС]
       ,CAST(PRODAGY_GRUPPA.[Сумма продаж в ценах закупки с НДС]*1./a * 100 as decimal(10,2)) AS PERSENT
       ,CAST( b*1./a * 100 as decimal(10,2)) AS [PERSENT_SUM], 
case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'A'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'B'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'C'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'D'
else 'X'
end [ABC]
INTO ##ABC_PRODAGY_GRUPPA
from ##PRODAGY_GRUPPA AS PRODAGY_GRUPPA WITH (NOLOCK)
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) a from ##PRODAGY_GRUPPA WITH (NOLOCK)) t1
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) b from ##PRODAGY_GRUPPA WITH (NOLOCK) where 
			            [Сумма продаж в ценах закупки с НДС] >= 
			            PRODAGY_GRUPPA.[Сумма продаж в ценах закупки с НДС]) t2
where a <> 0 and b <> 0 and [Сумма продаж в ценах закупки с НДС] <> 0;

/*АНАЛИЗ - ABC, ПРОДАЖИ, ПОДГРУППА*/
SELECT  PRODAGY_PODGRUPPA.Подгруппа AS Подгруппа
       ,PRODAGY_PODGRUPPA.[Сумма продаж в ценах закупки с НДС]
       ,CAST(PRODAGY_PODGRUPPA.[Сумма продаж в ценах закупки с НДС]*1./a * 100 as decimal(10,2)) AS PERSENT
       ,CAST( b*1./a * 100 as decimal(10,2)) AS [PERSENT_SUM], 
case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'A'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'B'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'C'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'D'
else 'X'
end [ABC]
INTO ##ABC_PRODAGY_PODGRUPPA
from ##PRODAGY_PODGRUPPA AS PRODAGY_PODGRUPPA WITH (NOLOCK)
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) a from ##PRODAGY_PODGRUPPA WITH (NOLOCK)) t1
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) b from ##PRODAGY_PODGRUPPA WITH (NOLOCK) where 
			            [Сумма продаж в ценах закупки с НДС] >= 
			            PRODAGY_PODGRUPPA.[Сумма продаж в ценах закупки с НДС]) t2
where a <> 0 and b <> 0 and [Сумма продаж в ценах закупки с НДС] <> 0;
/*АНАЛИЗ - ABC, ПРОДАЖИ, КАТЕГОРИЯ*/
SELECT  PRODAGY_CATEGORY.Категория AS Категория
       ,PRODAGY_CATEGORY.[Сумма продаж в ценах закупки с НДС]
       ,CAST(PRODAGY_CATEGORY.[Сумма продаж в ценах закупки с НДС]*1./a * 100 as decimal(10,2)) AS PERSENT
       ,CAST( b*1./a * 100 as decimal(10,2) ) AS [PERSENT_SUM], 
case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'A'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'B'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'C'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'D'
else 'X'
end [ABC]
INTO ##ABC_PRODAGY_CATEGORY 
from ##PRODAGY_CATEGORY AS PRODAGY_CATEGORY WITH (NOLOCK) 
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) a from ##PRODAGY_CATEGORY WITH (NOLOCK)) t1
cross apply (select sum([Сумма продаж в ценах закупки с НДС]) b from ##PRODAGY_CATEGORY WITH (NOLOCK) where 
			            [Сумма продаж в ценах закупки с НДС] >= 
			            PRODAGY_CATEGORY.[Сумма продаж в ценах закупки с НДС]) t2
where a <> 0 and b <> 0 and [Сумма продаж в ценах закупки с НДС] <> 0;

----------------------------------------------------------
--														--
---------------------/*FMR - АНАЛИЗ*/---------------------
--														--
----------------------------------------------------------
/*АНАЛИЗ - FMR, ЧАСТОТА, ТоварноеНаправление, БАРКОД*/
SELECT  
         CHASTOTA_BARCODE.ТоварноеНаправление AS ТоварноеНаправление
        ,CHASTOTA_BARCODE.НоменклатураАртикул AS Баркод
        ,CHASTOTA_BARCODE.[Частота продаж]
        ,CAST(CHASTOTA_BARCODE.[Частота продаж]*1./a * 100 as decimal(10,2)) AS PERSENT
        ,CAST( b*1./a * 100 as decimal(10,2)) AS [PERSENT_SUM], 
case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'F'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'M'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'R'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'S'
else 'X'
end [FMR]
INTO ##FMR_CHASTOTA_TN_BARCODE
from ##CHASTOTA_BARCODE_TN AS CHASTOTA_BARCODE WITH (NOLOCK) 
cross apply (select sum([Частота продаж]) a from ##CHASTOTA_BARCODE_TN WITH (NOLOCK) 
             WHERE ТоварноеНаправление = CHASTOTA_BARCODE.ТоварноеНаправление) t1
cross apply (select sum([Частота продаж]) b from ##CHASTOTA_BARCODE_TN WITH (NOLOCK) 
             WHERE [Частота продаж] >= CHASTOTA_BARCODE.[Частота продаж] AND 
			      ТоварноеНаправление = CHASTOTA_BARCODE.ТоварноеНаправление) t2
where a <> 0 and b <> 0 and [Частота продаж] <> 0;

/*АНАЛИЗ - FMR, ЧАСТОТА, ГРУППА, БАРКОД*/
SELECT  
         CHASTOTA_BARCODE.Группа AS Группа
        ,CHASTOTA_BARCODE.НоменклатураАртикул AS Баркод
        ,CHASTOTA_BARCODE.[Частота продаж]
        ,CAST(CHASTOTA_BARCODE.[Частота продаж]*1./a * 100 as decimal(10,2)) AS PERSENT
        ,CAST( b*1./a * 100 as decimal(10,2)) AS [PERSENT_SUM], 
case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'F'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'M'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'R'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'S'
else 'X'
end [FMR]
INTO ##FMR_CHASTOTA_GRUPPA_BARCODE
from ##CHASTOTA_BARCODE_TN_GRUPPA AS CHASTOTA_BARCODE WITH (NOLOCK) 
cross apply (select sum([Частота продаж]) a from ##CHASTOTA_BARCODE_TN_GRUPPA WITH (NOLOCK) 
             WHERE ГРУППА = CHASTOTA_BARCODE.ГРУППА) t1
cross apply (select sum([Частота продаж]) b from ##CHASTOTA_BARCODE_TN_GRUPPA WITH (NOLOCK) 
             WHERE [Частота продаж] >= CHASTOTA_BARCODE.[Частота продаж] AND 
			       ГРУППА = CHASTOTA_BARCODE.ГРУППА) t2
where a <> 0 and b <> 0 and [Частота продаж] <> 0;

/*АНАЛИЗ - FMR, ЧАСТОТА, ПОДГРУППА, БАРКОД*/
SELECT  
        CHASTOTA_BARCODE.Подгруппа AS Подгруппа
        ,CHASTOTA_BARCODE.НоменклатураАртикул AS Баркод
        ,CHASTOTA_BARCODE.[Частота продаж]
       ,CAST(CHASTOTA_BARCODE.[Частота продаж]*1./a * 100 as decimal(10,2)) AS PERSENT
       ,CAST( b*1./a * 100 as decimal(10,2)) AS [PERSENT_SUM], 
case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'F'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'M'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'R'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'S'
else 'X'
end [FMR]
INTO ##FMR_CHASTOTA_PODGRUPPA_BARCODE
from ##CHASTOTA_BARCODE_TN_PODGRUPPA AS CHASTOTA_BARCODE WITH (NOLOCK) 
cross apply (select sum([Частота продаж]) a from ##CHASTOTA_BARCODE_TN_PODGRUPPA WITH (NOLOCK) WHERE 
                        ПОДГРУППА = CHASTOTA_BARCODE.ПОДГРУППА) t1
cross apply (select sum([Частота продаж]) b from ##CHASTOTA_BARCODE_TN_PODGRUPPA WITH (NOLOCK) WHERE 
			            [Частота продаж] >= CHASTOTA_BARCODE.[Частота продаж] AND 
			            ПОДГРУППА = CHASTOTA_BARCODE.ПОДГРУППА) t2
where a <> 0 and b <> 0 and [Частота продаж] <> 0;

/*АНАЛИЗ - FMR, ЧАСТОТА, КАТЕГОРИЯ, БАРКОД*/
SELECT  
         CHASTOTA_BARCODE.Категория AS Категория
        ,CHASTOTA_BARCODE.НоменклатураАртикул AS Баркод
        ,CHASTOTA_BARCODE.[Частота продаж]
       ,CAST(CHASTOTA_BARCODE.[Частота продаж]*1./a * 100 as decimal(10,2)) AS PERSENT
       ,CAST( b*1./a * 100 as decimal(10,2)) AS [PERSENT_SUM], 
case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'F'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'M'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'R'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'S'
else 'X'
end [FMR]
INTO ##FMR_CHASTOTA_CATEGORY_BARCODE
from ##CHASTOTA_BARCODE_TN_CATEGORY AS CHASTOTA_BARCODE WITH (NOLOCK) 
cross apply (select sum([Частота продаж]) a from ##CHASTOTA_BARCODE_TN_CATEGORY WITH (NOLOCK) 
                        WHERE КАТЕГОРИЯ = CHASTOTA_BARCODE.КАТЕГОРИЯ) t1
cross apply (select sum([Частота продаж]) b from ##CHASTOTA_BARCODE_TN_CATEGORY WITH (NOLOCK) 
						WHERE [Частота продаж] >= CHASTOTA_BARCODE.[Частота продаж] AND 
			            КАТЕГОРИЯ = CHASTOTA_BARCODE.КАТЕГОРИЯ) t2
where a <> 0 and b <> 0 and [Частота продаж] <> 0;

/*АНАЛИЗ - FMR, ЧАСТОТА, БАРКОД*/

SELECT  CHASTOTA_BARCODE.НоменклатураАртикул AS Баркод
       ,CHASTOTA_BARCODE.[Частота продаж]
       ,CAST(CHASTOTA_BARCODE.[Частота продаж]*1./a * 100 as decimal(10,2)) AS PERSENT
       ,CAST( b*1./a * 100 as decimal(10,2)) AS [PERSENT_SUM], 
case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'F'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'M'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'R'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'S'
else 'X'
end [FMR]
INTO ##FMR_CHASTOTA_BARCODE
from ##CHASTOTA_BARCODE AS CHASTOTA_BARCODE WITH (NOLOCK) 
cross apply (select sum([Частота продаж]) a from ##CHASTOTA_BARCODE WITH (NOLOCK)) t1
cross apply (select sum([Частота продаж]) b from ##CHASTOTA_BARCODE WITH (NOLOCK) where 
			            [Частота продаж] >= CHASTOTA_BARCODE.[Частота продаж]) t2
where a <> 0 and b <> 0 and [Частота продаж] <> 0;

/*АНАЛИЗ - FMR, ЧАСТОТА, ТН*/
SELECT  CHASTOTA_TN.ТОВАРНОЕНАПРАВЛЕНИЕ AS ТОВАРНОЕНАПРАВЛЕНИЕ
       ,CHASTOTA_TN.[Частота продаж]
       ,CAST(CHASTOTA_TN.[Частота продаж]*1./a * 100 as decimal(10,2)) AS PERSENT
       ,CAST( b*1./a * 100 as decimal(10,2)) AS [PERSENT_SUM], 
case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'F'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'M'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'R'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'S'
else 'X'
end [FMR]
INTO ##FMR_CHASTOTA_TN
from ##CHASTOTA_TN AS CHASTOTA_TN WITH (NOLOCK) 
cross apply (select sum([Частота продаж]) a from ##CHASTOTA_TN WITH (NOLOCK)) t1
cross apply (select sum([Частота продаж]) b from ##CHASTOTA_TN WITH (NOLOCK) where 
			            [Частота продаж] >= CHASTOTA_TN.[Частота продаж]) t2
where a <> 0 and b <> 0 and [Частота продаж] <> 0;

/*АНАЛИЗ - FMR, ЧАСТОТА, ГРУППА*/
SELECT  CHASTOTA_GRUPPA.Группа AS Группа
       ,CHASTOTA_GRUPPA.[Частота продаж]
       ,CAST(CHASTOTA_GRUPPA.[Частота продаж]*1./a * 100 as decimal(10,2)) AS PERSENT
       ,CAST( b*1./a * 100 as decimal(10,2)) AS [PERSENT_SUM], 
case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'F'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'M'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'R'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'S'
else 'X'
end [FMR]
INTO ##FMR_CHASTOTA_GRUPPA
from ##CHASTOTA_GRUPPA AS CHASTOTA_GRUPPA WITH (NOLOCK) 
cross apply (select sum([Частота продаж]) a from ##CHASTOTA_GRUPPA WITH (NOLOCK)) t1
cross apply (select sum([Частота продаж]) b from ##CHASTOTA_GRUPPA WITH (NOLOCK) where 
			            [Частота продаж] >= CHASTOTA_GRUPPA.[Частота продаж]) t2
where a <> 0 and b <> 0 and [Частота продаж] <> 0;

/*АНАЛИЗ - FMR, ЧАСТОТА, ПОДГРУППА*/
SELECT  CHASTOTA_PODGRUPPA.Подгруппа AS Подгруппа
       ,CHASTOTA_PODGRUPPA.[Частота продаж]
       ,CAST(CHASTOTA_PODGRUPPA.[Частота продаж]*1./a * 100 as decimal(10,2)) AS PERSENT
       ,CAST( b*1./a * 100 as decimal(10,2)) AS [PERSENT_SUM], 
case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'F'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'M'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'R'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'S'
else 'X'
end [FMR]
INTO ##FMR_CHASTOTA_PODGRUPPA
from ##CHASTOTA_PODGRUPPA AS CHASTOTA_PODGRUPPA WITH (NOLOCK) 
cross apply (select sum([Частота продаж]) a from ##CHASTOTA_PODGRUPPA WITH (NOLOCK)) t1
cross apply (select sum([Частота продаж]) b from ##CHASTOTA_PODGRUPPA WITH (NOLOCK) where 
			            [Частота продаж] >= CHASTOTA_PODGRUPPA.[Частота продаж]) t2
where a <> 0 and b <> 0 and [Частота продаж] <> 0;

/*АНАЛИЗ - FMR, ЧАСТОТА, КАТЕГОРИЯ*/
SELECT  CHASTOTA_CATEGORY.Категория AS Категория
       ,CHASTOTA_CATEGORY.[Частота продаж]
       ,CAST(CHASTOTA_CATEGORY.[Частота продаж]*1./a * 100 as decimal(10,2)) AS PERSENT
       ,CAST( b*1./a * 100 as decimal(10,2)) AS [PERSENT_SUM], 
case 
when cast( b*1./a * 100 as decimal(10,2)) between 0 and 80 then 'F'
when cast( b*1./a * 100 as decimal(10,2)) between 80 and 95 then 'M'
when cast( b*1./a * 100 as decimal(10,2)) between 95 and 99 then 'R'
when cast( b*1./a * 100 as decimal(10,2)) between 99 and 100 then 'S'
else 'X'
end [FMR]
INTO ##FMR_CHASTOTA_CATEGORY
from ##CHASTOTA_CATEGORY AS CHASTOTA_CATEGORY WITH (NOLOCK) 
cross apply (select sum([Частота продаж]) a from ##CHASTOTA_CATEGORY WITH (NOLOCK)) t1
cross apply (select sum([Частота продаж]) b from ##CHASTOTA_CATEGORY WITH (NOLOCK)  where 
			            [Частота продаж] >= CHASTOTA_CATEGORY.[Частота продаж]) t2
where a <> 0 and b <> 0 and [Частота продаж] <> 0;

INSERT INTO [Table_Отчет_ABC&FMR]
SELECT DISTINCT
NOMENKLATURA.ТоварноеНаправление AS [1_Товарная номенклатура], 
NOMENKLATURA.Группа AS [2_Товарная категория], 
NOMENKLATURA.Подгруппа AS [3_Товарная группа], 
NOMENKLATURA.Категория AS [4_Товарная линейка], 
NOMENKLATURA.Номенклатура AS [0_Номенклатура общая], 
NOMENKLATURA.[Наименование группы ВЗТ], 
NOMENKLATURA.[НоменклатураОсновной статус товара], 
NOMENKLATURA.[НоменклатураВЗТОсновной статус товара],
(CASE WHEN ABC_PRODAGY_TN.ABC+FMR_CHASTOTA_TN.FMR IS NULL THEN 'XX' ELSE ABC_PRODAGY_TN.ABC+FMR_CHASTOTA_TN.FMR END) AS [Анализ ABC&FMR 1_Товарная номенклатура],
(CASE WHEN ABC_PRODAGY_GRUPPA.ABC+FMR_CHASTOTA_GRUPPA.FMR IS NULL THEN 'XX' ELSE ABC_PRODAGY_GRUPPA.ABC+FMR_CHASTOTA_GRUPPA.FMR END) AS [Анализ ABC&FMR 2_Товарная категория],
(CASE WHEN ABC_PRODAGY_PODGRUPPA.ABC+FMR_CHASTOTA_PODGRUPPA.FMR IS NULL THEN 'XX' ELSE ABC_PRODAGY_PODGRUPPA.ABC+FMR_CHASTOTA_PODGRUPPA.FMR END) AS [Анализ ABC&FMR 3_Товарная группа],
(CASE WHEN ABC_PRODAGY_CATEGORY.ABC+FMR_CHASTOTA_CATEGORY.FMR IS NULL THEN 'XX' ELSE ABC_PRODAGY_CATEGORY.ABC+FMR_CHASTOTA_CATEGORY.FMR END) AS [Анализ ABC&FMR 4_Товарная линейка],
(CASE WHEN ABC_PRODAGY_BARCODE.ABC+FMR_CHASTOTA_BARCODE.FMR IS NULL THEN 'XX' ELSE ABC_PRODAGY_BARCODE.ABC+FMR_CHASTOTA_BARCODE.FMR END) AS [Анализ ABC&FMR Баркод],
(CASE WHEN ABC_PRODAGY_TN_BARCODE.ABC + FMR_CHASTOTA_TN_BARCODE.FMR IS NULL THEN 'XX' ELSE ABC_PRODAGY_TN_BARCODE.ABC + FMR_CHASTOTA_TN_BARCODE.FMR END) AS [Анализ ABC&FMR 1_Товарная номенклатура_Баркод],
(CASE WHEN ABC_PRODAGY_GRUPPA_BARCODE.ABC + FMR_CHASTOTA_GRUPPA_BARCODE.FMR IS NULL THEN 'XX' ELSE ABC_PRODAGY_GRUPPA_BARCODE.ABC + FMR_CHASTOTA_GRUPPA_BARCODE.FMR END) AS [Анализ ABC&FMR 2_Товарная категория_Баркод],
(CASE WHEN ABC_PRODAGY_PODGRUPPA_BARCODE.ABC + FMR_CHASTOTA_PODGRUPPA_BARCODE.FMR IS NULL THEN 'XX' ELSE ABC_PRODAGY_PODGRUPPA_BARCODE.ABC + FMR_CHASTOTA_PODGRUPPA_BARCODE.FMR END) AS [Анализ ABC&FMR 3_Товарная группа_Баркод],
(CASE WHEN ABC_PRODAGY_CATEGORY_BARCODE.ABC + FMR_CHASTOTA_CATEGORY_BARCODE.FMR IS NULL THEN 'XX' ELSE ABC_PRODAGY_CATEGORY_BARCODE.ABC + FMR_CHASTOTA_CATEGORY_BARCODE.FMR END) AS [Анализ ABC&FMR 4_Товарная линейка_Баркод],
(CASE WHEN (CASE WHEN ABC_PRODAGY_TN_BARCODE.ABC + FMR_CHASTOTA_TN_BARCODE.FMR IS NULL THEN 'XX' ELSE ABC_PRODAGY_TN_BARCODE.ABC + FMR_CHASTOTA_TN_BARCODE.FMR END) IN ('AF', 'BF', 'AM') AND 
           (CASE WHEN ABC_PRODAGY_GRUPPA_BARCODE.ABC + FMR_CHASTOTA_GRUPPA_BARCODE.FMR IS NULL THEN 'XX' ELSE ABC_PRODAGY_GRUPPA_BARCODE.ABC + FMR_CHASTOTA_GRUPPA_BARCODE.FMR END) IN ('AF', 'BF', 'AM') AND 
           (CASE WHEN ABC_PRODAGY_PODGRUPPA_BARCODE.ABC + FMR_CHASTOTA_PODGRUPPA_BARCODE.FMR IS NULL THEN 'XX' ELSE ABC_PRODAGY_PODGRUPPA_BARCODE.ABC + FMR_CHASTOTA_PODGRUPPA_BARCODE.FMR END) IN ('AF', 'BF', 'AM') AND 
           (CASE WHEN ABC_PRODAGY_CATEGORY_BARCODE.ABC + FMR_CHASTOTA_CATEGORY_BARCODE.FMR IS NULL THEN 'XX' ELSE ABC_PRODAGY_CATEGORY_BARCODE.ABC + FMR_CHASTOTA_CATEGORY_BARCODE.FMR END) IN ('AF', 'BF', 'AM') 
THEN 'ДА' ELSE 'НЕТ' END) AS 'ВОЗМОЖНЫЙ СТАТУС <МФРЦ>',
NOMENKLATURA.Баркод,
NOMENKLATURA.ЖЦ,
NOMENKLATURA.Реализация,
NOMENKLATURA.[Базовая единица измерения],
NOMENKLATURA.[Кратность Упаковки],
MINIMUMSTANDARTFILLING.[Максимум],
MINIMUMSTANDARTFILLING.[ТочкаЗакза],
SREDNEVZVPRICE.[Сред_Взв_Цена],
FMR_CHASTOTA_BARCODE.[Частота продаж],
NOT_PRODAGY_THREE_MOUNTH.[Сумма_кол-во_без_движения_3_месяца],
OSTATKI_SKLAD.Количество AS [Остатки на складах],
PRODAGY_III_QUARTER.[Сумма продаж в ценах закупки с НДС_III_квартал], 
PRODAGY_IV_QUARTER.[Сумма продаж в ценах закупки с НДС_IV_квартал],
PRIHOD_DATA.[ПЕРВАЯ ДАТА ПОСТУПЛЕНИЯ],
PRIHOD_DATA.[ПОСЛЕДНЯЯ ДАТА ПОСТУПЛЕНИЯ],
FUNCTIONAL_RA_TP.ПФО_Актобе,
FUNCTIONAL_RA_TP.ПФО_Казань,
FUNCTIONAL_RA_TP.[ПФО_Н.Новгород],
FUNCTIONAL_RA_TP.[ПФО_Н.Новгород Веденяпина],
FUNCTIONAL_RA_TP.ПФО_Оренбург,
FUNCTIONAL_RA_TP.ПФО_Оренбург_авт,
FUNCTIONAL_RA_TP.ПФО_Пенза,
FUNCTIONAL_RA_TP.ПФО_Самара,
FUNCTIONAL_RA_TP.ПФО_Самара_Кир,
FUNCTIONAL_RA_TP.ПФО_Самара_Красн,
FUNCTIONAL_RA_TP.ПФО_Самара_УТ,
FUNCTIONAL_RA_TP.ПФО_Саратов,
FUNCTIONAL_RA_TP.ПФО_Тольятти,
FUNCTIONAL_RA_TP.ПФО_Тольятти_Громова,
FUNCTIONAL_RA_TP.ПФО_Ульяновск,
FUNCTIONAL_RA_TP.ПФО_Уфа,
FUNCTIONAL_RA_TP.[ПФО_Уфа Кавказская],
FUNCTIONAL_RA_TP.ПФО_Чебоксары,
FUNCTIONAL_RA_TP.ПФО_Энгельс,
FUNCTIONAL_RA_TP.[СЗФО_LVS Горелово-16 (с 18.04)],
FUNCTIONAL_RA_TP.СЗФО_Архангельск,
FUNCTIONAL_RA_TP.[СЗФО_Б.Сампсониевский],
FUNCTIONAL_RA_TP.СЗФО_Бабушкина,
FUNCTIONAL_RA_TP.СЗФО_Большевиков,
FUNCTIONAL_RA_TP.СЗФО_Будапештская,
FUNCTIONAL_RA_TP.[СЗФО_Великие Луки],
FUNCTIONAL_RA_TP.СЗФО_ВО,
FUNCTIONAL_RA_TP.СЗФО_Вологда,
FUNCTIONAL_RA_TP.СЗФО_Гатчина,
FUNCTIONAL_RA_TP.СЗФО_Е12,
FUNCTIONAL_RA_TP.СЗФО_Кингисепп,
FUNCTIONAL_RA_TP.СЗФО_Колпино,
FUNCTIONAL_RA_TP.СЗФО_Коми,
FUNCTIONAL_RA_TP.СЗФО_Косыгина,
FUNCTIONAL_RA_TP.СЗФО_Котлас,
FUNCTIONAL_RA_TP.СЗФО_Л260,
FUNCTIONAL_RA_TP.СЗФО_Ленинский,
FUNCTIONAL_RA_TP.СЗФО_Мурманск,
FUNCTIONAL_RA_TP.СЗФО_Науки,
FUNCTIONAL_RA_TP.СЗФО_Новгород,
FUNCTIONAL_RA_TP.СЗФО_Обводный,
FUNCTIONAL_RA_TP.СЗФО_П59,
FUNCTIONAL_RA_TP.СЗФО_Петрозаводск,
FUNCTIONAL_RA_TP.СЗФО_Псков,
FUNCTIONAL_RA_TP.СЗФО_Рижский,
FUNCTIONAL_RA_TP.СЗФО_СБ,
FUNCTIONAL_RA_TP.СЗФО_Среднеохтинский,
FUNCTIONAL_RA_TP.СЗФО_Стачек,
FUNCTIONAL_RA_TP.СЗФО_Сытный,
FUNCTIONAL_RA_TP.СЗФО_Чкаловский,
FUNCTIONAL_RA_TP.СЗФО_Школьная,
FUNCTIONAL_RA_TP.СЗФО_Электролайт,
FUNCTIONAL_RA_TP.СЗФО_Энгельса,
FUNCTIONAL_RA_TP.СФО_Кемерово,
FUNCTIONAL_RA_TP.СФО_Новосибирск,
FUNCTIONAL_RA_TP.СФО_Новосибирск_Ватутина,
FUNCTIONAL_RA_TP.СФО_Новосибирск_Зыряновское,
FUNCTIONAL_RA_TP.СФО_Новосибирск_Сибиряков,
FUNCTIONAL_RA_TP.УФО_Екатеринбург_Блюхера,
FUNCTIONAL_RA_TP.УФО_Екатеринбург_Мет,
FUNCTIONAL_RA_TP.УФО_Екатеринбург_Тит,
FUNCTIONAL_RA_TP.УФО_Екатеринбург_Шаумяна,
FUNCTIONAL_RA_TP.УФО_Магнитогорск,
FUNCTIONAL_RA_TP.УФО_Миасс,
FUNCTIONAL_RA_TP.УФО_Пермь,
FUNCTIONAL_RA_TP.УФО_Тюмень,
FUNCTIONAL_RA_TP.УФО_Челябинск,
FUNCTIONAL_RA_TP.УФО_Челябинск_Кулиб,
FUNCTIONAL_RA_TP.УФО_Челябинск_Поб,
FUNCTIONAL_RA_TP.УФО_Челябинск_Свердловский,
FUNCTIONAL_RA_TP.ЮФО_Армавир,
FUNCTIONAL_RA_TP.ЮФО_Астрахань,
FUNCTIONAL_RA_TP.ЮФО_Волгоград,
FUNCTIONAL_RA_TP.ЮФО_Волгодонск,
FUNCTIONAL_RA_TP.ЮФО_Воронеж,
FUNCTIONAL_RA_TP.ЮФО_Краснодар,
FUNCTIONAL_RA_TP.ЮФО_Краснодар_Рос,
FUNCTIONAL_RA_TP.ЮФО_Липецк,
FUNCTIONAL_RA_TP.ЮФО_Пятигорск,
FUNCTIONAL_RA_TP.ЮФО_Ростов,
FUNCTIONAL_RA_TP.ЮФО_Ростов_Вят,
FUNCTIONAL_RA_TP.ЮФО_Сочи
/*INTO [ABC&FMR]*/
FROM ##NOMENKLATURA AS NOMENKLATURA WITH (NOLOCK) 
LEFT JOIN ##PRODAGY_III_QUARTER AS PRODAGY_III_QUARTER WITH (NOLOCK)  ON NOMENKLATURA.Баркод = PRODAGY_III_QUARTER.Баркод
LEFT JOIN ##PRODAGY_IV_QUARTER AS PRODAGY_IV_QUARTER WITH (NOLOCK) ON NOMENKLATURA.Баркод = PRODAGY_IV_QUARTER.Баркод
LEFT JOIN ##NOT_PRODAGY_THREE_MOUNTH AS NOT_PRODAGY_THREE_MOUNTH WITH (NOLOCK) ON NOMENKLATURA.Баркод = NOT_PRODAGY_THREE_MOUNTH.Баркод
LEFT JOIN ##SREDNEVZVPRICE AS SREDNEVZVPRICE WITH (NOLOCK) ON NOMENKLATURA.Баркод = SREDNEVZVPRICE.Баркод
LEFT JOIN ##OSTATKI_SKLAD AS OSTATKI_SKLAD WITH (NOLOCK) ON NOMENKLATURA.Баркод = OSTATKI_SKLAD.Баркод

LEFT JOIN ##ABC_PRODAGY_TN_BARCODE AS ABC_PRODAGY_TN_BARCODE WITH (NOLOCK) ON NOMENKLATURA.Баркод = ABC_PRODAGY_TN_BARCODE.Баркод
LEFT JOIN ##ABC_PRODAGY_GRUPPA_BARCODE AS ABC_PRODAGY_GRUPPA_BARCODE WITH (NOLOCK) ON NOMENKLATURA.Баркод = ABC_PRODAGY_GRUPPA_BARCODE.Баркод
LEFT JOIN ##ABC_PRODAGY_PODGRUPPA_BARCODE AS ABC_PRODAGY_PODGRUPPA_BARCODE WITH (NOLOCK) ON NOMENKLATURA.Баркод = ABC_PRODAGY_PODGRUPPA_BARCODE.Баркод
LEFT JOIN ##ABC_PRODAGY_CATEGORY_BARCODE AS ABC_PRODAGY_CATEGORY_BARCODE WITH (NOLOCK) ON NOMENKLATURA.Баркод = ABC_PRODAGY_CATEGORY_BARCODE.Баркод
LEFT JOIN ##ABC_PRODAGY_BARCODE AS ABC_PRODAGY_BARCODE WITH (NOLOCK) ON NOMENKLATURA.Баркод = ABC_PRODAGY_BARCODE.Баркод
LEFT JOIN ##ABC_PRODAGY_TN AS ABC_PRODAGY_TN WITH (NOLOCK) ON NOMENKLATURA.ТоварноеНаправление = ABC_PRODAGY_TN.ТОВАРНОЕНАПРАВЛЕНИЕ
LEFT JOIN ##ABC_PRODAGY_GRUPPA AS ABC_PRODAGY_GRUPPA WITH (NOLOCK) ON NOMENKLATURA.Группа = ABC_PRODAGY_GRUPPA.Группа
LEFT JOIN ##ABC_PRODAGY_PODGRUPPA AS ABC_PRODAGY_PODGRUPPA WITH (NOLOCK) ON NOMENKLATURA.Подгруппа = ABC_PRODAGY_PODGRUPPA.Подгруппа
LEFT JOIN ##ABC_PRODAGY_CATEGORY AS ABC_PRODAGY_CATEGORY WITH (NOLOCK) ON NOMENKLATURA.Категория = ABC_PRODAGY_CATEGORY.Категория

LEFT JOIN ##FMR_CHASTOTA_TN_BARCODE AS FMR_CHASTOTA_TN_BARCODE WITH (NOLOCK) ON NOMENKLATURA.Баркод = FMR_CHASTOTA_TN_BARCODE.Баркод
LEFT JOIN ##FMR_CHASTOTA_GRUPPA_BARCODE AS FMR_CHASTOTA_GRUPPA_BARCODE WITH (NOLOCK) ON NOMENKLATURA.Баркод = FMR_CHASTOTA_GRUPPA_BARCODE.Баркод
LEFT JOIN ##FMR_CHASTOTA_PODGRUPPA_BARCODE AS FMR_CHASTOTA_PODGRUPPA_BARCODE WITH (NOLOCK) ON NOMENKLATURA.Баркод = FMR_CHASTOTA_PODGRUPPA_BARCODE.Баркод
LEFT JOIN ##FMR_CHASTOTA_CATEGORY_BARCODE AS FMR_CHASTOTA_CATEGORY_BARCODE WITH (NOLOCK) ON NOMENKLATURA.Баркод = FMR_CHASTOTA_CATEGORY_BARCODE.Баркод
LEFT JOIN ##FMR_CHASTOTA_BARCODE AS FMR_CHASTOTA_BARCODE WITH (NOLOCK) ON NOMENKLATURA.Баркод = FMR_CHASTOTA_BARCODE.Баркод
LEFT JOIN ##FMR_CHASTOTA_TN AS FMR_CHASTOTA_TN WITH (NOLOCK) ON NOMENKLATURA.ТоварноеНаправление = FMR_CHASTOTA_TN.ТОВАРНОЕНАПРАВЛЕНИЕ
LEFT JOIN ##FMR_CHASTOTA_GRUPPA AS FMR_CHASTOTA_GRUPPA WITH (NOLOCK) ON NOMENKLATURA.Группа = FMR_CHASTOTA_GRUPPA.Группа
LEFT JOIN ##FMR_CHASTOTA_PODGRUPPA AS FMR_CHASTOTA_PODGRUPPA WITH (NOLOCK) ON NOMENKLATURA.Подгруппа = FMR_CHASTOTA_PODGRUPPA.Подгруппа
LEFT JOIN ##FMR_CHASTOTA_CATEGORY AS FMR_CHASTOTA_CATEGORY WITH (NOLOCK) ON NOMENKLATURA.Категория = FMR_CHASTOTA_CATEGORY.Категория

LEFT JOIN ##FUNCTIONAL_RA_TP AS FUNCTIONAL_RA_TP WITH (NOLOCK) ON NOMENKLATURA.Баркод = FUNCTIONAL_RA_TP.Баркод
LEFT JOIN ##PRIHOD_DATA AS PRIHOD_DATA WITH (NOLOCK) ON NOMENKLATURA.Баркод = PRIHOD_DATA.БАРКОД

LEFT JOIN ##MINIMUMSTANDARTFILLING AS MINIMUMSTANDARTFILLING WITH (NOLOCK) ON NOMENKLATURA.Баркод = MINIMUMSTANDARTFILLING.Артикул

WHERE NOMENKLATURA.Баркод IS NOT NULL
ORDER BY 1 ASC;

/*Обновление поля [ВОЗМОЖНЫЙ СТАТУС <МФРЦ>], 
  если один из баркодов в группе ВЗТ имеет статус [ВОЗМОЖНЫЙ СТАТУС <МФРЦ>] = 'ДА', 
  то и все баркоды в группе приобретают такой статус*/
with t as 
(
select distinct 
ROW_NUMBER() OVER (ORDER BY [Наименование группы ВЗТ]  ASC) AS rownumber,
t.[Наименование группы ВЗТ]
from
(select distinct [Наименование группы ВЗТ]
FROM [ETL_1C_SQL].[dbo].[Table_Отчет_ABC&FMR]
where [Наименование группы ВЗТ] is not null) t
),
p as 
(
SELECT distinct [Наименование группы ВЗТ]
      ,[ВОЗМОЖНЫЙ СТАТУС <МФРЦ>]
      ,[Баркод]
FROM [ETL_1C_SQL].[dbo].[Table_Отчет_ABC&FMR]
where [Наименование группы ВЗТ] is not null
),
f as
(select t.rownumber, p.[Наименование группы ВЗТ], p.[ВОЗМОЖНЫЙ СТАТУС <МФРЦ>]
from t, p
where t.[Наименование группы ВЗТ] = p.[Наименование группы ВЗТ]

and p.[ВОЗМОЖНЫЙ СТАТУС <МФРЦ>] = 'ДА'
group by t.rownumber,p.[Наименование группы ВЗТ], p.[ВОЗМОЖНЫЙ СТАТУС <МФРЦ>]
)
update [ETL_1C_SQL].[dbo].[Table_Отчет_ABC&FMR]
set [ETL_1C_SQL].[dbo].[Table_Отчет_ABC&FMR].[ВОЗМОЖНЫЙ СТАТУС <МФРЦ>] = f.[ВОЗМОЖНЫЙ СТАТУС <МФРЦ>]
from f
inner join [ETL_1C_SQL].[dbo].[Table_Отчет_ABC&FMR] as k on k.[Наименование группы ВЗТ] = f.[Наименование группы ВЗТ]
where f.[Наименование группы ВЗТ] is not null;

/*УДАЛИМ ВСЕ ВРЕМЕННЫЕ ТАБЛИЦЫ*/
IF OBJECT_ID('[tempdb]..##PRODAGY_TN', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_TN;
IF OBJECT_ID('[tempdb]..##PRODAGY_GRUPPA', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_GRUPPA;
IF OBJECT_ID('[tempdb]..##PRODAGY_PODGRUPPA', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_PODGRUPPA;
IF OBJECT_ID('[tempdb]..##PRODAGY_CATEGORY', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_CATEGORY;
IF OBJECT_ID('[tempdb]..##PRODAGY_BARCODE', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_BARCODE;
IF OBJECT_ID('[tempdb]..[tempdb]..##PRODAGY_BARCODE_TN', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_BARCODE_TN;
IF OBJECT_ID('[tempdb]..##PRODAGY_BARCODE_TN_GRUPPA', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_BARCODE_TN_GRUPPA;
IF OBJECT_ID('[tempdb]..##PRODAGY_BARCODE_TN_PODGRUPPA', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_BARCODE_TN_PODGRUPPA;
IF OBJECT_ID('[tempdb]..##PRODAGY_BARCODE_TN_CATEGORY', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_BARCODE_TN_CATEGORY;
IF OBJECT_ID('[tempdb]..##CHASTOTA_TN', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_TN;
IF OBJECT_ID('[tempdb]..##CHASTOTA_GRUPPA', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_GRUPPA;
IF OBJECT_ID('[tempdb]..##CHASTOTA_PODGRUPPA', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_PODGRUPPA;
IF OBJECT_ID('[tempdb]..##CHASTOTA_CATEGORY', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_CATEGORY;
IF OBJECT_ID('[tempdb]..##CHASTOTA_BARCODE', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_BARCODE;
IF OBJECT_ID('[tempdb]..##CHASTOTA_BARCODE_TN', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_BARCODE_TN;
IF OBJECT_ID('[tempdb]..##CHASTOTA_BARCODE_TN_GRUPPA', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_BARCODE_TN_GRUPPA;
IF OBJECT_ID('[tempdb]..##CHASTOTA_BARCODE_TN_PODGRUPPA', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_BARCODE_TN_PODGRUPPA;
IF OBJECT_ID('[tempdb]..##CHASTOTA_BARCODE_TN_CATEGORY', 'U') IS NOT NULL
DROP TABLE ##CHASTOTA_BARCODE_TN_CATEGORY;
IF OBJECT_ID('[tempdb]..##NOMENKLATURA', 'U') IS NOT NULL
DROP TABLE ##NOMENKLATURA;
IF OBJECT_ID('[tempdb]..##PRODAGY_III_QUARTER', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_III_QUARTER;
IF OBJECT_ID('[tempdb]..##PRODAGY_IV_QUARTER', 'U') IS NOT NULL
DROP TABLE ##PRODAGY_IV_QUARTER;
IF OBJECT_ID('[tempdb]..##NOT_PRODAGY_THREE_MOUNTH', 'U') IS NOT NULL
DROP TABLE ##NOT_PRODAGY_THREE_MOUNTH;
IF OBJECT_ID('[tempdb]..##SREDNEVZVPRICE', 'U') IS NOT NULL
DROP TABLE ##SREDNEVZVPRICE;
IF OBJECT_ID('[tempdb]..##OSTATKI_SKLAD', 'U') IS NOT NULL
DROP TABLE ##OSTATKI_SKLAD;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_TN_BARCODE', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_TN_BARCODE;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_GRUPPA_BARCODE', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_GRUPPA_BARCODE;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_PODGRUPPA_BARCODE', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_PODGRUPPA_BARCODE;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_CATEGORY_BARCODE', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_CATEGORY_BARCODE;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_BARCODE', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_BARCODE;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_TN', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_TN;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_GRUPPA', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_GRUPPA;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_PODGRUPPA', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_PODGRUPPA;
IF OBJECT_ID('[tempdb]..##ABC_PRODAGY_CATEGORY', 'U') IS NOT NULL
DROP TABLE ##ABC_PRODAGY_CATEGORY;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_TN_BARCODE', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_TN_BARCODE;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_GRUPPA_BARCODE', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_GRUPPA_BARCODE;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_PODGRUPPA_BARCODE', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_PODGRUPPA_BARCODE;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_CATEGORY_BARCODE', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_CATEGORY_BARCODE;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_BARCODE', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_BARCODE;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_TN', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_TN;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_GRUPPA', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_GRUPPA;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_PODGRUPPA', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_PODGRUPPA;
IF OBJECT_ID('[tempdb]..##FMR_CHASTOTA_CATEGORY', 'U') IS NOT NULL
DROP TABLE ##FMR_CHASTOTA_CATEGORY;
IF OBJECT_ID('[tempdb]..##FUNCTIONAL_RA_TP', 'U') IS NOT NULL
DROP TABLE ##FUNCTIONAL_RA_TP;
IF OBJECT_ID('[tempdb]..##PRIHOD_DATA', 'U') IS NOT NULL
DROP TABLE ##PRIHOD_DATA;
IF OBJECT_ID('[tempdb]..##MINIMUMSTANDARTFILLING', 'U') IS NOT NULL
DROP TABLE ##MINIMUMSTANDARTFILLING;
COMMIT TRAN;