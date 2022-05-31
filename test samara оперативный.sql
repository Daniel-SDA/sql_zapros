
drop table ##OwnOrg_ /* создать перед выполнением ##таблицы на 192-168-0-252*/
create table ##OwnOrg_
(
    [КонтрагентСсылка] [binary](16) NOT NULL,
	[Контрагент] [nvarchar](100) NULL,
	[ТипСправочника] [binary](4) NOT NULL,
	[Подразделение_Ссылка] [binary](16) NOT NULL,
	[Подразделение] [nvarchar](75) NULL,
	[БазаДанных] [INT] NOT NULL
);

create table ##RegSal_
(
	[ДокументПродажи_Ссылка] [binary](16) NOT NULL,
	[Номенклатура_Ссылка] [binary](16) NOT NULL,
	[Количество] [numeric](38, 3) NULL,
	[Стоимость] [numeric](38, 2) NULL,
	[СтоимостьБезСкидок] [numeric](38, 2) NULL,
	[СтоимостьВБазовыхММЦенах] [numeric](38, 2) NULL,
	[БазаДанных] [INT] NOT NULL
);

create table ##DocLzk_
(
	[м_ЛЗК_Ссылка] [binary](16) NOT NULL,
	[Номенклатура_Ссылка] [binary](16) NOT NULL,
	[Количество] [numeric](38, 3) NULL,
	[СуммаСНДС] [numeric](38, 2) NULL,
	[БазаДанных] [INT] NOT NULL
);

create table ##DocPer_
(
	[Перемещение_Ссылка] [binary](16) NOT NULL,
	[Номенклатура_Ссылка] [binary](16) NOT NULL,
	[Количество] [numeric](38, 3) NULL,
	[СуммаПеремещения] [numeric](38, 5) NULL,
	[БазаДанных] [INT] NOT NULL
);

create table ##RoznSl_
(
	[ОтчетОРозничныхПродажах_Ссылка] [binary](16) NOT NULL,
	[Номенклатура_Ссылка] [binary](16) NOT NULL,
	[Количество] [numeric](38, 3) NULL,
	[Сумма] [numeric](38, 3) NULL,
	[СуммаБезСкидок] [numeric](38, 3) NULL,
	[БазаДанных] [INT] NOT NULL
);

create table ##CheKKM_
(
	[ЧекККМ_Ссылка] [binary](16) NOT NULL,
	[Номенклатура_Ссылка] [binary](16) NOT NULL,
	[Количество] [numeric](38, 3) NULL,
	[Сумма] [numeric](38, 3) NULL,
	[СуммаБезСкидки] [numeric](38, 3) NULL,
	[БазаДанных] [INT] NOT NULL
);

create table ##СборДанных_
(
	[Период] [datetime] NULL,
	[Номенклатура_ссылка] [binary](16) NOT NULL,
	[НомерСтроки] [numeric] (9,0) NULL, 
	[Контрагент_ссылка] [binary](16) NULL,
	[Документ_Ссылка] [binary](16) NULL,
	[Ответственный_Ссылка] [binary](16) NULL,
	[Редактор_Ссылка] [binary](16) NULL,
	[ЛП_Отгрузки_Ссылка] [binary](16) NULL,
	[ЛП_Отгрузки_Тип_Ссылка] [binary](16) NULL,
	[ЛП_Получатель_Ссылка] [binary](16) NULL,
	[ЛП_Получатель_Тип_Ссылка] [binary](16) NULL,
	[Регистратор_Номер] [nchar](50) NULL,
	[Регистратор_Тип] [varchar](150) NULL,
	[Тип_Отгрузки_Ссылка] [binary](16) NULL,/*[varchar](50) NULL,*/
	[Количество] [numeric](38, 3) NULL,
	[СебестоимостьСНДС] [numeric](38, 3) NULL,
	[БазаДанных] [INT] NOT NULL
);


insert into ##OwnOrg_ ([КонтрагентСсылка],[Контрагент],[ТипСправочника],[Подразделение_Ссылка],[Подразделение], [БазаДанных]) 
select distinct t_own.[_Fld7309_RRRef] as КонтрагентСсылка, (case when t_own1.[_Description] is null  then t_own2.[_Description] else t_own1.[_Description] end)  as Контрагент, t_own.[_Fld7309_RTRef] as ТипСправочника, t_own.[_Fld7311_RRRef] as Подразделение_Ссылка, t_own3.[_Description] as Подразделение , convert(int, 17) as [БазаДанных] from [Smr_Trade].[dbo].[_InfoRg7308] as  t_own  with(nolock) /*Значения свойств объектов*/ left join [Smr_Trade].[dbo].[_Reference54] as  t_own1  with(nolock) on t_own.[_Fld7309_RRRef]=t_own1.[_IDRRef] and t_own.[_Fld7309_RTRef]=0x00000036/*--справочник Контрагенты*/ left join [Smr_Trade].[dbo].[_Reference66] as  t_own2  with(nolock) on t_own.[_Fld7309_RRRef]=t_own2.[_IDRRef] and t_own.[_Fld7309_RTRef]=0x00000042/*--справочник Организации*/ left join [Smr_Trade].[dbo].[_Reference68] as  t_own3  with(nolock) on t_own.[_Fld7311_RRRef]=t_own3.[_IDRRef] and t_own.[_Fld7311_RTRef]=0x00000044/*--справочник Подразделения*/ where t_own.[_Fld7309_RTRef] in (0x00000036, 0x00000042)
insert into ##RegSal_ ([ДокументПродажи_Ссылка], [Номенклатура_Ссылка], [Количество], [Стоимость], [СтоимостьБезСкидок], [СтоимостьВБазовыхММЦенах], [БазаДанных])
 select rnSl.[_Fld8722_RRRef] as ДокументПродажи_Ссылка , rnSl.[_Fld8718RRef] as Номенклатура_Ссылка , sum(rnSl.[_Fld8727]) as Количество , sum(rnSl.[_Fld8728]) as Стоимость , sum(rnSl.[_Fld8729]) as СтоимостьБезСкидок , sum(rnSl.[_Fld13004]) as СтоимостьВБазовыхММЦенах , convert(int, 17) as [БазаДанных] from [Smr_Trade].[dbo].[_AccumRg8717] as  rnSl  with(nolock)  group by rnSl.[_Fld8722_RRRef], rnSl.[_Fld8718RRef]
insert into ##DocLzk_ ([м_ЛЗК_Ссылка], [Номенклатура_Ссылка], [Количество], [СуммаСНДС], [БазаДанных]) 
select tLzk.[_Document246_IDRRef] as м_ЛЗК_Ссылка , tLzk.[_Fld6605RRef] as Номенклатура_Ссылка , sum(tLzk.[_Fld6612]) as Количество , sum(tLzk.[_Fld6616]+tLzk.[_Fld6617]) as СуммаСНДС , convert(int, 17) as [БазаДанных] from [Smr_Trade].[dbo].[_Document246_VT6603] as  tLzk  with(nolock)  group by tLzk.[_Document246_IDRRef], tLzk.[_Fld6605RRef]
insert into ##DocPer_ ([Перемещение_Ссылка], [Номенклатура_Ссылка], [Количество], [СуммаПеремещения], [БазаДанных]) 
select tPer.[_Document189_IDRRef] as [Перемещение_Ссылка] , tPer.[_Fld4146RRef] as [Номенклатура_Ссылка] , sum(tPer.[_Fld4151]) as [Количество] , sum(tPer.[_Fld4152]*tPer.[_Fld4151]) as [СуммаПеремещения] , convert(int, 17) as [БазаДанных] from [Smr_Trade].[dbo].[_Document189_VT4144] as tPer with(nolock)  group by tPer.[_Document189_IDRRef], tPer.[_Fld4146RRef]
insert into ##RoznSl_ ([ОтчетОРозничныхПродажах_Ссылка], [Номенклатура_Ссылка], [Количество], [Сумма], [СуммаБезСкидок], [БазаДанных]) 
select tRozn.[_Document188_IDRRef] as [ОтчетОРозничныхПродажах_Ссылка] , tRozn.[_Fld4048RRef] as [Номенклатура_Ссылка] , sum(tRozn.[_Fld4052]) as [Количество] , sum(tRozn.[_Fld4053]) as [Сумма] , sum(tRozn.[_Fld4053]/((100-tRozn.[_Fld4057])/100)) as [СуммаБезСкидок] , convert(int, 17) as [БазаДанных] from [Smr_Trade].[dbo].[_Document188_VT4046] as tRozn with(nolock)  group by tRozn.[_Document188_IDRRef], tRozn.[_Fld4048RRef]
insert into ##CheKKM_ ([ЧекККМ_Ссылка], [Номенклатура_Ссылка], [Количество], [Сумма], [СуммаБезСкидки], [БазаДанных]) 
select kkm.[_Document241_IDRRef] as [ЧекККМ_Ссылка] , kkm.[_Fld6367RRef] as [Номенклатура_Ссылка] , sum(kkm.[_Fld6368]) as [Количество] , sum(kkm.[_Fld6373]) as [Сумма] , sum(kkm.[_Fld6373]/((100-kkm.[_Fld6372])/100)) as [СуммаБезСкидки] , convert(int, 17) as [БазаДанных] from [Smr_Trade].[dbo].[_Document241_VT6365] as kkm with(nolock)  group by kkm.[_Document241_IDRRef], kkm.[_Fld6367RRef]

insert into ##СборДанных_ ([Период], [Номенклатура_ссылка], [НомерСтроки],  [Контрагент_ссылка],[Документ_Ссылка],[Ответственный_Ссылка],  [Редактор_Ссылка],[ЛП_Отгрузки_Ссылка],[ЛП_Отгрузки_Тип_Ссылка],  [ЛП_Получатель_Ссылка],[ЛП_Получатель_Тип_Ссылка],[Регистратор_Номер],  [Регистратор_Тип],[Тип_Отгрузки_Ссылка],[Количество], [БазаДанных]) 
select dateadd(year,-2000,ТоварыНаСкладах.[_Period]) as Период , ТоварыНаСкладах.[_Fld8894RRef] as Номенклатура_ссылка , ТоварыНаСкладах.[_LineNo] as НомерСтроки , РеализацияТоваровУслуг.[_Fld5365RRef] as Контрагент_ссылка , case when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000D6 then РеализацияТоваровУслуг.[_IDRRef] when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000BD then ПеремещениеТоваров.[_IDRRef] when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000BC then ОтчетОРозничныхПродажах.[_IDRRef] when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000F1 then ЧекККМ.[_IDRRef] else РасходныйОрдерНаТовары.[_IDRRef] end  as  Документ_Ссылка  , case when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000D6 then ЗаказПокупателя.[_Fld2597RRef] when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000BD then ПеремещениеТоваров.[_Fld4128RRef] when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000BC then ОтчетОРозничныхПродажах.[_Fld4035RRef] when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000F1 then ЧекККМ.[_Fld6355RRef] end  as Ответственный_Ссылка , case when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000D6 then ЗаказПокупателя.[_Fld2615RRef] when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000BD then ПеремещениеТоваров.[_Fld4141RRef] when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000BC then ОтчетОРозничныхПродажах.[_Fld4044RRef] when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000F1 then ЧекККМ.[_Fld6364RRef] end as Редактор_Ссылка , Склады.[_IDRRef] as ЛП_Отгрузки_Ссылка , Склады.[_Fld9924RRef] as ЛП_Отгрузки_Тип_Ссылка , case when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000D6 then 0x00000000000000000000000000000000 when РасходныйОрдерНаТовары.[_Fld5322_RTRef]= 0x000000BD then Склады1.[_IDRRef] end  as ЛП_Получатель_Ссылка , Склады1.[_Fld9924RRef] as ЛП_Получатель_Тип_Ссылка, case when РасходныйОрдерНаТовары.[_Fld5322_RTRef]=0x000000D6 then РеализацияТоваровУслуг.[_Number] when РасходныйОрдерНаТовары.[_Fld5322_RTRef]=0x000000BD then ПеремещениеТоваров.[_Number] when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000BC then ОтчетОРозничныхПродажах.[_Number] when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000F1 then ЧекККМ.[_Number] end  as Регистратор_Номер , case when РасходныйОрдерНаТовары.[_Fld5322_RTRef]=0x000000D6 then 'РеализацияТоваровУслуг' when РасходныйОрдерНаТовары.[_Fld5322_RTRef]=0x000000BD then 'ПеремещениеТоваров' when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000BC then 'ОтчетОРозничныхПродажах' when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000F1 then 'ЧекККМ' else 'РасходныйОрдер.ДокументОснование '+cast(РасходныйОрдерНаТовары.[_Fld5322_RTRef] as nvarchar(100)) end  as Регистратор_Тип , case when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000D6 then РеализацияТоваровУслуг.[_Fld10278RRef] when РасходныйОрдерНаТовары.[_Fld5322_RTRef] = 0x000000BD then ПеремещениеТоваров.[_Fld10275RRef] end  as  Тип_Отгрузки_Ссылка  , ТоварыНаСкладах.[_Fld8898] as Количество , convert(int, 17) as [БазаДанных] from [Smr_Trade].dbo.[_AccumRg8892] as ТоварыНаСкладах with(nolock) /* рн_ Товары на складах */  left join [Smr_Trade].dbo.[_Document213] as РасходныйОрдерНаТовары with(nolock) on ТоварыНаСкладах.[_RecorderRRef] = РасходныйОрдерНаТовары.[_IDRRef] and ТоварыНаСкладах.[_RecorderTRef] = 0x000000D5/*документ РасходныйОрдерНаТовары*/ left join [Smr_Trade].dbo.[_Document214] as РеализацияТоваровУслуг with(nolock) on РасходныйОрдерНаТовары.[_Fld5322_RRRef] = РеализацияТоваровУслуг.[_IDRRef] and РасходныйОрдерНаТовары.[_Fld5322_RTRef]=0x000000D6 /*документ РеализацияТоваровУслуг*/  left join [Smr_Trade].dbo.[_Document143] as ЗаказПокупателя with(nolock) on РеализацияТоваровУслуг.[_Fld5362_RRRef] = ЗаказПокупателя.[_IDRRef] and РеализацияТоваровУслуг.[_Fld5362_RTRef]=0x0000008F /*документ ЗаказПокупателя*/  left join (select * from openquery([192.168.0.252], 'select * from ##OwnOrg_')) as OwnOrg on РеализацияТоваровУслуг.[_Fld5365RRef] = OwnOrg.КонтрагентСсылка and OwnOrg.[БазаДанных] = convert(int, 17) left join [Smr_Trade].dbo.[_Document189] as ПеремещениеТоваров with(nolock) on РасходныйОрдерНаТовары.[_Fld5322_RRRef] = ПеремещениеТоваров.[_IDRRef] and РасходныйОрдерНаТовары.[_Fld5322_RTRef]=0x000000BD /*документ ПеремещениеТоваров*/  left join [Smr_Trade].dbo.[_Reference80] as Склады1 with(nolock) on ПеремещениеТоваров.[_Fld4134RRef] = Склады1.[_IDRRef] left join [Smr_Trade].dbo.[_Reference80] as Склады with(nolock) on ТоварыНаСкладах.[_Fld8893RRef] = Склады.[_IDRRef] /* справочник складов */  left join [Smr_Trade].dbo.[_Document188] as ОтчетОРозничныхПродажах with(nolock) on РасходныйОрдерНаТовары.[_Fld5322_RRRef] = ОтчетОРозничныхПродажах.[_IDRRef] and РасходныйОрдерНаТовары.[_Fld5322_RTRef]=0x000000BC /*документ ОтчетОРозничныхПродажах*/  left join [Smr_Trade].dbo.[_Document241] as ЧекККМ with(nolock) on РасходныйОрдерНаТовары.[_Fld5322_RRRef] = ЧекККМ.[_IDRRef] and РасходныйОрдерНаТовары.[_Fld5322_RTRef]=0x000000F1 /*Документ ЧекККМ*/  where CONVERT(VARCHAR,DATEADD(YEAR,-2000,CAST(ТоварыНаСкладах.[_Period] AS DATETIME)),121) BETWEEN '2021-08-01 00:00:00.000' AND '2021-08-02 23:59:59.000' and ТоварыНаСкладах.[_RecorderTRef] = 0x000000D5 /*обрабатываем только документы РасходныйОрдерНаТовары*/  and РасходныйОрдерНаТовары.[_Fld5322_RTRef] in (0x000000D6, 0x000000BD) 
/*документы-основания РасходногоОрдераНаТовары: РеализацияТоваровУслуг, ПеремещениеТоваров*/ 
insert into ##СборДанных_ ([Период], [Номенклатура_ссылка],[НомерСтроки],  [Контрагент_ссылка],[Документ_Ссылка],[Ответственный_Ссылка],  [Редактор_Ссылка],[ЛП_Отгрузки_Ссылка],[ЛП_Отгрузки_Тип_Ссылка],  [ЛП_Получатель_Ссылка],[ЛП_Получатель_Тип_Ссылка],[Регистратор_Номер],  [Регистратор_Тип],[Тип_Отгрузки_Ссылка],[Количество], [БазаДанных]) 
select dateadd(year,-2000,ТоварыНаСкладах1.[_Period]) as Период , ТоварыНаСкладах1.[_Fld8894RRef] , ТоварыНаСкладах1.[_LineNo] , null , ПеремещениеТоваров1.[_IDRRef] , ПеремещениеТоваров1.[_Fld4128RRef] , ПеремещениеТоваров1.[_Fld4141RRef] , Склады2.[_IDRRef] as ЛП_Отгрузки_Ссылка , Склады2.[_Fld9924RRef] as ЛП_Отгрузки_Тип_Ссылка , Склады3.[_IDRRef] as ЛП_Получатель_Ссылка , Склады3.[_Fld9924RRef] as ЛП_Получатель_Тип_Ссылка , ПеремещениеТоваров1.[_Number] as Регистратор_Номер , 'ПеремещениеТоваров' as Регистратор_Тип , ПеремещениеТоваров1.[_Fld10275RRef] as Тип_Отгрузки_Ссылка , ТоварыНаСкладах1.[_Fld8898] as Количество , convert(int, 17) as [БазаДанных] from [Smr_Trade].dbo.[_AccumRg8892] as ТоварыНаСкладах1 with(nolock) /*рн_ Товары на складах*/ left join [Smr_Trade].dbo.[_Document189] as ПеремещениеТоваров1 with(nolock) on ТоварыНаСкладах1.[_RecorderRRef] = ПеремещениеТоваров1.[_IDRRef] and  ТоварыНаСкладах1.[_RecorderTRef] = 0x000000BD left join [Smr_Trade].dbo.[_Reference80] as Склады3 with(nolock) on ПеремещениеТоваров1.[_Fld4134RRef] = Склады3.[_IDRRef] left join [Smr_Trade].dbo.[_Reference80] as Склады2 with(nolock) on ТоварыНаСкладах1.[_Fld8893RRef] = Склады2.[_IDRRef] /*справочник складов*/  where CONVERT(VARCHAR,DATEADD(YEAR,-2000,CAST(ТоварыНаСкладах1.[_Period] AS DATETIME)),121) BETWEEN '2021-08-01 00:00:00.000' AND '2021-08-02 23:59:59.000' and ТоварыНаСкладах1.[_RecorderTRef] = 0x000000BD and ТоварыНаСкладах1.[_RecordKind] = 1
insert into ##СборДанных_ ([Период], [Номенклатура_ссылка],[НомерСтроки],  [Контрагент_ссылка],[Документ_Ссылка],[Ответственный_Ссылка],  [Редактор_Ссылка],[ЛП_Отгрузки_Ссылка],[ЛП_Отгрузки_Тип_Ссылка],  [ЛП_Получатель_Ссылка],[ЛП_Получатель_Тип_Ссылка],[Регистратор_Номер],  [Регистратор_Тип],[Тип_Отгрузки_Ссылка],[Количество], [БазаДанных]) 
select dateadd(year,-2000,ТоварыНаСкладах2.[_Period]) as Период  , ТоварыНаСкладах2.[_Fld8894RRef] , ТоварыНаСкладах2.[_LineNo] , РеализацияТоваровУслуг1.[_Fld5365RRef] as Контрагент_ссылка  , case when ТоварыНаСкладах2.[_RecorderTRef] = 0x000000D6 then РеализацияТоваровУслуг1.[_IDRRef] else ТоварыНаСкладах2.[_RecorderRRef] end  as  Документ_Ссылка  , case when ТоварыНаСкладах2.[_RecorderTRef] = 0x000000D6 then ЗаказПокупателя1.[_Fld2597RRef] when ТоварыНаСкладах2.[_RecorderTRef] = 0x000000BC then ОтчетОРозничныхПродажах1.[_Fld4035RRef] when ТоварыНаСкладах2.[_RecorderTRef] = 0x000000F1 then ЧекККМ1.[_Fld6355RRef] end Ответственный_Ссылка , case when ТоварыНаСкладах2.[_RecorderTRef] = 0x000000D6 then ЗаказПокупателя1.[_Fld2615RRef] when ТоварыНаСкладах2.[_RecorderTRef] = 0x000000BC then ОтчетОРозничныхПродажах1.[_Fld4044RRef] when ТоварыНаСкладах2.[_RecorderTRef] = 0x000000F1 then ЧекККМ1.[_Fld6364RRef] end Редактор_Ссылка , Склады4.[_IDRRef] as ЛП_Отгрузки_Ссылка  , Склады4.[_Fld9924RRef] as ЛП_Отгрузки_Тип_Ссылка  , 0x00000000000000000000000000000000 as ЛП_Получатель_Ссылка , 0x00000000000000000000000000000000 as ЛП_Получатель_Тип_Ссылка , case when ТоварыНаСкладах2.[_RecorderTRef] = 0x000000D6 then РеализацияТоваровУслуг1.[_Number] when ТоварыНаСкладах2.[_RecorderTRef] = 0x000000BC then ОтчетОРозничныхПродажах1.[_Number] when ТоварыНаСкладах2.[_RecorderTRef] = 0x000000F1 then ЧекККМ1.[_Number] end Регистратор_Номер , case when ТоварыНаСкладах2.[_RecorderTRef] = 0x000000D6 then 'РеализацияТоваровУслуг' when ТоварыНаСкладах2.[_RecorderTRef] = 0x000000BC then 'ОтчетОРозничныхПродажах' when ТоварыНаСкладах2.[_RecorderTRef] = 0x000000F1 then 'ЧекККМ' else 'ТоварыНаСкладах.Регистратор '+CAST(ТоварыНаСкладах2.[_RecorderTRef] as nvarchar(100)) end as Регистратор_Тип , РеализацияТоваровУслуг1.[_Fld10278RRef] as Тип_Отгрузки_Ссылка , ТоварыНаСкладах2.[_Fld8898] , convert(int, 17) as [БазаДанных] from [Smr_Trade].dbo.[_AccumRg8892] as ТоварыНаСкладах2 with(nolock) /*рн_ Товары на складах*/  left join [Smr_Trade].dbo.[_Document214] as РеализацияТоваровУслуг1 with(nolock) on ТоварыНаСкладах2.[_RecorderRRef] = РеализацияТоваровУслуг1.[_IDRRef] and ТоварыНаСкладах2.[_RecorderTRef] = 0x000000D6 left join [Smr_Trade].dbo.[_Document143] as ЗаказПокупателя1 with(nolock) on РеализацияТоваровУслуг1.[_Fld5362_RRRef] = ЗаказПокупателя1.[_IDRRef] and РеализацияТоваровУслуг1.[_Fld5362_RTRef] = 0x0000008F/*документ ЗаказПокупателя*/ left join (select * from openquery([192.168.0.252], 'select * from ##OwnOrg_')) as OwnOrg on РеализацияТоваровУслуг1.[_Fld5365RRef] = OwnOrg.КонтрагентСсылка and OwnOrg.[БазаДанных] = convert(int, 17) left join [Smr_Trade].dbo.[_Reference80] as Склады4 with(nolock) on ТоварыНаСкладах2.[_Fld8893RRef] = Склады4.[_IDRRef] left join [Smr_Trade].dbo.[_Document188] as ОтчетОРозничныхПродажах1 with(nolock) on ТоварыНаСкладах2.[_RecorderRRef] = ОтчетОРозничныхПродажах1.[_IDRRef] and ТоварыНаСкладах2.[_RecorderTRef]=0x000000BC /*документ ОтчетОРозничныхПродажах*/  left join [Smr_Trade].dbo.[_Document241] as ЧекККМ1 with(nolock) on ТоварыНаСкладах2.[_RecorderRRef] = ЧекККМ1.[_IDRRef] and ТоварыНаСкладах2.[_RecorderTRef]=0x000000F1 /*Документ ЧекККМ*/  where CONVERT(VARCHAR,DATEADD(YEAR,-2000,CAST(ТоварыНаСкладах2.[_Period] AS DATETIME)),121) BETWEEN '2021-08-01 00:00:00.000' AND '2021-08-02 23:59:59.000' and  ТоварыНаСкладах2.[_RecorderTRef] in (0x000000D6, 0x000000BC, 0x000000F1)
insert into ##СборДанных_ ([Период], [Номенклатура_ссылка],[НомерСтроки],  [Контрагент_ссылка], [Документ_Ссылка], [Ответственный_Ссылка],  [Редактор_Ссылка], [ЛП_Отгрузки_Ссылка], [ЛП_Отгрузки_Тип_Ссылка],  [ЛП_Получатель_Ссылка], [ЛП_Получатель_Тип_Ссылка], [Регистратор_Номер],  [Регистратор_Тип], [Тип_Отгрузки_Ссылка], [Количество], [БазаДанных]) 
select dateadd(year,-2000,ТоварыНаСкладах3.[_Period]) as  Период  , ТоварыНаСкладах3.[_Fld8894RRef] , ТоварыНаСкладах3.[_LineNo] , м_ЛЗК.[_Fld6590RRef] as Контрагент_ссылка , м_ЛЗК.[_IDRRef] , КомплектацияНоменклатуры.[_Fld11026RRef] , КомплектацияНоменклатуры.[_Fld3340RRef] , Склады5.[_IDRRef] as ЛП_Отгрузки_Ссылка , Склады5.[_Fld9924RRef] as ЛП_Отгрузки_Тип_Ссылка , 0x00000000000000000000000000000000 as ЛП_Получатель_Ссылка , 0x00000000000000000000000000000000 as ЛП_Получатель_Тип_Ссылка , КомплектацияНоменклатуры.[_Number] , 'КомплектацияНоменклатуры' , 0x00000000000000000000000000000000 as Тип_Отгрузки_Ссылка , ТоварыНаСкладах3.[_Fld8898] , convert(int, 17) as [БазаДанных] from [Smr_Trade].dbo.[_AccumRg8892] as ТоварыНаСкладах3 with(nolock) /*рн_ Товары на складах*/ left join [Smr_Trade].dbo.[_Document169] as КомплектацияНоменклатуры with(nolock) on ТоварыНаСкладах3.[_RecorderRRef] = КомплектацияНоменклатуры.[_IDRRef] and ТоварыНаСкладах3.[_RecorderTRef] = 0x000000A9 left join [Smr_Trade].dbo.[_Document246] as м_ЛЗК with(nolock) on КомплектацияНоменклатуры.[_Fld3349_RRRef] = м_ЛЗК.[_IDRRef] and КомплектацияНоменклатуры.[_Fld3349_RTRef]=0x000000F6/*документ м_ЛЗК (лимитно-заборная карта)*/ left join (select * from openquery([192.168.0.252], 'select * from ##OwnOrg_')) as OwnOrg on м_ЛЗК.[_Fld6590RRef]=OwnOrg.КонтрагентСсылка and OwnOrg.[БазаДанных] = convert(int, 17) left join [Smr_Trade].dbo.[_Reference80] as Склады5 with(nolock)  on ТоварыНаСкладах3.[_Fld8893RRef] = Склады5.[_IDRRef]/*справочник складов*/ where CONVERT(VARCHAR,DATEADD(YEAR,-2000,CAST(ТоварыНаСкладах3.[_Period] AS DATETIME)),121) BETWEEN '2021-08-01 00:00:00.000' AND '2021-08-02 23:59:59.000' and ТоварыНаСкладах3.[_RecorderTRef] = 0x000000A9 and ТоварыНаСкладах3.[_RecordKind] = 1/*ВидДвиженияПриходРасход регистра = Расход*/
--insert into ##_Продажи  ([БазаДанных], [Период], [Артикул],   [Номенклатура_ссылка], [НомерСтроки], [Документ_Ссылка],  [Контрагент_ссылка], [Ответственный_Ссылка], [Редактор_Ссылка],   [ЛП_Отгрузки_Ссылка], [ЛП_Отгрузки_Тип_Ссылка], [ЛП_Получатель_Ссылка],   [ЛП_Получатель_Тип_Ссылка], [Регистратор_Номер], [Регистратор_Тип], [Тип_Отгрузки_Ссылка],   [Количество], [Стоимость], [СтоимостьБезСкидок], [СтоимостьВБазовыхММЦенах], [Дата_Актуальности])  select distinct СборДанных.БазаДанных , СборДанных.Период , Номенклатура.Артикул as  Артикул , СборДанных.Номенклатура_ссылка , СборДанных.НомерСтроки , СборДанных.Документ_Ссылка , СборДанных.Контрагент_ссылка , СборДанных.Ответственный_Ссылка , СборДанных.Редактор_Ссылка , СборДанных.ЛП_Отгрузки_Ссылка , СборДанных.ЛП_Отгрузки_Тип_Ссылка , СборДанных.ЛП_Получатель_Ссылка , СборДанных.ЛП_Получатель_Тип_Ссылка , СборДанных.Регистратор_Номер , СборДанных.Регистратор_Тип , СборДанных.Тип_Отгрузки_Ссылка , СборДанных.Количество ,  case when СборДанных.Регистратор_Тип = 'РеализацияТоваровУслуг' and RegSal.Количество>0 then (RegSal.Стоимость/RegSal.Количество)*СборДанных.Количество when СборДанных.Регистратор_Тип = 'КомплектацияНоменклатуры' and DocLzk.Количество>0 then (DocLzk.СуммаСНДС/DocLzk.Количество)*СборДанных.Количество when СборДанных.Регистратор_Тип = 'ПеремещениеТоваров' and DocPer.Количество>0 then (DocPer.СуммаПеремещения/DocPer.Количество)*СборДанных.Количество when СборДанных.Регистратор_Тип = 'ОтчетОРозничныхПродажах' and RoznSl.Количество>0 then (RoznSl.Сумма/RoznSl.Количество)*СборДанных.Количество when СборДанных.Регистратор_Тип = 'ЧекККМ' and CheKKM.Количество>0 then (CheKKM.Сумма/CheKKM.Количество)*СборДанных.Количество else 0 end Стоимость ,  case when СборДанных.Регистратор_Тип = 'РеализацияТоваровУслуг' and RegSal.Количество>0 then (RegSal.СтоимостьБезСкидок/RegSal.Количество)*СборДанных.Количество when СборДанных.Регистратор_Тип = 'КомплектацияНоменклатуры' and DocLzk.Количество>0 then (DocLzk.СуммаСНДС/DocLzk.Количество)*СборДанных.Количество when СборДанных.Регистратор_Тип = 'ПеремещениеТоваров' and DocPer.Количество>0 then (DocPer.СуммаПеремещения/DocPer.Количество)*СборДанных.Количество when СборДанных.Регистратор_Тип = 'ОтчетОРозничныхПродажах' and RoznSl.Количество>0 then (RoznSl.СуммаБезСкидок/RoznSl.Количество)*СборДанных.Количество when СборДанных.Регистратор_Тип = 'ЧекККМ' and CheKKM.Количество>0 then (CheKKM.СуммаБезСкидки/CheKKM.Количество)*СборДанных.Количество end СтоимостьБезСкидок ,  case when СборДанных.Регистратор_Тип = 'РеализацияТоваровУслуг' and RegSal.Количество>0 then (RegSal.СтоимостьВБазовыхММЦенах/RegSal.Количество)*СборДанных.Количество when СборДанных.Регистратор_Тип = 'ПеремещениеТоваров' and DocPer.Количество>0 then (DocPer.СуммаПеремещения/DocPer.Количество)*СборДанных.Количество end СтоимостьВБазовыхММЦенах , convert(smalldatetime, getdate()) as [Дата_Актуальности] FROM ##СборДанных_ as СборДанных left join ##RegSal_ as RegSal on СборДанных.Документ_Ссылка = RegSal.ДокументПродажи_Ссылка and СборДанных.Номенклатура_ссылка = RegSal.Номенклатура_Ссылка and СборДанных.[БазаДанных] = RegSal.[БазаДанных] left join ##DocLzk_ as DocLzk on СборДанных.Документ_Ссылка = DocLzk.м_ЛЗК_Ссылка and СборДанных.Номенклатура_ссылка = DocLzk.Номенклатура_Ссылка and СборДанных.[БазаДанных] = DocLzk.[БазаДанных] left join ##DocPer_ as DocPer on СборДанных.Документ_Ссылка = DocPer.Перемещение_Ссылка and СборДанных.Номенклатура_ссылка = DocPer.Номенклатура_Ссылка and СборДанных.[БазаДанных] = DocPer.[БазаДанных] left join ##RoznSl_ as RoznSl on СборДанных.Документ_Ссылка = RoznSl.ОтчетОРозничныхПродажах_Ссылка and СборДанных.Номенклатура_ссылка = RoznSl.Номенклатура_Ссылка and СборДанных.[БазаДанных] = RoznSl.[БазаДанных] left join ##CheKKM_ as CheKKM on СборДанных.Документ_Ссылка = CheKKM.ЧекККМ_Ссылка and СборДанных.Номенклатура_ссылка = CheKKM.Номенклатура_Ссылка and СборДанных.[БазаДанных] = CheKKM.[БазаДанных] left join etl_1c_sql.dbo.Table_Справочник_Номенклатура as Номенклатура  on СборДанных.Номенклатура_ссылка = Номенклатура.Ссылка_ID /*--справочник Номенклатура*/
select * from ##СборДанных_ 