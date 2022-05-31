   --alter database MINIMAKS_SQL set Single_user   with rollback 
use MINIMAKS_SQL

   
declare @ReportDate as nvarchar(10), @MnOne as nvarchar(10), @MnTwo as nvarchar(10)
set @ReportDate='2020-11-01'-- convert(nvarchar (10),DATEADD(month, DATEDIFF(month, -1, getdate()) - 2, 0) ,121)


set @MnOne = CONVERT(nvarchar(10), dateadd(month, -2, convert(date, @ReportDate))) 
set @MnTwo = CONVERT(nvarchar(10), dateadd(month, -1, convert(date, @ReportDate))) 
print @ReportDate + '; ' + @MnOne + '; ' + @MnTwo + '; ' + convert(nvarchar(10), dateadd(dd, -1, dateadd(month, 1, convert(date, @ReportDate))))



delete from minimaks_sql.dbo.�����������������������_��� 
where [���� ������]=convert(date, @ReportDate)


declare @qwr as nvarchar(max), @qwr1 as nvarchar(max), @qwr2 as nvarchar(max)
declare @qwr3 as nvarchar(max), @qwr4 as nvarchar(max), @qwr5 as nvarchar(max)
declare @qwr6 as nvarchar(max), @qwr7 as nvarchar(max), @qwr8 as nvarchar(max)
declare @dtWhF as nvarchar(255), @dtWhL as nvarchar(255)
declare @SalesPeriodT as NVarChar(255), @SalesPeriodO as NVarChar(255)
--declare @ReportDate as NVarChar(15)


set @dtWhF='[Dim_���������].[����].&[' + @MnOne + 'T00:00:00]' --��������� ���� ��������
set @dtWhL='[Dim_���������].[����].&[' + convert(nvarchar(10), dateadd(dd, -1, dateadd(month, 1, convert(date, @ReportDate)))) + 'T00:00:00]' --�������� ���� ��������
-- ������ ������ 3 ������
set @SalesPeriodT='[Dim_���������].[�����].&['+ @MnOne +'T00:00:00], [Dim_���������].[�����].&['+ @MnTwo +'T00:00:00], [Dim_���������].[�����].&[' + @ReportDate + 'T00:00:00]'
-- ��������� ������ ������
set @SalesPeriodO='[Dim_���������].[�����].&[' + @ReportDate +'T00:00:00]'
--set @ReportDate = '2021-02-01'

print @dtWhF
print @dtWhL
print @SalesPeriodT
print @SalesPeriodO





set @qwr = 'insert into minimaks_sql.dbo.�����������������������_��� ([���� ������],[���������],[�����],[����������������],[����������������],[�������],[����������_����������],[����������_�������������],[����������_����������],[����������_�������������],[�������1���_����������],[�������1���_�������������],[�������1���_���������],[�������3���_����������],[�������3���_�������������],[�������3���_���������],[��������1���_����������],[��������1���_�������������],[��������1���_���������],[��������3���_����������],[��������3���_�������������],[��������3���_���������]) ' + 
'select ''' + @ReportDate + ''' as [���� ������], ' +
'���������, ' + 
'�����, ' + 
'����������������, ' + 
'����������������, ' + 
'�������, ' + 
'SUM(����������_����������) as ����������_����������, ' + 
'SUM(����������_�������������) as ����������_�������������, ' + 
'sum(����������_����������) as ����������_����������, ' + 
'sum(����������_�������������) as ����������_�������������, ' + 
'sum(�������1���_����������) as �������1���_����������, ' + 
'sum(�������1���_�������������) as �������1���_�������������, ' + 
'sum(�������1���_���������) as �������1���_���������, ' + 
'sum(�������3���_����������) as �������3���_����������, ' + 
'sum(�������3���_�������������) as �������3���_�������������, ' + 
'sum(�������3���_���������) as �������3���_���������, ' + 
'sum(��������1���_����������) as ��������1���_����������, ' + 
'sum(��������1���_�������������) as ��������1���_�������������, ' + 
'sum(��������1���_���������) as ��������1���_���������, ' + 
'sum(��������3���_����������) as ��������3���_����������, ' + 
'sum(��������3���_�������������) as ��������3���_�������������, ' + 
'sum(��������3���_���������) as ��������3���_��������� ' + 

--'into minimaks_sql.dbo.�����������������������_��� ' + 

'from ' + 
'( ' + 
'select ' + 
'''��������'' as ���������, ' + 
'cast([[Dim_�����������]].[�����������_�����_������������]].[�����������_�����_������������]].[MEMBER_CAPTION]]] as nvarchar(100)) as �����, ' + 
'cast([[Dim_�����������]].[�����������_�������������_���]].[�����������_�������������_���]].[MEMBER_CAPTION]]] as nvarchar(20)) as ����������������, ' + 
'cast([[Dim_�����������]].[�����������_�������������_������������]].[�����������_�������������_������������]].[MEMBER_CAPTION]]] as nvarchar(100)) as ����������������, ' + 
'CAST([[Dim_������������]].[������������_�������]].[������������_�������]].[MEMBER_CAPTION]]] as nvarchar(20)) as �������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������_����������_����������]]] as nvarchar)) as ����������_����������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������_���������_����������_�_���]]] as nvarchar)) as ����������_�������������, ' + 
'0 as �������3���_����������, ' + 
'0 as �������3���_�������������, ' + 
'0 as �������3���_���������, ' + 
'0 as ��������3���_����������, ' + 
'0 as ��������3���_�������������, ' + 
'0 as ��������3���_���������, ' + 
'0 as �������1���_����������, ' + 
'0 as �������1���_�������������, ' + 
'0 as �������1���_���������, ' + 
'0 as ��������1���_����������, ' + 
'0 as ��������1���_�������������, ' + 
'0 as ��������1���_���������, ' + 
'0 as ����������_����������, ' + 
'0 as ����������_������������� '  


set @qwr1 = 'from openquery(SSAS_DATA_ANALYSIS, '' SELECT NON EMPTY { [Measures].[�������_����������_����������], ' + 
'[Measures].[�������_���������_����������_�_���] } ON COLUMNS ' + 
', NON EMPTY { (' + @dtWhF + ', ' + 
'[Dim_�����������].[�����������_�����_������������].[�����������_�����_������������].ALLMEMBERS * ' + 
'[Dim_�����������].[�����������_�������������_���].[�����������_�������������_���].ALLMEMBERS * ' + 
'[Dim_�����������].[�����������_�������������_������������].[�����������_�������������_������������].ALLMEMBERS * ' + 
'[Dim_������������].[������������_�������].[������������_�������].ALLMEMBERS * ' + 
'[Dim_������������].[������������_������������].[������������_������������].ALLMEMBERS ) } ' + 
'having ( [Measures].[�������_����������_����������]>0 and [Measures].[�������_���������_����������_�_���]>=0) ON ROWS ' + 
'FROM ( SELECT ( { [Dim_�����������].[�����������_�������������_�������_�������������].&[��] } ) ON COLUMNS ' + 
'FROM ( SELECT ( { { [Dim_�����������].[�����������_�����_���].&[101. ������� �������], [Dim_�����������].[�����������_�����_���].&[106. �������] } } ) ON COLUMNS ' + 
'FROM ( SELECT ( { ' + @dtWhF + ' } ) ON COLUMNS ' + 
'FROM [DATA_ANALYSIS]))) ' + 
'WHERE ( [Dim_�����������].[�����������_�����_���].CurrentMember, [Dim_�����������].[�����������_�������������_�������_�������������].&[��] ) '') ' 

set @qwr2 = 'union ' + 

'select ' + 
'''��������������'' as ���������, ' + 
'cast([[Dim_�����������]].[�����������_�����_������������]].[�����������_�����_������������]].[MEMBER_CAPTION]]] as nvarchar(100)) as �����, ' + 
'cast([[Dim_�����������]].[�����������_�������������_���]].[�����������_�������������_���]].[MEMBER_CAPTION]]] as nvarchar(20)) as ����������������, ' + 
'cast([[Dim_�����������]].[�����������_�������������_������������]].[�����������_�������������_������������]].[MEMBER_CAPTION]]] as nvarchar(100)) as ����������������, ' + 
'CAST([[Dim_������������]].[������������_�������]].[������������_�������]].[MEMBER_CAPTION]]] as nvarchar(20)) as �������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������_����������_����������]]] as nvarchar)) as ����������_����������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������_���������_����������_�_���]]] as nvarchar)) as ����������_�������������, ' + 
'0 as �������3���_����������, ' + 
'0 as �������3���_�������������, ' + 
'0 as �������3���_���������, ' + 
'0 as ��������3���_����������, ' + 
'0 as ��������3���_�������������, ' + 
'0 as ��������3���_���������, ' + 
'0 as �������1���_����������, ' + 
'0 as �������1���_�������������, ' + 
'0 as �������1���_���������, ' + 
'0 as ��������1���_����������, ' + 
'0 as ��������1���_�������������, ' + 
'0 as ��������1���_���������, ' + 
'0 as ����������_����������, ' + 
'0 as ����������_������������� ' + 

'from openquery(SSAS_DATA_ANALYSIS, '' SELECT NON EMPTY { [Measures].[�������_����������_����������], ' + 
'[Measures].[�������_���������_����������_�_���] } ON COLUMNS ' + 
', NON EMPTY { (' + @dtWhF + ', ' + 
'[Dim_�����������].[�����������_�����_������������].[�����������_�����_������������].ALLMEMBERS * ' + 
'[Dim_�����������].[�����������_�������������_���].[�����������_�������������_���].ALLMEMBERS * ' + 
'[Dim_�����������].[�����������_�������������_������������].[�����������_�������������_������������].ALLMEMBERS * ' + 
'[Dim_������������].[������������_�������].[������������_�������].ALLMEMBERS * ' + 
'[Dim_������������].[������������_������������].[������������_������������].ALLMEMBERS ) } ' + 
'having ( [Measures].[�������_����������_����������]>0 and [Measures].[�������_���������_����������_�_���]>=0) ON ROWS ' + 
'FROM ( SELECT ( { [Dim_�����������].[�����������_�������������_�������_�������������].&[��] } ) ON COLUMNS ' + 
'FROM ( SELECT ( { { [Dim_�����������].[�����������_�����_���].&[102. �������], [Dim_�����������].[�����������_�����_���].&[103. ����], [Dim_�����������].[�����������_�����_���].&[104. ������], [Dim_�����������].[�����������_�����_���].&[109. ������������] } } ) ON COLUMNS ' + 
'FROM ( SELECT ( { ' + @dtWhF + ' } ) ON COLUMNS ' + 
'FROM [DATA_ANALYSIS]))) ' + 
'WHERE ( [Dim_�����������].[�����������_�����_���].CurrentMember, [Dim_�����������].[�����������_�������������_�������_�������������].&[��] ) '') ' 

set @qwr3= 'union ' + 

'select ' + 
'''��������'' as ���������, ' + 
'cast([[Dim_�����������]].[�����������_�����_������������]].[�����������_�����_������������]].[MEMBER_CAPTION]]] as nvarchar(100)) as �����, ' + 
'cast([[Dim_�����������]].[�����������_�������������_���]].[�����������_�������������_���]].[MEMBER_CAPTION]]] as nvarchar(20)) as ����������������, ' + 
'cast([[Dim_�����������]].[�����������_�������������_������������]].[�����������_�������������_������������]].[MEMBER_CAPTION]]] as nvarchar(100)) as ����������������, ' + 
'CAST([[Dim_������������]].[������������_�������]].[������������_�������]].[MEMBER_CAPTION]]] as nvarchar(20)) as �������, ' + 
'0 as ����������_����������, ' + 
'0 as ����������_�������������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������_����������]]] as nvarchar)) as �������3���_����������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������_�������������_�_���]]] as nvarchar)) as �������3���_�������������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������_���������]]] as nvarchar)) as �������3���_���������, ' + 
'0 as ��������3���_����������, ' + 
'0 as ��������3���_�������������, ' + 
'0 as ��������3���_���������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������������������_����������]]] as nvarchar)) as �������1���_����������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������������������_�������������]]] as nvarchar)) as �������1���_�������������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������������������_���������]]] as nvarchar)) as �������1���_���������, ' + 
'0 as ��������1���_����������, ' + 
'0 as ��������1���_�������������, ' + 
'0 as ��������1���_���������, ' + 
'0 as ����������_����������, ' + 
'0 as ����������_������������� ' + 

'from openquery(SSAS_DATA_ANALYSIS, ''with ' +
												 'member measures.[�������������������_����������] as sum(' + @SalesPeriodO + ', [Measures].[�������_����������])' +
												 'member measures.[�������������������_���������] as sum(' + @SalesPeriodO + ', [Measures].[�������_���������]) ' +
												 'member measures.[�������������������_�������������] as sum(' + @SalesPeriodO + ', [Measures].[�������_�������������_�_���]) ' +

											 'SELECT ' +
													'NON EMPTY { '+
																'[Measures].[�������_����������], ' +
																'[Measures].[�������_�������������_�_���], ' +
																'[Measures].[�������_���������] ' +
																', ' +
																'measures.[�������������������_����������], ' +
																'measures.[�������������������_���������], ' +
																'measures.[�������������������_�������������] ' +
																'} ON COLUMNS ' +
												  ', NON EMPTY { ([Dim_�����������].[�����������_�����_������������].[�����������_�����_������������].ALLMEMBERS * ' +
																 '[Dim_�����������].[�����������_�������������_���].[�����������_�������������_���].ALLMEMBERS * ' +
																 '[Dim_�����������].[�����������_�������������_������������].[�����������_�������������_������������].ALLMEMBERS * ' +
																 '[Dim_������������].[������������_�������].[������������_�������].ALLMEMBERS * ' + 
																 '[Dim_������������].[������������_������������].[������������_������������].ALLMEMBERS ) ' +
																 '}  ON ROWS ' 
set @qwr4= 											'FROM ( '+
											    --    'SELECT ( { [Dim_�����������].[�����������_�������_������������].&[���], ' + 
												--			  '[Dim_�����������].[�����������_�������_������������].[All].UNKNOWNMEMBER } ) ON COLUMNS ' + 
												--   'FROM (' 
												   'SELECT ( { [Dim_���������].[���������_�������_����������_����������_�������].&[���] } ) ON COLUMNS '  + 
														  'FROM ( SELECT ( { [Dim_���������].[���������_���].&[�����������������������], ' + 
																			'[Dim_���������].[���������_���].&[����������������������], ' + 
																			'[Dim_���������].[���������_���].&[������] } ) ON COLUMNS ' +
																' FROM ( SELECT ( { [Dim_�����������].[�����������_�������������_�������_�������������].&[��] } ) ON COLUMNS ' +
																		'FROM ( SELECT ( { ' + @SalesPeriodT + ' } ) ON COLUMNS ' +
																			   'FROM [DATA_ANALYSIS])))) ' + --')'
											'WHERE ( [Dim_���������].[�����].CurrentMember, ' +
													'[Dim_�����������].[�����������_�������������_�������_�������������].&[��], ' +
													'[Dim_���������].[���������_���].CurrentMember, ' +
													'[Dim_���������].[���������_�������_����������_����������_�������].&[���], ' + 
													--'[Dim_�����������].[�����������_�������_������������].CurrentMember, ' + 
													'{([Dim_�����������].[�����������_���].[�����������_���].members, { [Dim_�����������].[�����������_�������_������������].&[���], [Dim_�����������].[�����������_�������_������������].[All].UNKNOWNMEMBER }), ' + -- ��������� 05/10/20
													'({[Dim_�����������].[�����������_���].&[7814605174], [Dim_�����������].[�����������_���].&[7804339445]}, [Dim_�����������].[�����������_�������_������������].&[��])}' + -- ��������� 05/10/20
													') '')  '    

set @qwr5= 'union ' +

'select ' + 
      '''��������''  as ���������, ' +
'cast([[Dim_�����������]].[�����������_�����_������������]].[�����������_�����_������������]].[MEMBER_CAPTION]]] as nvarchar(100)) as �����, ' + 
'cast([[Dim_�����������]].[�����������_�������������_���]].[�����������_�������������_���]].[MEMBER_CAPTION]]] as nvarchar(20)) as ����������������, ' + 
'cast([[Dim_�����������]].[�����������_�������������_������������]].[�����������_�������������_������������]].[MEMBER_CAPTION]]] as nvarchar(100)) as ����������������, ' + 
'CAST([[Dim_������������]].[������������_�������]].[������������_�������]].[MEMBER_CAPTION]]] as nvarchar(20)) as �������, ' + 
'0 as ����������_����������, ' + 
'0 as ����������_�������������, ' + 
'0 as �������3���_����������, ' + 
'0 as �������3���_�������������, ' + 
'0 as �������3���_���������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������_����������]]] as nvarchar)) as ��������3���_����������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������_�������������_�_���]]] as nvarchar)) as ��������3���_�������������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������_���������]]] as nvarchar)) as ��������3���_���������, ' + 
'0 as �������1���_����������, ' + 
'0 as �������1���_�������������, ' + 
'0 as �������1���_���������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������������������_����������]]] as nvarchar)) as ��������1���_����������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������������������_�������������]]] as nvarchar)) as ��������1���_�������������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������������������_���������]]] as nvarchar)) as ��������1���_���������, ' + 
'0 as ����������_����������, ' + 
'0 as ����������_������������� ' + 

'from openquery(SSAS_DATA_ANALYSIS, ''with ' + 
												'member measures.[�������������������_����������] as sum(' + @SalesPeriodO + ', [Measures].[�������_����������]) ' +
												'member measures.[�������������������_���������] as sum(' + @SalesPeriodO + ', [Measures].[�������_���������]) ' +
												'member measures.[�������������������_�������������] as sum(' + @SalesPeriodO + ', [Measures].[�������_�������������_�_���]) ' +

											 'SELECT ' +
													'NON EMPTY {' +
																'[Measures].[�������_����������], ' +
																'[Measures].[�������_�������������_�_���], ' +
																'[Measures].[�������_���������]' +
																', '+
																'measures.[�������������������_����������], '+
																'measures.[�������������������_���������], '+
																'measures.[�������������������_�������������] ' +
																'} ON COLUMNS '+
												  ', NON EMPTY { ([Dim_�����������].[�����������_�����_������������].[�����������_�����_������������].ALLMEMBERS * '+
																 '[Dim_�����������].[�����������_�������������_���].[�����������_�������������_���].ALLMEMBERS * '+
																 '[Dim_�����������].[�����������_�������������_������������].[�����������_�������������_������������].ALLMEMBERS * '+
																 '[Dim_������������].[������������_�������].[������������_�������].ALLMEMBERS * '+
																 '[Dim_������������].[������������_������������].[������������_������������].ALLMEMBERS ) '+
																 '}  ON ROWS '+
											'FROM (  SELECT ( { [Dim_���������].[���������_�������_����������_����������_�������].&[���] } ) ON COLUMNS '+
														  'FROM ( SELECT ( { [Dim_���������].[���������_���].&[�����������������������], '+
														                    '[Dim_���������].[���������_���].&[����������������������], '+
														                    '[Dim_���������].[���������_���].&[������], '+
														                    '[Dim_���������].[���������_���].&[������������������]  } ) ON COLUMNS '+
																 'FROM ( SELECT ( { [Dim_�����������].[�����������_�������������_�������_�������������].&[��] } ) ON COLUMNS '+
																		'FROM ( SELECT ( { ' + @SalesPeriodT + ' } ) ON COLUMNS '+
																			   'FROM [DATA_ANALYSIS])))) '+
											'WHERE ( [Dim_���������].[�����].CurrentMember, '+
													'[Dim_�����������].[�����������_�������������_�������_�������������].&[��], '+
													'[Dim_���������].[���������_���].CurrentMember, '+
													'[Dim_���������].[���������_�������_����������_����������_�������].&[���], '+
													'[Dim_�����������].[�����������_�������_������������].CurrentMember ) '') '

-- ������� �� ����� �������
set @qwr6= 'union '+

'select ' +
'''��������'' as ���������, ' + 
'cast([[Dim_�����������]].[�����������_�����_������������]].[�����������_�����_������������]].[MEMBER_CAPTION]]] as nvarchar(100)) as �����, ' + 
'cast([[Dim_�����������]].[�����������_�������������_���]].[�����������_�������������_���]].[MEMBER_CAPTION]]] as nvarchar(20)) as ����������������, ' + 
'cast([[Dim_�����������]].[�����������_�������������_������������]].[�����������_�������������_������������]].[MEMBER_CAPTION]]] as nvarchar(100)) as ����������������, ' + 
'CAST([[Dim_������������]].[������������_�������]].[������������_�������]].[MEMBER_CAPTION]]] as nvarchar(20)) as �������, ' + 
'0 as ����������_����������, ' + 
'0 as ����������_�������������, ' + 
'0 as �������3���_����������, ' + 
'0 as �������3���_�������������, ' + 
'0 as �������3���_���������, ' + 
'0 as ��������3���_����������, ' + 
'0 as ��������3���_�������������, ' + 
'0 as ��������3���_���������, ' + 
'0 as �������1���_����������, ' + 
'0 as �������1���_�������������, ' + 
'0 as �������1���_���������, ' + 
'0 as ��������1���_����������, ' + 
'0 as ��������1���_�������������, ' + 
'0 as ��������1���_���������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������_����������_����������]]] as nvarchar)) as ����������_����������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������_���������_����������_�_���]]] as nvarchar)) as ����������_������������� ' + 


'from openquery(SSAS_DATA_ANALYSIS, '' SELECT NON EMPTY { [Measures].[�������_����������_����������], ' + 
'[Measures].[�������_���������_����������_�_���] } ON COLUMNS ' + 
', NON EMPTY { (' + @dtWhL + ', ' + 
'[Dim_�����������].[�����������_�����_������������].[�����������_�����_������������].ALLMEMBERS * ' + 
'[Dim_�����������].[�����������_�������������_���].[�����������_�������������_���].ALLMEMBERS * ' + 
'[Dim_�����������].[�����������_�������������_������������].[�����������_�������������_������������].ALLMEMBERS * ' + 
'[Dim_������������].[������������_�������].[������������_�������].ALLMEMBERS * ' + 
'[Dim_������������].[������������_������������].[������������_������������].ALLMEMBERS ) } ' + 
'having ( [Measures].[�������_����������_����������]>0 and [Measures].[�������_���������_����������_�_���]>=0) ON ROWS ' + 
'FROM ( SELECT ( { [Dim_�����������].[�����������_�������������_�������_�������������].&[��] } ) ON COLUMNS ' + 
'FROM ( SELECT ( { { [Dim_�����������].[�����������_�����_���].&[101. ������� �������], [Dim_�����������].[�����������_�����_���].&[106. �������] } } ) ON COLUMNS ' + 
'FROM ( SELECT ( { ' + @dtWhL + ' } ) ON COLUMNS ' + 
'FROM [DATA_ANALYSIS]))) ' + 
'WHERE ( [Dim_�����������].[�����������_�����_���].CurrentMember, [Dim_�����������].[�����������_�������������_�������_�������������].&[��] ) '') '  

set @qwr7= 'union ' + 

'select ' + 
'''��������������'' as ���������, ' + 
'cast([[Dim_�����������]].[�����������_�����_������������]].[�����������_�����_������������]].[MEMBER_CAPTION]]] as nvarchar(100)) as �����, ' + 
'cast([[Dim_�����������]].[�����������_�������������_���]].[�����������_�������������_���]].[MEMBER_CAPTION]]] as nvarchar(20)) as ����������������, ' + 
'cast([[Dim_�����������]].[�����������_�������������_������������]].[�����������_�������������_������������]].[MEMBER_CAPTION]]] as nvarchar(100)) as ����������������, ' + 
'CAST([[Dim_������������]].[������������_�������]].[������������_�������]].[MEMBER_CAPTION]]] as nvarchar(20)) as �������, ' + 
'0 as ����������_����������, ' + 
'0 as ����������_�������������, ' + 
'0 as �������3���_����������, ' + 
'0 as �������3���_�������������, ' + 
'0 as �������3���_���������, ' + 
'0 as ��������3���_����������, ' + 
'0 as ��������3���_�������������, ' + 
'0 as ��������3���_���������, ' + 
'0 as �������1���_����������, ' + 
'0 as �������1���_�������������, ' + 
'0 as �������1���_���������, ' + 
'0 as ��������1���_����������, ' + 
'0 as ��������1���_�������������, ' + 
'0 as ��������1���_���������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������_����������_����������]]] as nvarchar)) as ����������_����������, ' + 
'CONVERT(/*numeric(12,2)*/float, cast([[Measures]].[�������_���������_����������_�_���]]] as nvarchar)) as ����������_������������� ' + 

'from openquery(SSAS_DATA_ANALYSIS, '' SELECT NON EMPTY { [Measures].[�������_����������_����������], ' + 
'[Measures].[�������_���������_����������_�_���] } ON COLUMNS ' + 
', NON EMPTY { (' + @dtWhL + ', ' + 
'[Dim_�����������].[�����������_�����_������������].[�����������_�����_������������].ALLMEMBERS * ' + 
'[Dim_�����������].[�����������_�������������_���].[�����������_�������������_���].ALLMEMBERS * ' + 
'[Dim_�����������].[�����������_�������������_������������].[�����������_�������������_������������].ALLMEMBERS * ' + 
'[Dim_������������].[������������_�������].[������������_�������].ALLMEMBERS * ' + 
'[Dim_������������].[������������_������������].[������������_������������].ALLMEMBERS ) } ' + 
'having ( [Measures].[�������_����������_����������]>0 and [Measures].[�������_���������_����������_�_���]>=0) ON ROWS ' + 
'FROM ( SELECT ( { [Dim_�����������].[�����������_�������������_�������_�������������].&[��] } ) ON COLUMNS ' + 
'FROM ( SELECT ( { { [Dim_�����������].[�����������_�����_���].&[102. �������], [Dim_�����������].[�����������_�����_���].&[103. ����], [Dim_�����������].[�����������_�����_���].&[104. ������], [Dim_�����������].[�����������_�����_���].&[108. ���. ����� ��� �������], [Dim_�����������].[�����������_�����_���].&[109. ������������] } } ) ON COLUMNS ' + 
'FROM ( SELECT ( { ' + @dtWhL + ' } ) ON COLUMNS ' + 
'FROM [DATA_ANALYSIS]))) ' + 
'WHERE ( [Dim_�����������].[�����������_�����_���].CurrentMember, [Dim_�����������].[�����������_�������������_�������_�������������].&[��] ) '') ' + 
') as ���_���� ' + 
'group by ' + 
'���������, ' + 
'�����, ' + 
'����������������, ' + 
'����������������, ' + 
'������� '                                 

	
print(@qwr+@qwr1+@qwr2+@qwr3+@qwr4+@qwr5+@qwr6+@qwr7)
exec(@qwr+@qwr1+@qwr2+@qwr3+@qwr4+@qwr5+@qwr6+@qwr7)

select (@qwr+@qwr1+@qwr2+@qwr3+@qwr4+@qwr5+@qwr6+@qwr7)										                                
select (@qwr)
select (@qwr1)
select (@qwr2)
select (@qwr3)
select (@qwr4)
select (@qwr5)
select (@qwr6)
select (@qwr7)
--alter database MINIMAKS_SQL set multi_user

