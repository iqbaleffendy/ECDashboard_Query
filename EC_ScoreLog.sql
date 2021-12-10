delete from EDW_ANALYTICS.CRM.EC_fact_scorelog where format(MTD, 'yyyy-MM') = format(CURRENT_TIMESTAMP, 'yyyy-MM');

	with forecaseDaily as(
select
    oppt.LEAD_ID LeadID,
    oppt.opp_id OpportunityID,
    oppt.opp_item_no OpportunityItemNo,
	oppt.SO_ID SOID,
    oppt.so_item_no SOItemNo,
    c.Model,
	c.Model_Desc,
	c.Series_Rating,
	c.Series_Desc,
	c.Family_Prod_Desc,
	c.Market_Desc

from [LS_BI_PROD].EDW_ANALYTICS.CRM.fact_opportunity oppt
left join [LS_BI_PROD].[EDW_ANALYTICS].[ECC].[dim_material_prod_hie] c on oppt.product_hie_key = c.Product_Hie_Key 

where 1=1
and format(CONVERT(date, CAST(oppt.DELIVERY_DATE_KEY AS varchar)),'yyyy')=format(CURRENT_TIMESTAMP,'yyyy')
           
)
insert into EDW_ANALYTICS.CRM.EC_fact_scorelog
select 
	distinct
	case when (sales_type = 'PP' and Status in ('BACK OUT', 'EX-BACK OUT')) then BillingDate
	when sales_type in ('ST3', 'ST5') then SCORE_DATE
	when SCORE_DATE is null then BillingDate
	else SCORE_DATE end as MTD,
	table1.*


from (

select 
	distinct a.*,
	case 
		when das.area_name like '%Java' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Java'
		when das.area_name like '%Sumatera' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Sumatera'
		when das.area_name = 'Trakindo Utama' then 'TUS'
	else das.area_name end as area_name,
	case when das.area_name like '% MA' Then 'Major Account' else 'Retail Account' end Customer_Type
from (

select
	cast(a.VBELN_VBAP as int) SalesDocument, 
	a.POSNR_VBAP SalesDocumentItem,
	a.AUART SalesDocumentType,
	CONVERT(BIGINT,b.BILLING_DOC) BillingDocument,
	b.BILLING_DATE BillingDate,
	b.MFRPN MaterialNumber,
	case when left(b.MFRPN, 2) = 'E1' then 'ENGINE' when left(b.MFRPN, 2) = 'F1' then 'FORK_LIFT' when left(b.MFRPN, 2) = 'M1' then 'MACHINE' end as MaterialType,
	b.SERNR SerialNumber,
	f.Model_Desc ProductModel,
	f.Series_Desc ProductModelDetail,
	f.Series_Rating Product_Hierarchy,
	f.Family_Prod_Desc,
	f.Market_Desc,
	a.KUNNR_VBAK AccountID,
	a.NAME1_SOl AccountName,
	case
		when (DATEADD(DAY,1,EOMONTH(b.Billing_Date)) = ScoreLog.SCORE_DATE and DATEPART(HOUR, ScoreLog.SCORE_TIME) <= 6) then DATEADD(DAY, -1, ScoreLog.SCORE_DATE)
		else ScoreLog.SCORE_DATE
	end as SCORE_DATE,
	ScoreLog.SCORE_TIME,
	CAST(ScoreLog.SCORE_DATE as DATETIME) + CAST(ScoreLog.SCORE_TIME as DATETIME) AS SCORE_TIME_SAP,
	ScoreLog.SCORE_ERROR,
	case when ScoreLog.SCORE_DATE < DATEADD(DAY,1,EOMONTH(b.Billing_Date)) and format(b.billing_date, 'yyyy-MM') <> format(ScoreLog.SCORE_DATE, 'yyyy-MM') and ScoreLog.CUST_NAME_OR_RENT_CUST_NAME = b.CUST_NAME and ScoreLog.SCORE_ERROR = 'N' then 'BACK OUT'
		when ScoreLog.SCORE_DATE >= DATEADD(DAY,1,EOMONTH(b.Billing_Date)) and DATEPART(HOUR, ScoreLog.SCORE_TIME) > 6 and format(b.billing_date, 'yyyy-MM') <> format(ScoreLog.SCORE_DATE, 'yyyy-MM') and ScoreLog.CUST_NAME_OR_RENT_CUST_NAME = b.CUST_NAME and ScoreLog.SCORE_ERROR = 'N' then 'INVOICED PREVIOUS MONTH'
		when ScoreLog.SCORE_DATE  < DATEADD(DAY,1,EOMONTH(b.Billing_Date)) and format(b.billing_date, 'yyyy-MM') <> format(ScoreLog.SCORE_DATE, 'yyyy-MM') and ScoreLog.CUST_NAME_OR_RENT_CUST_NAME <> b.CUST_NAME and ScoreLog.SCORE_ERROR = 'N' then 'EX-BACK OUT'
		when ScoreLog.SCORE_ERROR <> 'N' and ScoreLog.SCORE_DATE is not null and a.AUART = 'ZEPP' then 'FAILED'
		when ScoreLog.SCORE_DATE is null and a.AUART = 'ZEPP' then 'FAILED'
		when ScoreLog.SCORE_DATE is null and a.AUART <> 'ZEPP' then 'OTHER DEALER'
	else 'NEW UNIT' end Status,
	'PP' sales_type,
	case 
		when a.VKORG = '0Z02' then 
			case 
				when dcm.Sales_code is not null then 
					case 
						when dcm.Flag ='Exception' then '0ZY2'
						when left(b.MFRPN, 2)='E1' then dcm.Sales_code 
					end
				else  d.sales_code
			end
		when a.VKORG = '1Z02' then a.GSBER
	end sales_off_code,
	b.mvgr1 market_sector,
	--case 
	--	when dpgm.ProductGroup = 'GCI' then 'GCI'
	--	when (dpgm.ProductGroup <> 'GCI' or dpgm.ProductGroup is null) and ScoreLog.SCORE_DATE is not null then 'Non-GCI'
	--	when (dpgm.ProductGroup <> 'GCI' or dpgm.ProductGroup is null) and ScoreLog.SCORE_DATE is null then null
	--end as ProductGroup,
	a.GTEXT EndDestination,
	a.GSBER BusinessArea


from [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_V_ODRPP_BW a
left join [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_F_INV_SOLD b on a.VBELN_VBAP = b.VBELN and a.POSNR_VBAP = b.POSNR 
left join
(
select * from (
	select SCORE_DATE,SCORE_TIME,PRIME_PRODUCT_SERIAL_NUMBER, MODEL_NUMBER, CUSTOMER_CODE, CUST_NAME_OR_RENT_CUST_NAME, SCORE_ERROR, 'M1' AS MATERIAL_TYPE, SALE_TYPE, ROW_NUMBER() OVER(PARTITION BY PRIME_PRODUCT_SERIAL_NUMBER ORDER BY SCORE_DATE DESC, SCORE_TIME DESC) as RowNum from [LS_BI_PROD].EDW_STG_SAP_CRM_DAILY.dbo.zldb_score_mach union
	select SCORE_DATE,SCORE_TIME,PRIME_PRODUCT_SERIAL_NUMBER, MODEL_NUMBER, CUSTOMER_CODE, CUST_NAME_RENT_CUST_NAME, SCORE_ERROR, 'E1' AS MATERIAL_TYPE, SALE_TYPE, ROW_NUMBER() OVER(PARTITION BY PRIME_PRODUCT_SERIAL_NUMBER ORDER BY SCORE_DATE DESC, SCORE_TIME DESC) as RowNum  from [LS_BI_PROD].EDW_STG_SAP_CRM_DAILY.dbo.zldb_score_engn
	) as a
	where CUSTOMER_CODE is not null
	and SALE_TYPE in (1,3,5)
	and SCORE_DATE between DATEADD(year, -2, CURRENT_TIMESTAMP) and EOMONTH(CURRENT_TIMESTAMP)
	and RowNum = 1
	--and SCORE_ERROR = 'N'
) as ScoreLog on b.SERNR = ScoreLog.PRIME_PRODUCT_SERIAL_NUMBER
left join forecaseDaily f on cast(a.VBELN_VBAP as int) = f.SOID and a.POSNR_VBAP = f.SOItemNo 
left join EDW_ANALYTICS.CRM.EC_dim_area_sales d on a.SALID  = d.sales_id
left join EDW_ANALYTICS.CRM.EC_dim_company_exception_map dcm on dcm.CompanyName = a.Name2_Payer

where 1=1 
--and (FORMAT(b.billing_date, 'yyyy') = format(CURRENT_TIMESTAMP, 'yyyy') or FORMAT(ScoreLog.SCORE_DATE, 'yyyy') = format(CURRENT_TIMESTAMP, 'yyyy')) 
and (FORMAT(b.billing_date, 'yyyyMM') = format(CURRENT_TIMESTAMP, 'yyyyMM') or FORMAT(ScoreLog.SCORE_DATE, 'yyyyMM') = format(CURRENT_TIMESTAMP, 'yyyyMM')) 
and left(b.MFRPN, 2) in ('M1','E1')

) a left join EDW_ANALYTICS.CRM.EC_dim_area_store das on das.sales_code = a.sales_off_code

UNION

select 
	distinct a.*,
	case 
		when das.area_name like '%Java' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Java'
		when das.area_name like '%Sumatera' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Sumatera'
		when das.area_name = 'Trakindo Utama' then 'TUS'
	else das.area_name end as area_name,
	case when das.area_name like '% MA' Then 'Major Account' else 'Retail Account' end Customer_Type
from (

select 
	st3.VBELN_VA as SalesDocument,
	st3.POSNR_VA as SalesDocumentItem,
	st3.AUART as SalesDocumentType,
	st3.BAST_NO AS BillingDocument,
	st3.FKDAT AS BillingDate,
	NULL AS MaterialNumber,
	st3.MaterialType,
	st3.SERNR SerialNumber,
	f.Model_Desc ProductModel,
	f.Series_Desc ProductModelDetail,
	f.Series_Rating Product_Hierarchy,
	f.Family_Prod_Desc,
	f.Market_Desc,
	CAST(st3.SOLD AS INT) AccountID,
	st3.SOLD_NM AccountName,
	st3.SCORE_DATE,
	st3.SCORE_TIME,
	CAST(st3.SCORE_DATE as DATETIME) + CAST(st3.SCORE_TIME as DATETIME) AS SCORE_TIME_SAP,
	st3.SCORE_ERROR,
	'NEW UNIT' Status,
	CASE WHEN st3.SALE_TYPE = 3 THEN 'ST3' WHEN st3.SALE_TYPE = 5 THEN 'ST5' END AS sales_type,
	st3.sales_code AS sales_off_code,
	NULL as market_sector,
	--case 
	--	when dpgm.ProductGroup = 'GCI' then 'GCI'
	--	when (dpgm.ProductGroup <> 'GCI' or dpgm.ProductGroup is null) and st3.SCORE_DATE is not null then 'Non-GCI'
	--	when (dpgm.ProductGroup <> 'GCI' or dpgm.ProductGroup is null) and st3.SCORE_DATE is null then null
	--end as ProductGroup,
	st3.GTEXT EndDestination,
	st3.GSBER BusinessArea

from (
select 
	a.SALE_TYPE, 
	a.PRIME_PRODUCT_SERIAL_NUMBER,
	a.MODEL_NUMBER,
	'MACHINE' MaterialType,
	c.sales_code,
	c.area_name Area_Name, 
	b.*,
	a.SCORE_DATE,
	a.SCORE_TIME,
	a.SCORE_ERROR
	from [LS_BI_PROD].EDW_STG_SAP_CRM_DAILY.dbo.zldb_score_mach a
left join [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_V_ODRRUE_BW b 
on a.PRIME_PRODUCT_SERIAL_NUMBER = b.sernr
left join EDW_ANALYTICS.CRM.EC_dim_area_store c 
on b.vkbur = c.sales_code
where a.SCORE_ERROR = 'N' and a.SALE_TYPE in (3,5) and b.release_apprv = 'X'
--and format(a.SCORE_DATE,'yyyyMM') in (format(DATEADD(MONTH, -1, CURRENT_TIMESTAMP),'yyyyMM'), format(CURRENT_TIMESTAMP,'yyyyMM'))
union
select 
	a.SALE_TYPE, 
	a.PRIME_PRODUCT_SERIAL_NUMBER,
	a.MODEL_NUMBER,
	'ENGINE' MaterialType,
	c.sales_code,
	c.area_name Area_Name, 
	b.*,
	a.SCORE_DATE,
	a.SCORE_TIME,
	a.SCORE_ERROR
	from [LS_BI_PROD].EDW_STG_SAP_CRM_DAILY.dbo.zldb_score_engn a
left join [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_V_ODRRUE_BW b 
on a.PRIME_PRODUCT_SERIAL_NUMBER = b.sernr
left join EDW_ANALYTICS.CRM.EC_dim_area_store c 
on b.vkbur = c.sales_code
where a.SCORE_ERROR = 'N' and a.SALE_TYPE in (3,5) and b.release_apprv = 'X'
) as st3
LEFT JOIN forecaseDaily AS f ON st3.VBELN_VA = f.SOID AND st3.POSNR_VA = f.SOItemNo
LEFT JOIN EDW_ANALYTICS.CRM.EC_dim_area_store das ON das.sales_code = st3.sales_code

where 1=1 
--and FORMAT(st3.SCORE_DATE, 'yyyy') = FORMAT(CURRENT_TIMESTAMP, 'yyyy')
and FORMAT(st3.SCORE_DATE, 'yyyyMM') = FORMAT(CURRENT_TIMESTAMP, 'yyyyMM')

) a left join EDW_ANALYTICS.CRM.EC_dim_area_store das on das.sales_code = a.sales_off_code

) as table1
;