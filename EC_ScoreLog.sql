--delete from EDW_ANALYTICS.dbo.invoiceDataLogScore where format(MTD, 'yyyyMM') = format(CURRENT_TIMESTAMP, 'yyyyMM');
--delete from EDW_ANALYTICS.dbo.invoiceDataLogScore where format(MTD, 'yyyyMM') = '202111';
--drop table EDW_ANALYTICS.dbo.invoiceDataLogScore;

with forecaseDaily as(
select
    oppt.LEAD_ID LeadID,
    oppt.OPPORTUNITY_ID OpportunityID,
    oppt.ITEM_NO OpportunityItemNo,
	oppt.SO_ID SOID,
    oppt.ITEM_NO_SO SOItemNo,
    cic.CIC_Group CICGroup,
    da.INDUSTRY_KEY
from
[LS_BI_PROD].EDW_CRM_ANALYTICS.dbo.FACT_CRM_OPPORTUNITY oppt
left join [LS_BI_PROD].EDW_DIMENSION.CRM.DIM_CUSTOMER dc on (oppt.CUSTOMER_KEY = dc.CUSTOMER_KEY) 
left join [LS_BI_PROD].EDW_DIMENSION.CRM.Dim_CIC cic on (oppt.CIC_KEY = cic.CIC_KEY)
left join [LS_BI_PROD].EDW_ANALYTICS.ECC.dim_account da on (case when dc.CUSTOMER_CODE < 0 then dc.CUSTOMER_CODE else RIGHT('00000' + CAST(dc.CUSTOMER_CODE as varchar(10)), 10) end = da.account_id)
where 1=1
and format(CONVERT(date, CAST(oppt.DELIVERY_DATE_KEY AS varchar)),'yyyy')=format(CURRENT_TIMESTAMP,'yyyy')
            
)

--insert into EDW_ANALYTICS.dbo.invoiceDataLogScore
select 
	case when (sales_type = 'PP' and Status in ('BACK OUT', 'EX-BACK OUT')) then BillingDate
	when sales_type in ('ST3', 'ST5') then SCORE_DATE
	when SCORE_DATE is null then BillingDate
	else SCORE_DATE end as MTD,
	table1.*

--into EDW_ANALYTICS.dbo.invoiceDataLogScore

from (

select 
	distinct a.*,
	case 
		when das.area_name like '%Java' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Java'
		when das.area_name like '%Sumatera' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Sumatera'
		when das.area_name = 'Trakindo Utama' then 'TUS'
	else das.area_name end as area_name,
	case when das.area_name like '%MA' Then 'Major Account' else 'Retail Account' end Customer_Type,
	case 
		when das.area_name like '%MA' Then CICGroups 
	else 
		case 
			when (market_sector is null or market_sector='') then CICGroups 
		else market_sector end 
	end MarketSector
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
	dpm.model_general ProductModel,
	dpm.model_detail ProductModelDetail,
	dpm.product_hierarchy_detail Product_Hierarchy,
	a.KUNNR_VBAK AccountID,
	a.NAME1_SOl AccountName,
	case
		when (DATEADD(DAY,1,EOMONTH(b.Billing_Date)) = ScoreLog.SCORE_DATE and DATEPART(HOUR, ScoreLog.SCORE_TIME) <= 6) then DATEADD(DAY, -1, ScoreLog.SCORE_DATE)
		else ScoreLog.SCORE_DATE
	end as SCORE_DATE,
	ScoreLog.SCORE_TIME,
	CAST(ScoreLog.SCORE_DATE as DATETIME) + CAST(ScoreLog.SCORE_TIME as DATETIME) AS SCORE_TIME_SAP,
	ScoreLog.SCORE_ERROR,
	case when ScoreLog.SCORE_DATE < DATEFROMPARTS(YEAR(b.BILLING_DATE), MONTH(b.BILLING_DATE)+1, 1) and format(b.billing_date, 'yyyy-MM') <> format(ScoreLog.SCORE_DATE, 'yyyy-MM') and ScoreLog.CUST_NAME_OR_RENT_CUST_NAME = b.CUST_NAME and ScoreLog.SCORE_ERROR = 'N' then 'BACK OUT'
		when ScoreLog.SCORE_DATE >= DATEFROMPARTS(YEAR(b.BILLING_DATE), MONTH(b.BILLING_DATE)+1, 1) and DATEPART(HOUR, ScoreLog.SCORE_TIME) > 6 and format(b.billing_date, 'yyyy-MM') <> format(ScoreLog.SCORE_DATE, 'yyyy-MM') and ScoreLog.CUST_NAME_OR_RENT_CUST_NAME = b.CUST_NAME and ScoreLog.SCORE_ERROR = 'N' then 'INVOICED PREVIOUS MONTH'
		when ScoreLog.SCORE_DATE  < DATEFROMPARTS(YEAR(b.BILLING_DATE), MONTH(b.BILLING_DATE)+1, 1) and format(b.billing_date, 'yyyy-MM') <> format(ScoreLog.SCORE_DATE, 'yyyy-MM') and ScoreLog.CUST_NAME_OR_RENT_CUST_NAME <> b.CUST_NAME and ScoreLog.SCORE_ERROR = 'N' then 'EX-BACK OUT'
		when ScoreLog.SCORE_ERROR <> 'N' and ScoreLog.SCORE_DATE is not null and a.AUART = 'ZEPP' then 'FAILED'
		when ScoreLog.SCORE_DATE is null and a.AUART = 'ZEPP' then 'FAILED'
		when ScoreLog.SCORE_DATE is null and a.AUART <> 'ZEPP' then 'OTHER DEALER'
	else 'NEW UNIT' end Status,
	'PP' sales_type,
	f.INDUSTRY_KEY,
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
	cic.cic_group CICGroups,
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
left join (
	select * from (
		select 
			ROW_NUMBER() OVER(PARTITION BY aa.product_material_code ORDER BY aa.product_material_code ASC) AS Row#, 
			aa.product_material_code, 
			case when bb.Model_Desc is null then mm.product_model else bb.Model_Desc end model_general,
			mm.product_model model_detail, 
			case when bb.Model is null then product_hierarchy else bb.Model end  product_hierarchy_general, 
			case 
				when bb.Series_Rating is null and bb.Model is not null then model 
				when bb.Series_Rating is null and bb.Model is null then product_hierarchy else bb.Series_Rating end product_hierarchy_detail,
			aa.description 
		from 
		[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_PRODUCT_MATERIAL mm 
		left join [LS_BI_PROD].EDW_ANALYTICS.CRM.dim_opp_product_material aa 
		on (case when isnumeric(mm.product_material_code)=1 then cast(CAST(mm.product_material_code as int) as nvarchar)  else mm.product_material_code end = case when isnumeric(aa.product_material_code)=1 then cast(CAST(aa.product_material_code as int) as nvarchar)  else aa.product_material_code end)
		left join [LS_BI_PROD].[EDW_ANALYTICS].[ECC].[dim_material_prod_hie] bb on aa.product_hierarchy = bb.Model or aa.product_hierarchy = bb.Series_Rating
		where LEFT(product_hierarchy,2) in ('M1', 'E1', 'F1') and mm.product_model is not null
	) cc where Row# =1
) dpm on CAST(a.MATNR_VBAP AS INT) = CAST (dpm.PRODUCT_MATERIAL_CODE AS INT)
left join (select DISTINCT cic_group_id,cic_group from ls_bi_prod.EDW_DIMENSION.CRM.Dim_CIC) cic on cic.cic_group_id = b.brsch
left join forecaseDaily f on cast(a.VBELN_VBAP as int) = f.SOID and a.POSNR_VBAP = f.SOItemNo 
left join EDW_ANALYTICS.dbo.dim_area_sales d on a.SALID  = d.sales_id
left join EDW_ANALYTICS.dbo.dim_company_map dcm on dcm.CompanyName = a.Name2_Payer
--left join EDW_ANALYTICS.dbo.dim_productgroupmapping dpgm on dpgm.Model = dpm.product_model

where 1=1 
and (FORMAT(b.billing_date, 'yyyy-MM') = '2021-11' or FORMAT(ScoreLog.SCORE_DATE, 'yyyy-MM') = '2021-11') 
--and (FORMAT(b.billing_date, 'yyyy-MM') = format(CURRENT_TIMESTAMP, 'yyyy-MM') or FORMAT(ScoreLog.SCORE_DATE, 'yyyy-MM') = format(CURRENT_TIMESTAMP, 'yyyy-MM')) 
and left(b.MFRPN, 2) in ('M1','E1')

) a left join EDW_ANALYTICS.dbo.dim_area_store das on das.sales_code = a.sales_off_code

UNION

select 
	distinct a.*,
	case 
		when das.area_name like '%Java' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Java'
		when das.area_name like '%Sumatera' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Sumatera'
		when das.area_name = 'Trakindo Utama' then 'TUS'
	else das.area_name end as area_name,
	case when das.area_name like '%MA' Then 'Major Account' else 'Retail Account' end Customer_Type,
	case 
		when das.area_name like '%MA' Then CICGroups 
	else 
		case 
			when (market_sector is null or market_sector='') then CICGroups 
		else market_sector end 
	end MarketSector
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
	st3.ProductModel,
	st3.ProductModelDetail,
	st3.PRODUCT_HIERARCHY ProductHierarchy,
	CAST(st3.SOLD AS INT) AccountID,
	st3.SOLD_NM AccountName,
	st3.SCORE_DATE,
	st3.SCORE_TIME,
	CAST(st3.SCORE_DATE as DATETIME) + CAST(st3.SCORE_TIME as DATETIME) AS SCORE_TIME_SAP,
	st3.SCORE_ERROR,
	'NEW UNIT' Status,
	CASE WHEN st3.SALE_TYPE = 3 THEN 'ST3' WHEN st3.SALE_TYPE = 5 THEN 'ST5' END AS sales_type,
	f.INDUSTRY_KEY,
	st3.sales_code AS sales_off_code,
	f.CICGroup as CICGroups,
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
	dpm.product_hierarchy_detail Product_Hierarchy,
	dpm.model_general ProductModel,
	dpm.model_detail ProductModelDetail,
	a.SCORE_DATE,
	a.SCORE_TIME,
	a.SCORE_ERROR
	from [LS_BI_PROD].EDW_STG_SAP_CRM_DAILY.dbo.zldb_score_mach a
left join [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_V_ODRRUE_BW b 
on a.PRIME_PRODUCT_SERIAL_NUMBER = b.sernr
left join EDW_ANALYTICS.dbo.dim_area_store c 
on b.vkbur = c.sales_code
left join (
	select * from (
		select 
			ROW_NUMBER() OVER(PARTITION BY aa.product_material_code ORDER BY aa.product_material_code ASC) AS Row#, 
			aa.product_material_code, 
			case when bb.Model_Desc is null then mm.product_model else bb.Model_Desc end model_general,
			mm.product_model model_detail, 
			case when bb.Model is null then product_hierarchy else bb.Model end  product_hierarchy_general, 
			case 
				when bb.Series_Rating is null and bb.Model is not null then model 
				when bb.Series_Rating is null and bb.Model is null then product_hierarchy else bb.Series_Rating end product_hierarchy_detail,
			aa.description 
		from 
		[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_PRODUCT_MATERIAL mm 
		left join [LS_BI_PROD].EDW_ANALYTICS.CRM.dim_opp_product_material aa 
		on (case when isnumeric(mm.product_material_code)=1 then cast(CAST(mm.product_material_code as int) as nvarchar)  else mm.product_material_code end = case when isnumeric(aa.product_material_code)=1 then cast(CAST(aa.product_material_code as int) as nvarchar)  else aa.product_material_code end)
		left join [LS_BI_PROD].[EDW_ANALYTICS].[ECC].[dim_material_prod_hie] bb on aa.product_hierarchy = bb.Model or aa.product_hierarchy = bb.Series_Rating
		where LEFT(product_hierarchy,2) in ('M1', 'E1', 'F1') and mm.product_model is not null
	) cc where Row# =1
) dpm on CAST(b.MATNR AS INT) = CAST (dpm.PRODUCT_MATERIAL_CODE AS INT)
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
	dpm.product_hierarchy_detail Product_Hierarchy,
	dpm.model_general ProductModel,
	dpm.model_detail ProductModelDetail,
	a.SCORE_DATE,
	a.SCORE_TIME,
	a.SCORE_ERROR
	from [LS_BI_PROD].EDW_STG_SAP_CRM_DAILY.dbo.zldb_score_engn a
left join [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_V_ODRRUE_BW b 
on a.PRIME_PRODUCT_SERIAL_NUMBER = b.sernr
left join EDW_ANALYTICS.dbo.dim_area_store c 
on b.vkbur = c.sales_code
left join (
	select * from (
		select 
			ROW_NUMBER() OVER(PARTITION BY aa.product_material_code ORDER BY aa.product_material_code ASC) AS Row#, 
			aa.product_material_code, 
			case when bb.Model_Desc is null then mm.product_model else bb.Model_Desc end model_general,
			mm.product_model model_detail, 
			case when bb.Model is null then product_hierarchy else bb.Model end  product_hierarchy_general, 
			case 
				when bb.Series_Rating is null and bb.Model is not null then model 
				when bb.Series_Rating is null and bb.Model is null then product_hierarchy else bb.Series_Rating end product_hierarchy_detail,
			aa.description 
		from 
		[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_PRODUCT_MATERIAL mm 
		left join [LS_BI_PROD].EDW_ANALYTICS.CRM.dim_opp_product_material aa 
		on (case when isnumeric(mm.product_material_code)=1 then cast(CAST(mm.product_material_code as int) as nvarchar)  else mm.product_material_code end = case when isnumeric(aa.product_material_code)=1 then cast(CAST(aa.product_material_code as int) as nvarchar)  else aa.product_material_code end)
		left join [LS_BI_PROD].[EDW_ANALYTICS].[ECC].[dim_material_prod_hie] bb on aa.product_hierarchy = bb.Model or aa.product_hierarchy = bb.Series_Rating
		where LEFT(product_hierarchy,2) in ('M1', 'E1', 'F1') and mm.product_model is not null
	) cc where Row# =1
) dpm on CAST(b.MATNR AS INT) = CAST (dpm.PRODUCT_MATERIAL_CODE AS INT)
where a.SCORE_ERROR = 'N' and a.SALE_TYPE in (3,5) and b.release_apprv = 'X'
) as st3
LEFT JOIN forecaseDaily AS f ON st3.VBELN_VA = f.SOID AND st3.POSNR_VA = f.SOItemNo
--LEFT JOIN EDW_ANALYTICS.dbo.dim_productgroupmapping dpgm on dpgm.Model = st3.product_model
LEFT JOIN EDW_ANALYTICS.dbo.dim_area_store das ON das.sales_code = st3.sales_code

where 1=1 
and FORMAT(st3.SCORE_DATE, 'yyyy-MM') = '2021-11'
--and FORMAT(st3.SCORE_DATE, 'yyyy-MM') = FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM')

) a left join EDW_ANALYTICS.dbo.dim_area_store das on das.sales_code = a.sales_off_code

) as table1
;

