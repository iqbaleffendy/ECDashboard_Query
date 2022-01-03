truncate table EDW_ANALYTICS_STG.CRM.stg_EC_scorelog;

insert into EDW_ANALYTICS_STG.CRM.stg_EC_scorelog
select 
	b.sernr,
	b.CUST_NAME,
	a.vbrk_fkdat, 
	b.billing_date, 
	ScoreLog.SCORE_DATE,
	case when ScoreLog.SCORE_DATE < DATEADD(DAY,1,EOMONTH(b.Billing_Date)) and format(b.billing_date, 'yyyy-MM') <> format(ScoreLog.SCORE_DATE, 'yyyy-MM') and ScoreLog.CUST_NAME_OR_RENT_CUST_NAME = b.CUST_NAME then 0
		when ScoreLog.SCORE_DATE  < DATEADD(DAY,1,EOMONTH(b.Billing_Date)) and format(b.billing_date, 'yyyy-MM') <> format(ScoreLog.SCORE_DATE, 'yyyy-MM') and ScoreLog.CUST_NAME_OR_RENT_CUST_NAME <> b.CUST_NAME then 1
	else 1 end hitung,
	GETDATE() AS ETL_DATE

from [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_V_ODRPP_BW a
left join [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_F_INV_SOLD b on a.VBELN_VBAP = b.VBELN and a.POSNR_VBAP =b.POSNR 
left join(
	select sl.*, sn.material_no, pm.product_model
	from (
		select SCORE_DATE,PRIME_PRODUCT_SERIAL_NUMBER, CUSTOMER_CODE, CUST_NAME_OR_RENT_CUST_NAME, SCORE_ERROR, 'M1' AS MATERIAL_TYPE, SALE_TYPE from [LS_BI_PROD].EDW_STG_SAP_CRM_DAILY.dbo.zldb_score_mach union
		select SCORE_DATE,PRIME_PRODUCT_SERIAL_NUMBER, CUSTOMER_CODE, CUST_NAME_RENT_CUST_NAME, SCORE_ERROR, 'E1' AS MATERIAL_TYPE, SALE_TYPE from [LS_BI_PROD].EDW_STG_SAP_CRM_DAILY.dbo.zldb_score_engn
	) sl
	left join (
		select SERIAL_NO,  cast(material_no as int) material_no 
		from [LS_BI_PROD].EDW_DIMENSION.ECC.Dim_Serial_Equi 
		where material_no not in ('Not Available', 'Unknown') and left(material_no,9) <> '000000005'
	) sn on sl.PRIME_PRODUCT_SERIAL_NUMBER = sn.SERIAL_NO
	left join (
		select product_model,  cast(product_material_code as int) product_material_code 
		from [LS_BI_PROD].EDW_ANALYTICS.CRM.dim_opp_product_material
		where product_hierarchy is not null
	) pm on sn.material_no = pm.product_material_code
) as ScoreLog
on b.SERNR = ScoreLog.PRIME_PRODUCT_SERIAL_NUMBER
where 1=1 
and FORMAT (b.billing_date, 'yyyy') >= format(CURRENT_TIMESTAMP, 'yyyy')  
and ScoreLog.SALE_TYPE  in (1,3,5) 
--and ScoreLog.SCORE_ERROR = 'N'
;