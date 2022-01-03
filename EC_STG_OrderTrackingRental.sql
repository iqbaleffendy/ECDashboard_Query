truncate table EDW_ANALYTICS_STG.CRM.stg_EC_ordertrackingrental;

with st3 as (
select 
	a.SALE_TYPE, 
	a.PRIME_PRODUCT_SERIAL_NUMBER,
	'MACHINE' MaterialType,
	c.sales_code,
	c.area_name Area_Name, 
	b.*,
	dpm.PRODUCT_HIERARCHY,
	a.SCORE_DATE 
	from [LS_BI_PROD].EDW_STG_SAP_CRM_DAILY.dbo.zldb_score_mach a
left join [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_V_ODRRUE_BW b 
on a.PRIME_PRODUCT_SERIAL_NUMBER = b.sernr
left join EDW_ANALYTICS.CRM.EC_dim_area_store c 
on b.vkbur = c.sales_code
left join (select * from [LS_BI_PROD].EDW_ANALYTICS.CRM.dim_opp_product_material where product_material_code not in ('N/A', 'Unknown') and left(product_hierarchy, 2) in ('M1','E1','F1')) dpm
on CAST(b.MATNR AS INT) = CAST (dpm.PRODUCT_MATERIAL_CODE AS INT)
where a.SCORE_ERROR = 'N' and a.SALE_TYPE in (3,5) and b.release_apprv = 'X'
--and format(a.SCORE_DATE,'yyyyMM') in (format(DATEADD(MONTH, -1, CURRENT_TIMESTAMP),'yyyyMM'), format(CURRENT_TIMESTAMP,'yyyyMM'))
union
select 
	a.SALE_TYPE, 
	a.PRIME_PRODUCT_SERIAL_NUMBER,
	'ENGINE' MaterialType,
	c.sales_code,
	c.area_name Area_Name, 
	b.*,
	dpm.PRODUCT_HIERARCHY,
	a.SCORE_DATE 
	from [LS_BI_PROD].EDW_STG_SAP_CRM_DAILY.dbo.zldb_score_engn a
left join [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_V_ODRRUE_BW b 
on a.PRIME_PRODUCT_SERIAL_NUMBER = b.sernr
left join EDW_ANALYTICS.CRM.EC_dim_area_store c 
on b.vkbur = c.sales_code
left join (select * from [LS_BI_PROD].EDW_ANALYTICS.CRM.dim_opp_product_material where product_material_code not in ('N/A', 'Unknown') and left(product_hierarchy, 2) in ('M1','E1','F1')) dpm
on CAST(b.MATNR AS INT) = CAST (dpm.PRODUCT_MATERIAL_CODE AS INT)
where a.SCORE_ERROR = 'N' and a.SALE_TYPE in (3,5) and b.release_apprv = 'X'
--and format(a.SCORE_DATE,'yyyyMM') in (format(DATEADD(MONTH, -1, CURRENT_TIMESTAMP),'yyyyMM'), format(CURRENT_TIMESTAMP,'yyyyMM'))
)

insert into EDW_ANALYTICS_STG.CRM.stg_EC_ordertrackingrental
select * from st3;