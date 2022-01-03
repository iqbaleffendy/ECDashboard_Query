truncate table EDW_ANALYTICS_STG.CRM.stg_EC_forecastdaily;

insert into EDW_ANALYTICS_STG.CRM.stg_EC_forecastdaily
select 
	oppt.LEAD_ID LeadID, 
	oppt.OPPORTUNITY_ID OpportunityID, 
	oppt.ITEM_NO OpportunityItemNo,
	oppt.QUOTATION_ID QuotationID,
	oppt.ITEM_NO_QUOTATION QuotationItemNo,
	oppt.SO_ID SOID,
	oppt.ITEM_NO_SO SOItemNo,
    CONVERT(date, CAST(oppt.LEAD_DATE_KEY AS varchar)) as LeadCreatedDate, 
    CONVERT(date, CAST(oppt.LEAD_CHANGED_DATE_KEY AS varchar)) as LeadChangedDate,
	slo.[DESCRIPTION] as SourceofLead,
    CONVERT(date, CAST(oppt.OPP_DATE_KEY AS varchar)) as OppIDCreatedDate,
    CONVERT(date, CAST(oppt.OPP_CHANGED_DATE_KEY AS varchar)) as OppIDChangedDate,
    CONVERT(date, CAST(oppt.OPP_ITEM_DATE_KEY AS varchar)) as OppItemCreatedDate,
    CONVERT(date, CAST(oppt.OPP_ITEM_CHANGED_DATE_KEY AS varchar)) as OppItemChangedDate,
	CONVERT(date, CAST(oppt.QUOT_CREATED_DATE_KEY AS varchar)) as QuotationIDCreatedDate, 
    CONVERT(date, CAST(oppt.QUOT_CHANGED_DATE_KEY AS varchar)) as QuotationIDChangedDate,
    CONVERT(date, CAST(oppt.QUOT_ITEM_CREATED_DATE_KEY AS varchar)) as QuotationItemCreatedDate, 
    CONVERT(date, CAST(oppt.QUOT_ITEM_CHANGED_DATE_KEY AS varchar)) as QuotationItemChangedDate,
	CONVERT(date, CAST(oppt.SO_CREATED_DATE_KEY AS varchar)) as SOIDCreatedDate,
    CONVERT(date, CAST(oppt.BILLING_CREATED_DATE AS varchar)) as BillingIDCreatedDate,
	oppt.TRANSACTION_DESC TransactionDescription,
	oppt.customer_key,
	dc.CUSTOMER_CODE as AccountID,
	dc.FULL_NAME as AccountName,
	cic.CIC_Group CICGroup,
	cic.CIC_Description CICDescription,
	dsl.AREA_NAME AS SalesOfficeArea,
	dsl.STORE_ABBREVATION_NAME + ' - ' + dsl.STORE_NAME as SALES_OFFICE,
	dsl.store_abbrevation_name sales_off_code,
	dsl.STORE_ABBREVATION_NAME AS SalesOffice,
	dsl.COMPANY_NAME,
	dsl.SUB_COMPANY_NAME,
	dsl.REGION_NAME,
	case when dse.occupation_code = 'SE' then 
	dse.FULL_NAME else 
	dsp.FULL_NAME end as SalesRepsName,
		case when dse.occupation_code = 'SE' then oppt.SALES_EXECUTIVE_KEY else oppt.SALES_PERSON_KEY end as SALES_KEY, 
	case when dse.occupation_code = 'SE' then 
	dse.OCCUPATION_CODE else 
	dsp.OCCUPATION_CODE end as OccupationCode,
	opst.[DESCRIPTION] as OpportunityStatus,
    CONVERT(date, CAST(oppt.OPPORTUNITY_STATUS_DATE_KEY AS varchar)) as OpportunityStatusDate,
    dqs.[DESCRIPTION] as QuotationStatus,
	dss.[DESCRIPTION] AS SOItemStatus,
	drj.Rejection_Reason as SORejectionReason,
    optp.OPPORTUNITY_TYPE_DESCRIPTION as OpportunityType, 
	dpm.PRODUCT_MATERIAL_CODE ProductID,
	dpm.PRODUCT_MODEL ProductModel,
	dpm.[DESCRIPTION] as ProductDescription,
	dpm.VALID_MATERIAL ValidMaterial,
	sn.Serial_No SerialNo,
	sn.Batch_ID BatchID,
	oppt.MARKET_SECTOR_KEY as market_sector_key,
	dms.MARKET_SECTOR_CODE PWC,
	dms.[DESCRIPTION] as MarketDescription,
	dms.INDUSTRY_GROUP,
	dms.PRODUCT_DIVISION,
	dms.INDUSTRY_SEGMENT,
	case when dms.[INDUSTRY_GROUP] is not null then dms.[INDUSTRY_GROUP] else dms.[INDUSTRY_SEGMENT] end as MarketSector,
	dpd.[DESCRIPTION] as POINT_OF_DELIVERY_DESC,
    CONVERT(date, CAST(oppt.DELIVERY_DATE_KEY AS varchar)) as deliverydate,
	oppt.net_value NetValueInUSD,
    oppt.net_value NetValueInIDR,
	oppt.EXPECTED_TOTAL_VALUE ExpectedTotalVaue,
	curr.CURRENCY_CODE Currency,
	oppt.QUOT_NET_VALUE QuotNetValue,
	currq.CURRENCY_CODE as QOUT_CURRENCY_CODE,
	flag.[DESCRIPTION] as FORECAST,
	da.Major_Account_Classification MajorAccountClassification,
	da.Customer_Class_Code CustomerClassCode,
	da.INDUSTRY_KEY,
	da.Vertical_Industry VerticalIndustry,
	oppt.product_material_key,
    dpm.material_category MaterialType,
	dpm.Category_ID,
	oppt.CONFIDENCE_LEVEL ConfidenceLevel, 
	dpm.BASIC_SELLING_PRICE_VALID_FROM BasicSellingPriceValidFrom,
	dpm.BASIC_SELLING_PRICE_VALID_TO BasicSellingPriceValidTo,
	dpmm.product_hierarchy,
	dps.sales_type,
	CONVERT(date, CAST(oppt.REPORT_DATE_KEY AS varchar)) as LOAD_DATE,
	GETDATE() AS ETL_DATE
from
[LS_BI_PROD].EDW_CRM_ANALYTICS.dbo.FACT_CRM_OPPORTUNITY oppt left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_OPPORTUNITY_TYPE optp on (oppt.OPPORTUNITY_TYPE_KEY = optp.OPPORTUNITY_TYPE_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_USER_STATUS opst on (oppt.USER_STATUS_KEY = opst.USER_STATUS_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.dbo.DIM_FLAG_YN flag on (oppt.FLAG_FORECAST_KEY = flag.FLAG_YN_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_SALES_PERSON dsp on (oppt.SALES_PERSON_KEY = dsp.SALES_PERSON_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_SALES_PERSON dse on (oppt.SALES_EXECUTIVE_KEY = dse.SALES_PERSON_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_SALES_LOCATION dsl on (oppt.SALES_LOCATION_KEY = dsl.SALES_LOCATION_KEY) left join
(select case when isnumeric(product_material_code)=1 then cast(CAST(product_material_code as int) as nvarchar) else product_material_code end material_code
, * from [LS_BI_PROD].EDW_DIMENSION.CRM.DIM_PRODUCT_MATERIAL) dpm on (oppt.PRODUCT_MATERIAL_KEY = dpm.PRODUCT_MATERIAL_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_MARKET_SECTOR dms on (oppt.MARKET_SECTOR_KEY = dms.MARKET_SECTOR_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_SO_STATUS dss on (oppt.SO_STATUS_KEY = dss.SO_STATUS_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.Dim_Rejection drj on (oppt.SO_REJECTION_KEY = drj.Rejection_key) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_POINT_OF_DELIVERY dpd on (oppt.POINT_OF_DELIVERY_KEY = dpd.POINT_OF_DELIVERY_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_QUOTATION_STATUS dqs on (oppt.QUOTATION_STATUS_KEY = dqs.QUOTATION_STATUS_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_CUSTOMER dc on (oppt.CUSTOMER_KEY = dc.CUSTOMER_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.Dim_CIC cic on (oppt.CIC_KEY = cic.CIC_KEY) left join 
[LS_BI_PROD].EDW_DIMENSION.ECC.Dim_Serial_Equi sn on (oppt.SerialEqui_Key = sn.SerialEqui_Key) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_CURRENCY curr on (oppt.CURRENCY_KEY = curr.CURRENCY_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_CURRENCY currq on (oppt.QUOT_CURRENCY_KEY = currq.CURRENCY_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_ORIGIN_LEAD_OPP slo on (oppt.LEAD_ORIGIN_KEY = slo.ORIGIN_OPP_LEAD_KEY) left join
[LS_BI_PROD].EDW_ANALYTICS.ECC.dim_account da on (case when dc.CUSTOMER_CODE < 0 then dc.CUSTOMER_CODE else RIGHT('00000' + CAST(dc.CUSTOMER_CODE as varchar(10)), 10) end = da.account_id) left join
[LS_BI_PROD].EDW_ANALYTICS.crm.dim_price_scenario dps on oppt.PRICE_SCENARIO = dps.price_scenario_key
left join (select case when isnumeric(product_material_code)=1 then cast(CAST(product_material_code as int) as nvarchar)  else product_material_code end material_code
, * from [LS_BI_PROD].EDW_ANALYTICS.CRM.dim_opp_product_material) dpmm on dpm.material_code = dpmm.material_code
where
            optp.OPPORTUNITY_TYPE_DESCRIPTION not in ('N/A', 'Unknown', 'LEAD')
            and ( opst.[DESCRIPTION] like '%stage 3%' or
            opst.[DESCRIPTION] like '%stage 4%' or
            opst.[DESCRIPTION] like '%stage 5%' or
            opst.[DESCRIPTION] like '%stage 6%' or
            opst.[DESCRIPTION] like '%won%' or
            opst.[DESCRIPTION] like '%delivered%' or
            opst.[DESCRIPTION] like '%lost%')
			and left(oppt.LEAD_DATE_KEY, 4) = YEAR(GETDATE());