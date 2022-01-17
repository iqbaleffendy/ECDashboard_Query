truncate table EDW_ANALYTICS_STG.CRM.stg_EC_forecastdaily;

insert into EDW_ANALYTICS_STG.CRM.stg_EC_forecastdaily
select 
	oppt.LEAD_ID LeadID, 
	oppt.opp_id OpportunityID, 
	oppt.opp_item_no OpportunityItemNo,
	oppt.quot_id QuotationID,
	oppt.quot_item_no QuotationItemNo,
	oppt.SO_ID SOID,
	oppt.so_item_no SOItemNo,
    oppt.lead_created_date as LeadCreatedDate, 
    oppt.lead_changed_date as LeadChangedDate,
	slo.[DESCRIPTION] as SourceofLead,
    oppt.opp_created_date as OppIDCreatedDate,
    oppt.opp_changed_date as OppIDChangedDate,
    oppt.opp_item_created_date as OppItemCreatedDate,
    oppt.opp_item_changed_date as OppItemChangedDate,
	oppt.quot_created_date as QuotationIDCreatedDate, 
    oppt.quot_changed_date as QuotationIDChangedDate,
    oppt.quot_item_created_date as QuotationItemCreatedDate, 
    oppt.quot_changed_date as QuotationItemChangedDate,
	oppt.so_created_date as SOIDCreatedDate,
    oppt.BILLING_CREATED_DATE as BillingIDCreatedDate,
	oppt.TRANSACTION_DESC TransactionDescription,
	oppt.customer_key,
	dc.Account_ID as AccountID,
	dc.Account_Name as AccountName,
	cic.CIC_Group CICGroup,
	cic.CIC_Description CICDescription,
	dsl.AREA_NAME AS SalesOfficeArea,
	dsl.store_abbreviation_name + ' - ' + dsl.Sales_Office_Sales_Contributor as SALES_OFFICE,
	dsl.store_abbreviation_name sales_off_code,
	dsl.store_abbreviation_name AS SalesOffice,
	dsl.COMPANY_NAME,
	dsl.SUB_COMPANY_NAME,
	dsl.REGION_NAME,
	case when dsl.SUB_COMPANY_NAME = 'TUS' then dsr.Sales_Person_Name when oppt.sales_person_key not in (-1, -2)
		then dsp.Sales_Person_Name else dse.Sales_Person_Name end as SalesRepsName,
	case when dsl.SUB_COMPANY_NAME = 'TUS' then oppt.responsible_key when oppt.sales_person_key not in (-1, -2) then oppt.SALES_PERSON_KEY else oppt.sales_exec_key end as SALES_KEY, 
	case when dsl.SUB_COMPANY_NAME = 'TUS' then dsr.OCCUPATION_CODE
		when oppt.sales_person_key not in (-1, -2) then dsp.OCCUPATION_CODE
		else dse.OCCUPATION_CODE end as OccupationCode,
	case when oist.funel_status like '%lost%' or oist.funel_status like '%reviewed by superior%' or
		opst.[description] like '%lost%' then 'LOST'
		when oist.funel_status is null then UPPER(opst.[description]) else UPPER(LTRIM(SUBSTRING(oist.funel_status, 4, LEN(oist.funel_status)))) end as OpportunityStatus,
    CONVERT(date, CAST(oppt.opp_status_date AS varchar)) as OpportunityStatusDate,
    dqs.Quotation_Status as QuotationStatus,
	dss.Sales_Order_Status AS SOItemStatus,
	drj.Rejection_Reason as SORejectionReason,
    optp.OPPORTUNITY_TYPE_DESCRIPTION as OpportunityType, 
	dpm.PRODUCT_MATERIAL_CODE ProductID,
	dpm.PRODUCT_MODEL ProductModel,
	dpm.[DESCRIPTION] as ProductDescription,
	dpm.VALID_MATERIAL ValidMaterial,
	sn.Serial_No SerialNo,
	sn.Batch_ID BatchID,
	oppt.MARKET_SECTOR_KEY as market_sector_key,
	dms.PWC_Code PWC,
	dms.PWC as MarketDescription,
	dms.INDUSTRY_GROUP,
	dms.PRODUCT_DIVISION,
	dms.INDUSTRY_SEGMENT,
	case when dms.[INDUSTRY_GROUP] is not null then dms.[INDUSTRY_GROUP] else dms.[INDUSTRY_SEGMENT] end as MarketSector,
	dba.Business_Area_Name as POINT_OF_DELIVERY_DESC,
    oppt.delivery_date as deliverydate,
	oppt.opp_net_value NetValueInUSD,
    oppt.opp_net_value NetValueInIDR,
	oppt.opp_expected_value ExpectedTotalVaue,
	curr.CURRENCY_CODE Currency,
	oppt.QUOT_NET_VALUE QuotNetValue,
	currq.CURRENCY_CODE as QOUT_CURRENCY_CODE,
	oppt.forecast_flag as FORECAST,
	da.Major_Account_Classification MajorAccountClassification,
	da.Customer_Class_Code CustomerClassCode,
	da.INDUSTRY_KEY,
	da.Vertical_Industry VerticalIndustry,
	oppt.product_material_key,
    dpm.material_category MaterialType,
	dpm.Category_ID,
	oppt.CONFIDENCE_LEVEL ConfidenceLevel, 
	dpm.valid_from BasicSellingPriceValidFrom,
	dpm.valid_to BasicSellingPriceValidTo,
	dpm.product_hierarchy,
	dps.sales_type,
	oppt.report_date as LOAD_DATE,
	GETDATE() AS ETL_DATE
from
EDW_ANALYTICS.CRM.fact_opportunity oppt left join
EDW_ANALYTICS.CRM.dim_opportunity_type optp on (oppt.opp_type_key = optp.opportunity_type_key) left join
EDW_ANALYTICS.CRM.dim_user_status opst on (
oppt.opp_status_key = opst.user_status_key and opst.status_profile = 'ZOPPPS'
) left join
EDW_ANALYTICS.CRM.dim_opportunity_item_status oist on (
oppt.opp_item_status_key = oist.Opportunity_Item_Status_Key
) left join
EDW_ANALYTICS.CRM.dim_origin_lead_opp slo on (oppt.LEAD_ORIGIN_KEY = slo.origin_lead_opp_key) left join
EDW_ANALYTICS.CRM.dim_account dc on (oppt.CUSTOMER_KEY = dc.Account_Key) left join
EDW_ANALYTICS.CRM.dim_crm_cic cic on (oppt.CIC_KEY = cic.CIC_KEY) left join
EDW_ANALYTICS.CRM.dim_sales_location dsl on (oppt.sales_loc_key = dsl.SALES_LOCATION_KEY) left join
EDW_ANALYTICS.CRM.dim_sales_person dsp on (oppt.sales_person_key = dsp.Sales_Person_Key) left join
EDW_ANALYTICS.CRM.dim_sales_person dse on (oppt.sales_exec_key = dse.Sales_Person_Key) left join
EDW_ANALYTICS.CRM.dim_sales_person dsr on (oppt.responsible_key = dsr.Sales_Person_Key) left join
EDW_ANALYTICS.CRM.dim_sales_location dslr on (dsr.Sales_Location_Code = dslr.Sales_Location_Code) left join 
EDW_ANALYTICS.CRM.dim_quotation_status dqs on (oppt.quot_status_key = dqs.QUOTATION_STATUS_KEY) left join
EDW_ANALYTICS.CRM.dim_sales_order_status dss on (oppt.so_status_key = dss.Sales_Order_Status_Key) left join
EDW_ANALYTICS.CRM.dim_rejection_reason drj on (oppt.so_reject_key = drj.Rejection_Reason_key) left join
EDW_ANALYTICS.CRM.dim_opp_product_material dpm on (oppt.PRODUCT_MATERIAL_KEY = dpm.PRODUCT_MATERIAL_KEY) left join
EDW_ANALYTICS.ECC.dim_serial_equi sn on (oppt.serial_key = sn.Serial_Equi_Key) left join
EDW_ANALYTICS.CRM.dim_pwc dms on (oppt.market_sector_key = dms.PWC_KEY) left join
EDW_ANALYTICS.ECC.dim_business_area dba on (oppt.delivery_business_area_key = dba.Business_Area_Key) left join
EDW_ANALYTICS.CRM.dim_currency curr on (oppt.opp_CURRENCY_KEY = curr.CURRENCY_KEY) left join
EDW_ANALYTICS.CRM.dim_currency currq on (oppt.QUOT_CURRENCY_KEY = currq.CURRENCY_KEY) left join
EDW_ANALYTICS.ECC.dim_account da on (dc.Account_ID = da.account_id) left join
EDW_ANALYTICS.crm.dim_price_scenario dps on oppt.price_scenario_key = dps.price_scenario_key
where
    optp.opportunity_type_description not in ('N/A', 'Unknowan', 'LEAD')
	and ((oist.funel_status like '%Stage 3%' or opst.[description] like '%Stage 3%') or
	(oist.funel_status like '%Stage 4%' or opst.[description] like '%Stage 4%') or
	(oist.funel_status like '%Stage 5%' or opst.[description] like '%Stage 5%') or
	(oist.funel_status like '%Stage 6%' or opst.[description] like '%Stage 6%') or
	(oist.funel_status like '%won%' or opst.[description] like '%won%') or
	(oist.funel_status like '%delivered%' or opst.[description] like '%delivered%' ) or
	(oist.funel_status like '%lost%' or oist.funel_status like '%reviewed by superior%' or
	opst.[description] like '%lost%'))
	and year(oppt.lead_created_date) > 2020

order by oppt.LEAD_ID, oppt.opp_id, oppt.opp_item_no
;