delete from EDW_ANALYTICS.CRM.EC_fact_invoice where format(MTD,'yyyyMM') = format(CURRENT_TIMESTAMP, 'yyyyMM');
--delete from EDW_ANALYTICS.CRM.EC_fact_invoice where format(MTD,'yyyyMM') = '202111';

--CTE Forecast Daily
with forecaseDaily as(
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
	CONVERT(date, CAST(oppt.REPORT_DATE_KEY AS varchar)) as LOAD_DATE
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
			and left(oppt.LEAD_DATE_KEY, 4) in (YEAR(GETDATE()), YEAR(GETDATE())-1)
            
)
--CTE Order Tracking Daily
,invoice as (
select 
		cast(VBELN_VBAP as int) SalesDocument, 
		POSNR_VBAP SalesDocumentItem,
		AUART SalesDocumentType,
		--uepos HigerLevelItem,
		BSTKD_E SpaNo,
		a.BSTKD PurchaseOrderNo,
		BSTDK PurchaseOrderDate,
		a.VKORG SalesOrganization,
		--a.SPART Division,
		a.AREA Area,
		a.VKBUR SalesOffice,
		a.KUNNR_VBAK SoldToParty,
		a.Gtext EndDestination,
		--a.Name1_bilpar BilltoPartyname,
		a.KUNNR_PAYER Payer,
		a.Name2_Payer Payername,
		a.Kunnr_finance FinancingCompany,
		a.name2_finance FinancingCompanyName,
		a.VTEXT_t179T Model,
		b.BWTAR Batch,
		b.SERNR serialNumber,
		b.MFRPN MaterialNumber,
		a.Arktx MaterialDescription,
		a.HERKL Source,
		a.INCO1 Incoterms,
		a.INCO2 IncotermsPart2,
		a.MVGR2 PWCCode,
		a.MVGR3 ApplicationCode,
		cic.cic_group CICGroup,
		a.DELDATE RequestDeliveryDate,
		--a.Payment TermsofPayment,
		--a.PAYMENTNOTE PaymentTermsNote,
		a.VTEXT Description,
		a.NETWR Price,
		a.MWSBP Tax ,
		--a.WAERK PDC_PDG_curr,
		a.SALID SalesmanID,
		a.SALNAME SalesmanName,
		--a.DSALID DeliverySalesmanID,
		--a.DSALNAME DeliverySalesmanName,
		--a.MKTPGM Marketingprogram,
		--a.VCHR Voucher,
		--a.CCR CCRNo,
		a.RLAPNO ReleaseApprovalNo,
		a.RLAPDT ReleaseApprovalDate,
		a.RLAPBY ReleaseApprovalBy,
		 case when a.Created='X' THEN 'Yes' else 'No' end Created,
		 case when a.REQSTAT='X' THEN 'Yes' else 'No' end RequestChangeStatustoWorkable,
		 case when a.WOrkable='X' THEN 'Yes' else 'No' end Workable,
		 case when a.REQODCHK='X' THEN 'Yes' else 'No' end RequestToDoOverdueCheck,
		 case when a.REQRLAPNO='X' THEN 'Yes' else 'No' end RequestReleaseApprovalNo,
		 case when a.RELAPPR='X' THEN 'Yes' else 'No' end ReleaseApproval,
		 case when a.FULLY='X' THEN 'Yes' else 'No' end FullyPaid,
		 case when po.ZZLEASING_STAT = 'X' THEN 'Yes' else 'No' end POLeasing,
		--a.NOTEBR NotesforOverdueBranch,
		--a.NOTEGRP NotesforOverdueGroup,
		a.REMARKS Remarks,
		a.STCD1 NPWP,
		a.VBELN_LIPS Delivery,
		a.BLDAT DeliveryDate,
		a.BAST_NO BAST_NO,
		a.WBSTK StatusPGI,
		a.WADAT_IST PGIdate,
		a.E_DATLO BASTSigndate ,
		a.PDSTK ProofOfdeliveryStatus,
		a.PODAT ProofOfdeliverydate,
		CONVERT(BIGINT,b.BILLING_DOC) BillingDocument,
		b.BILLING_DATE  BillingDate,
		--a.ZREJECT_RSN ReasonforRejection,
		--a.PDC_PDG PDC_PDG,
		a.Amount Amount ,
		a.DUE_DATE DueDate,
		--a.VTWEG DistributionChannel,
		--a.Region Region,
		a.NAME1_SOl SoldToPartyName ,
		a.GSBER BusinessArea,
		--a.TELF1 CustomerPhoneNo,
		--a.Name_ser ServiceContact,
		--a.NAME_CON SalesContact,
		a.KUNNR_BILPAR BillToParty,
		a.KWMENG Quantity,
		a.CURR_So SOCurr,
		a.dat_lease LeasingFundingDate,
		a.dat_Sppka SPPKADate,
		b.ACTUAL_NETREV ActualNetRevenue,
		a.DAT_LEASE SODeliveryDate,
		b.Division typeDivision,
		b.mvgr1 market_sector,
		null DeliveryFlag ,
		null PP_ST3_ST5 ,
		null ProgressStatus ,
		b.ACTUAL_NETREV * (ABS(d.kurrf)) ActualNetInIDR,
		b.ACTUAL_NETREV ActualNetInUSD,
		da.customer_class_code CustomerClassCodes,
		a.MATNR_VBAP as ProductCode,
		dpm.PRODUCT_HIERARCHY
		-- into EDW_ANALYTICS.dbo.invoice
		from [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_V_ODRPP_BW a left join 
		[LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_F_INV_SOLD b on a.VBELN_VBAP = b.VBELN and a.POSNR_VBAP =b.POSNR
		left join [LS_BI_PROD].EDW_ANALYTICS.ECC.dim_account da on da.account_id = a.KUNNR_VBAK
		left join [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.VBRK d on b.BILLING_DOC = d.VBELN
		left join [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_V_SO_OPPTY po on cast(VBELN_VBAP as int) = po.SO_NUMBER and POSNR_VBAP = po.SO_ITEM
		left join (select DISTINCT cic_group_id,cic_group from ls_bi_prod.EDW_DIMENSION.CRM.Dim_CIC) cic on cic.cic_group_id = b.brsch
		left join (select * from [LS_BI_PROD].EDW_ANALYTICS.CRM.dim_opp_product_material where product_material_code not in ('N/A', 'Unknown') and left(product_hierarchy, 2) in ('M1','E1','F1')) dpm on CAST(a.MATNR_VBAP AS INT) = CAST (dpm.PRODUCT_MATERIAL_CODE AS INT)
		where 1=1
		and format(a.DELDATE,'yyyy') between (format(CURRENT_TIMESTAMP,'yyyy') - 1) and (format(CURRENT_TIMESTAMP,'yyyy') + 1)
		--and format(a.DELDATE,'yyyyMM') in (format(DATEADD(MONTH, -1, CURRENT_TIMESTAMP),'yyyyMM'), format(CURRENT_TIMESTAMP,'yyyyMM'))
)
--CTE Logscore
,logscore as (
select 
	b.sernr,
	b.CUST_NAME,
	a.vbrk_fkdat, 
	b.billing_date, 
	ScoreLog.SCORE_DATE,
	case when ScoreLog.SCORE_DATE < DATEADD(DAY,1,EOMONTH(b.Billing_Date)) and format(b.billing_date, 'yyyy-MM') <> format(ScoreLog.SCORE_DATE, 'yyyy-MM') and ScoreLog.CUST_NAME_OR_RENT_CUST_NAME = b.CUST_NAME then 0
		when ScoreLog.SCORE_DATE  < DATEADD(DAY,1,EOMONTH(b.Billing_Date)) and format(b.billing_date, 'yyyy-MM') <> format(ScoreLog.SCORE_DATE, 'yyyy-MM') and ScoreLog.CUST_NAME_OR_RENT_CUST_NAME <> b.CUST_NAME then 1
	else 1 end hitung

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
)
--CTE Order Tracking ST3/ST5
,st3 as (
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
--CTE for Invoiced PP
,invoiced as (
select 
	distinct a.*,
	case 
		when das.area_name like '%Java' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Java'
		when das.area_name like '%Sumatera' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Sumatera'
	else das.area_name end as area_name,
	case when das.area_name like '% MA' Then 'Major Account' else 'Retail Account' end Customer_Type,
	case 
		when das.area_name like '% MA' Then CICGroups 
	else 
		case 
			when (market_sector is null or market_sector='') then CICGroups 
		else market_sector end 
	end MarketSector
	,0 rate
from (
select distinct * from (
select 
	i.*,
	i.BillingDate as MTD,
	i.BillingDate as DeliveryDatef,
	'PP' as sales_type,
	f.Forecast,
	f.ConfidenceLevel,
	f.SOID,
	f.SOItemNo,
	f.OpportunityStatus,
	f.customer_key,
	i.CustomerClassCodeS CustomerClassCode,
	f.OpportunityID, 
	f.OpportunityItemNo,
	f.product_material_key,
	f.ProductID,
	f.ProductModel,
	f.INDUSTRY_KEY,
	f.VerticalIndustry, 
	i.ActualNetInIDR NetValueInIDR,
	i.ActualNetInUSD NetValueInUSD,
	f.SerialNo,
	f.market_sector_key,
	case 
		when i.SalesOrganization = '0Z02' then 
			case 
				when dcm.Sales_code is not null then 
					case 
						when dcm.Flag ='Exception' then '0ZY2'
						when left(MaterialNumber, 2)='E1' then dcm.Sales_code
						else d.sales_code
					end
				else  d.sales_code
			end
		when i.SalesOrganization = '1Z02' then
			case
				when i.BusinessArea = '0Z02' then d.sales_code
				else i.BusinessArea
			end
	end sales_off_code,
	f.sales_off_code Business_Area_Key,
	CASE 
		WHEN left(MaterialNumber, 2)='M1' THEN 'MACHINE'
		WHEN left(MaterialNumber, 2)='E1'THEN 'ENGINE'
		WHEN left(MaterialNumber, 2)='F1' THEN 'FORK_LIFT'
	else left(MaterialNumber, 2) end MaterialType,
	--case 
	--	when c.OpportunityID is null then 'No'
	--else 'Yes' end isForecast,
	i.CICGroup CICGroups,
	CAST(i.SoldToParty as INT) AccountID,
	i.SoldToPartyName AccountName,
	i.PRODUCT_HIERARCHY ProductHierarchy,
	da.Customer_Group
	--,CONCAT(CAST(i.SoldToParty as INT),i.ProductHierarchy, year(i.BillingDate), month(i.BillingDate)) AS forecastedkey

from invoice i 
left join forecaseDaily f on i.salesDocument = f.SOID and i.salesdocumentItem = f.SOItemNo  
--left join EDW_ANALYTICS.dbo.forecast c on  c.OpportunityID = f.OpportunityID and c.OpportunityItemNo = f.OpportunityItemNo
left join EDW_ANALYTICS.CRM.EC_dim_area_sales d on i.SalesmanID  = d.sales_id
--left join EDW_ANALYTICS.CRM.EC_dim_area_store das on d.sales_code = das.sales_code
left join EDW_ANALYTICS.CRM.EC_dim_company_exception_map dcm on dcm.CompanyID = CAST(i.SoldToParty as INT)
left join logscore ls on ls.sernr = i.serialnumber and ls.billing_date = i.billingdate
left join EDW_ANALYTICS.CRM.EC_dim_customer_group_mapping da on i.SoldToParty = da.Account_ID

where 1=1
--and opportunityType in ('Opp  Machine','Opp  Engine','Opp  ForkLift')
and format(i.BillingDate,'yyyyMM') = format(CURRENT_TIMESTAMP, 'yyyyMM')
--and format(i.BillingDate,'yyyyMM') = '202111'
--and format(i.BillingDate,'yyyy') = format(CURRENT_TIMESTAMP,'yyyy')
and left(MaterialNumber, 2) in ('M1','E1','F1')
and (ls.hitung = 1 or left(MaterialNumber, 2) = 'F1' or i.SalesDocumentType <> 'ZEPP' or ls.SCORE_DATE is null)
--and BillingDocument is not null
--and confidence_level>=75
--and forecast ='YES'
) abc
) a 
left join EDW_ANALYTICS.CRM.EC_dim_area_store das on das.sales_code = a.sales_off_code
)
--CTE for Progress Data PP
,pp_progress as (
select 
	distinct a.*,
	case 
		when das.area_name like '%Java' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Java'
		when das.area_name like '%Sumatera' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Sumatera'
	else das.area_name end as area_name,
	case when das.area_name like '% MA' Then 'Major Account' else 'Retail Account' end Customer_Type,
	case 
		when das.area_name like '% MA' Then CICGroups 
	else 
		case 
			when (market_sector is null or market_sector='') then CICGroups 
		else market_sector end 
	end MarketSector
	,0 rate
from (
select 
	i.*,
	f.deliverydate as MTD,
	f.deliverydate as DeliveryDatef,
	case when f.opportunityType in ('Opp  Machine','Opp  Engine','Opp  ForkLift') then 'PP' 
	else f.sales_type end sales_type,
	f.Forecast,
	f.ConfidenceLevel,
	f.SOID,
	f.SOItemNo,
	f.OpportunityStatus,
	f.customer_key,
	f.CustomerClassCode,
	f.OpportunityID, 
	f.OpportunityItemNo,
	f.product_material_key,
	f.ProductID,
	f.ProductModel,
	f.INDUSTRY_KEY,
	f.VerticalIndustry,
	f.NetValueInIDR,
	f.NetValueInUSD,
	f.SerialNo,
	f.market_sector_key,
	case 
		when dcm.Sales_code is not null then 
			case 
				when dcm.Flag ='Exception' then '0ZY2'
				when left(MaterialNumber, 2)='E1' and f.sub_company_name ='TUS' then dcm.Sales_code
				else i.BusinessArea
			end
		else i.BusinessArea
	end sales_off_code,
	f.sales_off_code Business_Area_Key,
	CASE 
		WHEN f.Category_ID='M1' THEN 'MACHINE'
		when f.Category_ID='E1'THEN 'ENGINE'
		WHEN f.Category_ID='F1' THEN 'FORK_LIFT'
	else f.Category_ID end MaterialType,
	--case 
	--	when c.OpportunityID is null then 'No' 
	--else 'Yes' end isForecast,
	f.MarketSector CICGroups,
	f.AccountID,
	f.AccountName,
	f.PRODUCT_HIERARCHY ProductHierarchy,
	da.Customer_Group
	--,CONCAT(f.AccountID,i.ProductHierarchy, year(f.deliverydate), month(f.deliverydate)) AS forecastedkey

from forecaseDaily f 
left join invoice i on i.salesDocument = f.SOID and i.salesdocumentItem = f.SOItemNo 
--left join EDW_ANALYTICS.dbo.forecast c on c.OpportunityID = f.OpportunityID and c.OpportunityItemNo = f.OpportunityItemNo 
left join EDW_ANALYTICS.CRM.EC_dim_area_store das on das.sales_code = f.SalesOffice
left join EDW_ANALYTICS.CRM.EC_dim_company_exception_map dcm on dcm.CompanyID = f.AccountID
left join EDW_ANALYTICS.CRM.EC_dim_customer_group_mapping da on concat('000',cast(f.AccountID as nvarchar(10))) = da.Account_ID

where 1=1
and f.opportunityType in ('Opp  Machine','Opp  Engine','Opp  ForkLift')
and format(f.deliverydate,'yyyyMM') = format(CURRENT_TIMESTAMP, 'yyyyMM')
--and format(f.deliverydate,'yyyyMM') = '202111'
--and format(f.deliverydate,'yyyy') = format(CURRENT_TIMESTAMP, 'yyyy')
and f.confidencelevel>=75
and left(f.product_hierarchy,2) in ('M1','E1','F1')
and f.forecast ='YES'
and (i.salesdocument is null or i.billingDocument is null)
) a 
left join EDW_ANALYTICS.CRM.EC_dim_area_store das on das.sales_code = a.sales_off_code
)
--CTE for Invoiced ST3/ST5
,st3_invoiced as (
select 
	distinct a.*,
	case 
		when das.area_name like '%Java' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Java'
		when das.area_name like '%Sumatera' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Sumatera'
	else das.area_name end as area_name,
	case when das.area_name like '% MA' Then 'Major Account' else 'Retail Account' end Customer_Type,
	case 
		when das.area_name like '% MA' Then CICGroups 
	else 
		case 
			when (market_sector is null or market_sector='') then CICGroups 
		else market_sector end 
	end MarketSector
	,0 rate
from (
SELECT
	CASE
		WHEN st3.VBELN_VA IS NULL THEN 333555
		ELSE VBELN_VA
	END AS SalesDocument
	,CASE
		WHEN st3.POSNR_VA IS NULL THEN 0
		ELSE POSNR_VA
	END AS SalesDocumentItem
	,'-' AS SalesDocumentType
	--,0 AS HigerLevelItem
	,st3.BSTKD AS SpaNo
	,'-' AS PurchaseOrderNo
	,st3.BSTDK AS PurchaseOrderDate
	,NULL AS SalesOrganization
	--,NULL AS Division
	,NULL AS Area
	,NULL AS SalesOffice
	,NULL AS SoldToParty
	,st3.GTEXT AS EndDestination
	--,NULL AS BilltoPartyname
	,st3.PAYER AS Payer
	,st3.PAYER_NM AS Payername
	,st3.FIN AS FinancingCompany
	,st3.FIN_NM AS FinancingCompanyName
	,NULL AS Model
	,CAST(st3.EQUNR AS INT) AS Batch
	,st3.SERNR AS serialNumber
	,NULL AS MaterialNumber
	,NULL AS MaterialDescription
	,NULL AS Source
	,st3.INCO1 AS Incoterms
	,st3.INCO2 AS IncotermsPart2
	,st3.MVGR2 AS PWCCode
	,NULL AS ApplicationCode
	,NULL AS CICCode
	,NULL AS RequestDeliveryDate
	--,NULL AS TermsofPayment
	--,NULL AS PaymentTermsNote
	,st3.ZTERM_NM AS Description
	,NULL AS Price
	,NULL AS Tax
	--,NULL AS PDC_PDG_curr
	,st3.SLS AS SalesmanID
	,st3.SLS_NM AS SalesmanName
	--,NULL AS DeliverySalesmanID
	--,NULL AS DeliverySalesmanName
	--,NULL AS Marketingprogram
	--,NULL AS Voucher
	--,NULL AS CCRNo
	,NULL AS ReleaseApprovalNo
	,NULL AS ReleaseApprovalDate
	,NULL AS ReleaseApprovalBy
	,NULL AS Created
	,NULL AS RequestChangeStatustoWorkable
	,NULL AS Workable
	,NULL AS RequestToDoOverdueCheck
	,NULL AS RequestReleaseApprovalNo
	,NULL AS ReleaseApproval
	,NULL AS FullyPaid
	,NULL AS POLeasing
	--,NULL AS NotesforOverdueBranch
	--,NULL AS NotesforOverdueGroup
	,NULL AS Remarks
	,NULL AS NPWP
	,NULL AS Delivery
	,NULL AS DeliveryDate
	,CASE
		WHEN st3.BAST_NO IS NULL THEN NULL
		ELSE st3.BAST_NO
	END AS BAST_NO
	,NULL AS StatusPGI
	,NULL AS PGIdate
	,NULL AS BASTSigndate
	,NULL AS ProofOfdeliveryStatus
	,NULL AS ProofOfdeliverydate
	,st3.BAST_NO AS BillingDocument
	,st3.FKDAT AS BillingDate
	--,NULL AS ReasonforRejection
	--,NULL AS PDC_PDG
	,NULL AS Amount
	,NULL AS DueDate
	--,NULL AS DistributionChannel
	--,NULL AS Region
	,NULL AS SoldToPartyName
	,st3.GSBER AS BusinessArea
	--,NULL AS CustomerPhoneNo
	--,NULL AS ServiceContact
	--,NULL AS SalesContact
	,NULL AS BillToParty
	,1 AS Quantity
	,NULL AS SOCurr
	,NULL AS LeasingFundingDate
	,NULL AS SPPKADate
	,NULL AS ActualNetRevenue
	,NULL AS SODeliveryDate
	,NULL AS typeDivision
	,NULL AS market_sector
	,NULL AS DeliveryFlag
	,NULL AS PP_ST3_ST5
	,NULL AS ProgressStatus
	,NULL AS ActualNetInIDR
	,NULL AS ActualNetInUSD
	,NULL AS CustomerClassCodes
	,st3.MATNR AS ProductCode
	,st3.PRODUCT_HIERARCHY
	,st3.SCORE_DATE AS MTD
	,st3.SCORE_DATE AS DeliveryDatef
	,CASE 
		WHEN st3.SALE_TYPE = 3 THEN 'ST3'
		WHEN st3.SALE_TYPE = 5 THEN 'ST5'
	END AS sales_type
	/*,st3.Area_Name
	,CASE 
		WHEN st3.area_name like '%MA' THEN 'Major Account' 
		ELSE 'Retail Account' 
	END AS Customer_Type*/
	,f.Forecast
	,f.ConfidenceLevel
	,f.SOID
	,f.SOItemNo
	,f.OpportunityStatus
	,f.customer_key
	,f.CustomerClassCode
	,f.OpportunityID
	,f.OpportunityItemNo
	,f.product_material_key
	,f.ProductID
	,st3.MODEL as ProductModel
	,f.INDUSTRY_KEY
	,f.VerticalIndustry
	,0 NetValueInIDR
	,st3.NETWR AS NetValueInUSD
	,st3.PRIME_PRODUCT_SERIAL_NUMBER as SerialNo
	,f.market_sector_key
	--,f.MarketSector
	,st3.sales_code AS sales_off_code
	,st3.sales_code AS Business_Area_Key
	,st3.MaterialType
	--,CASE 
	--	WHEN c.OpportunityID is null THEN 'No' 
	--	ELSE 'Yes' 
	--END AS isForecast
	,f.CICGroup CICGroups
	,CAST(st3.SOLD AS INT) AccountID
	,st3.SOLD_NM AccountName
	,st3.PRODUCT_HIERARCHY ProductHierarchy
	,da.Customer_Group
	--,CONCAT(f.AccountID,st3.ProductHierarchy, year(st3.SCORE_DATE), month(st3.SCORE_DATE)) AS forecastedkey

FROM st3
LEFT JOIN forecaseDaily AS f ON st3.VBELN_VA = f.SOID AND st3.POSNR_VA = f.SOItemNo
--LEFT JOIN EDW_ANALYTICS.dbo.forecast AS c ON c.OpportunityID = f.OpportunityID AND c.OpportunityItemNo = f.OpportunityItemNo
LEFT JOIN EDW_ANALYTICS.CRM.EC_dim_area_store das ON das.sales_code = st3.sales_code
left join EDW_ANALYTICS.CRM.EC_dim_customer_group_mapping da on st3.SOLD = da.Account_ID

WHERE 1=1
--AND f.opportunityType IN ('Opp  Rental')
--AND f.Category_ID in ('M1','E1','F1')
and format(st3.SCORE_DATE,'yyyyMM') = format(CURRENT_TIMESTAMP, 'yyyyMM')
--and format(st3.SCORE_DATE,'yyyyMM') = '202111'
--and format(st3.SCORE_DATE,'yyyy') = format(CURRENT_TIMESTAMP, 'yyyy')
AND st3.BAST_NO IS NOT NULL AND st3.BAST_NO <> ''
) a 
left join EDW_ANALYTICS.CRM.EC_dim_area_store das on das.sales_code = a.sales_off_code
)
--CTE for ST3/ST5 Progress Data
,st3_progress as (
select 
	distinct a.*,
	case 
		when das.area_name like '%Java' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Java'
		when das.area_name like '%Sumatera' and MaterialType in ('MACHINE', 'FORK_LIFT') then 'Sumatera'
	else das.area_name end as area_name,
	case when das.area_name like '% MA' Then 'Major Account' else 'Retail Account' end Customer_Type,
	case 
		when das.area_name like '% MA' Then CICGroups 
	else 
		case 
			when (market_sector is null or market_sector='') then CICGroups 
		else market_sector end 
	end MarketSector
	,0 rate
from (
SELECT
	CASE
		WHEN st3.VBELN_VA IS NULL THEN 333555
		ELSE VBELN_VA
	END AS SalesDocument
	,CASE
		WHEN st3.POSNR_VA IS NULL THEN 0
		ELSE POSNR_VA
	END AS SalesDocumentItem
	,'-' AS SalesDocumentType
	--,0 AS HigerLevelItem
	,st3.BSTKD AS SpaNo
	,'-' AS PurchaseOrderNo
	,st3.BSTDK AS PurchaseOrderDate
	,NULL AS SalesOrganization
	--,NULL AS Division
	,NULL AS Area
	,NULL AS SalesOffice
	,NULL AS SoldToParty
	,st3.GTEXT AS EndDestination
	--,NULL AS BilltoPartyname
	,st3.PAYER AS Payer
	,st3.PAYER_NM AS Payername
	,st3.FIN AS FinancingCompany
	,st3.FIN_NM AS FinancingCompanyName
	,NULL AS Model
	,CAST(st3.EQUNR AS INT) AS Batch
	,st3.SERNR AS serialNumber
	,NULL AS MaterialNumber
	,NULL AS MaterialDescription
	,NULL AS Source
	,st3.INCO1 AS Incoterms
	,st3.INCO2 AS IncotermsPart2
	,st3.MVGR2 AS PWCCode
	,NULL AS ApplicationCode
	,NULL AS CICCode
	,NULL AS RequestDeliveryDate
	--,NULL AS TermsofPayment
	--,NULL AS PaymentTermsNote
	,st3.ZTERM_NM AS Description
	,NULL AS Price
	,NULL AS Tax
	--,NULL AS PDC_PDG_curr
	,st3.SLS AS SalesmanID
	,st3.SLS_NM AS SalesmanName
	--,NULL AS DeliverySalesmanID
	--,NULL AS DeliverySalesmanName
	--,NULL AS Marketingprogram
	--,NULL AS Voucher
	--,NULL AS CCRNo
	,NULL AS ReleaseApprovalNo
	,NULL AS ReleaseApprovalDate
	,NULL AS ReleaseApprovalBy
	,NULL AS Created
	,NULL AS RequestChangeStatustoWorkable
	,NULL AS Workable
	,NULL AS RequestToDoOverdueCheck
	,NULL AS RequestReleaseApprovalNo
	,NULL AS ReleaseApproval
	,NULL AS FullyPaid
	,NULL AS POLeasing
	--,NULL AS NotesforOverdueBranch
	--,NULL AS NotesforOverdueGroup
	,NULL AS Remarks
	,NULL AS NPWP
	,NULL AS Delivery
	,NULL AS DeliveryDate
	,CASE
		WHEN st3.BAST_NO IS NULL THEN NULL
		ELSE st3.BAST_NO
	END AS BAST_NO
	,NULL AS StatusPGI
	,NULL AS PGIdate
	,NULL AS BASTSigndate
	,NULL AS ProofOfdeliveryStatus
	,NULL AS ProofOfdeliverydate
	,NULL AS BillingDocument
	,NULL AS BillingDate
	--,NULL AS ReasonforRejection
	--,NULL AS PDC_PDG
	,NULL AS Amount
	,NULL AS DueDate
	--,NULL AS DistributionChannel
	--,NULL AS Region
	,NULL AS SoldToPartyName
	,st3.GSBER AS BusinessArea
	--,NULL AS CustomerPhoneNo
	--,NULL AS ServiceContact
	--,NULL AS SalesContact
	,NULL AS BillToParty
	,1 AS Quantity
	,NULL AS SOCurr
	,NULL AS LeasingFundingDate
	,NULL AS SPPKADate
	,NULL AS ActualNetRevenue
	,NULL AS SODeliveryDate
	,NULL AS typeDivision
	,NULL AS market_sector
	,NULL AS DeliveryFlag
	,NULL AS PP_ST3_ST5
	,NULL AS ProgressStatus
	,NULL AS ActualNetInIDR
	,NULL AS ActualNetInUSD
	,NULL AS CustomerClassCodes
	,st3.MATNR AS ProductCode
	,st3.PRODUCT_HIERARCHY
	,f.deliverydate AS MTD
	,f.deliverydate AS DeliveryDatef
	,f.sales_type
	/*,das.area_name Area_Name
	,CASE 
		WHEN das.area_name like '%MA' THEN 'Major Account' 
		ELSE 'Retail Account' 
	END AS Customer_Type*/
	,f.Forecast
	,f.ConfidenceLevel
	,f.SOID
	,f.SOItemNo
	,f.OpportunityStatus
	,f.customer_key
	,f.CustomerClassCode
	,f.OpportunityID
	,f.OpportunityItemNo
	,f.product_material_key
	,f.ProductID
	,f.ProductModel
	,f.INDUSTRY_KEY
	,f.VerticalIndustry
	,f.NetValueInIDR
	,f.NetValueInUSD
	,f.SerialNo
	,f.market_sector_key
	--,f.MarketSector
	,f.sales_off_code
	,f.sales_off_code AS Business_Area_Key
	,CASE
		WHEN f.Category_ID = 'M1' THEN 'MACHINE'
		WHEN f.Category_ID = 'E1' THEN 'ENGINE'
		WHEN f.Category_ID = 'F1' THEN 'FORK_LIFT'
		ELSE f.Category_ID 
	END AS MaterialType
	--,CASE 
	--	WHEN c.OpportunityID is null then 'No' 
	--	ELSE 'Yes' 
	--END AS isForecast
	,f.CICGroup CICGroups
	,f.AccountID
	,f.AccountName
	,f.PRODUCT_HIERARCHY ProductHierarchy
	,da.Customer_Group
	--,CONCAT(f.AccountID,st3.ProductHierarchy, year(f.deliverydate), month(f.deliverydate)) AS forecastedkey

FROM forecaseDaily AS f
LEFT JOIN st3 ON st3.VBELN_VA = f.SOID AND st3.POSNR_VA = f.SOItemNo
--LEFT JOIN EDW_ANALYTICS.dbo.forecast AS c ON c.OpportunityID = f.OpportunityID AND c.OpportunityItemNo = f.OpportunityItemNo
LEFT JOIN EDW_ANALYTICS.CRM.EC_dim_area_store das ON das.sales_code = f.SalesOffice
left join EDW_ANALYTICS.CRM.EC_dim_customer_group_mapping da on concat('000',cast(f.AccountID as nvarchar(10))) = da.Account_ID


WHERE 1=1
AND f.opportunityType IN ('Opp  Rental')
AND f.sales_type in ('ST3','ST5')
AND f.confidencelevel >= 75
and left(f.product_hierarchy,2) in ('M1','E1','F1')
AND f.forecast ='YES'
and format(f.deliverydate,'yyyyMM') = format(CURRENT_TIMESTAMP, 'yyyyMM')
--and format(f.deliverydate,'yyyyMM') ='202111'
--and format(f.deliverydate,'yyyy') = format(CURRENT_TIMESTAMP, 'yyyy')
AND (st3.BAST_NO IS NULL OR st3.BAST_NO = '')
) a 
left join EDW_ANALYTICS.CRM.EC_dim_area_store das on das.sales_code = a.sales_off_code
)
--CTE Invoiced PP that mapped as carried over
,invoiced_carriedover as (
select b.*, case when c.OpportunityID is not null then 'Carried Over' else 'No' end as isForecast 
from (

select 
	a.*
	,ROW_NUMBER() OVER(PARTITION BY format(a.MTD, 'yyyy'),format(a.MTD, 'MM'), a.accountID, a.producthierarchy ORDER BY format(a.MTD, 'yyyy') ASC,format(a.MTD, 'MM') ASC, a.accountID ASC, a.producthierarchy ASC, a.billingdocument desc) AS Row#
from invoiced a

) b
left join 
(select *, 
ROW_NUMBER() OVER(PARTITION BY format(DeliveryDate, 'yyyy') ,format(DeliveryDate, 'MM'),accountID, producthierarchy ORDER BY format(DeliveryDate, 'yyyy') ASC,format(DeliveryDate, 'MM') ASC,accountID ASC, producthierarchy ASC) AS Row#
from EDW_ANALYTICS.CRM.EC_fact_locked_forecast where FORECAST = 'Carried Over') as c 
on b.AccountID = c.AccountID and b.ProductHierarchy = c.ProductHierarchy and format(b.MTD, 'yyyy') = format(c.DeliveryDate, 'yyyy') and format(b.MTD, 'MM') = format(c.DeliveryDate, 'MM')and b.Row# = c.Row#
where c.OpportunityID is not null
)
--CTE Invoiced PP that mapped as forecasted
,invoiced_forecasted as (
select b.*, case when c.OpportunityID is not null then 'Yes' else 'No' end as isForecast 
from (

select 
	a.*
	,ROW_NUMBER() OVER(PARTITION BY format(a.MTD, 'yyyy'),format(a.MTD, 'MM'), a.accountID, a.producthierarchy ORDER BY format(a.MTD, 'yyyy') ASC,format(a.MTD, 'MM') ASC, a.accountID ASC, a.producthierarchy ASC, a.billingdocument desc) AS Row#
from invoiced a
left join invoiced_carriedover invd on a.SalesDocument = invd.SalesDocument and a.SalesDocumentItem = invd.SalesDocumentItem
where invd.SalesDocument is null

) b
left join 
(select *, 
ROW_NUMBER() OVER(PARTITION BY format(DeliveryDate, 'yyyy') ,format(DeliveryDate, 'MM'),accountID, producthierarchy ORDER BY format(DeliveryDate, 'yyyy') ASC,format(DeliveryDate, 'MM') ASC,accountID ASC, producthierarchy ASC) AS Row#
from EDW_ANALYTICS.CRM.EC_fact_locked_forecast where AccountID <> 1 and FORECAST <> 'Carried Over') as c 
on b.AccountID = c.AccountID and b.ProductHierarchy = c.ProductHierarchy and format(b.MTD, 'yyyy') = format(c.DeliveryDate, 'yyyy') and format(b.MTD, 'MM') = format(c.DeliveryDate, 'MM')and b.Row# = c.Row#
where c.OpportunityID is not null
)
--CTE Invoiced PP that mapped as forecasted by customer group
,invoiced_forecasted_customer_group as (
select 
	b.SalesDocument,
	b.SalesDocumentItem,
	b.SalesDocumentType,
	--b.HigerLevelItem,
	b.SpaNo,
	b.PurchaseOrderNo,
	b.PurchaseOrderDate,
	b.SalesOrganization,
	--b.Division,
	b.Area,
	b.SalesOffice,
	b.SoldToParty,
	b.EndDestination,
	--b.BilltoPartyname,
	b.Payer,
	b.Payername,
	b.FinancingCompany,
	b.FinancingCompanyName,
	b.Model,
	b.Batch,
	b.serialNumber,
	b.MaterialNumber,
	b.MaterialDescription,
	b.Source,
	b.Incoterms,
	b.IncotermsPart2,
	b.PWCCode,
	b.ApplicationCode,
	b.CICGroup,
	b.RequestDeliveryDate,
	--b.TermsofPayment,
	--b.PaymentTermsNote,
	b.Description,
	b.Price,
	b.Tax,
	--b.PDC_PDG_curr,
	b.SalesmanID,
	b.SalesmanName,
	--b.DeliverySalesmanID,
	--b.DeliverySalesmanName,
	--b.Marketingprogram,
	--b.Voucher,
	--b.CCRNo,
	b.ReleaseApprovalNo,
	b.ReleaseApprovalDate,
	b.ReleaseApprovalBy,
	b.Created,
	b.RequestChangeStatustoWorkable,
	b.Workable,
	b.RequestToDoOverdueCheck,
	b.RequestReleaseApprovalNo,
	b.ReleaseApproval,
	b.FullyPaid,
	b.POLeasing,
	--b.NotesforOverdueBranch,
	--b.NotesforOverdueGroup,
	b.Remarks,
	b.NPWP,
	b.Delivery,
	b.DeliveryDate,
	b.BAST_NO,
	b.StatusPGI,
	b.PGIdate,
	b.BASTSigndate,
	b.ProofOfdeliveryStatus,
	b.ProofOfdeliverydate,
	b.BillingDocument,
	b.BillingDate,
	--b.ReasonforRejection,
	--b.PDC_PDG,
	b.Amount,
	b.DueDate,
	--b.DistributionChannel,
	--b.Region,
	b.SoldToPartyName,
	b.BusinessArea,
	--b.CustomerPhoneNo,
	--b.ServiceContact,
	--b.SalesContact,
	b.BillToParty,
	b.Quantity,
	b.SOCurr,
	b.LeasingFundingDate,
	b.SPPKADate,
	b.ActualNetRevenue,
	b.SODeliveryDate,
	b.typeDivision,
	b.market_sector,
	b.DeliveryFlag,
	b.PP_ST3_ST5,
	b.ProgressStatus,
	b.ActualNetInIDR,
	b.ActualNetInUSD,
	b.CustomerClassCodes,
	b.ProductCode,
	b.PRODUCT_HIERARCHY,
	b.MTD,
	b.DeliveryDatef,
	b.sales_type,
	b.Forecast,
	b.ConfidenceLevel,
	b.SOID,
	b.SOItemNo,
	b.OpportunityStatus,
	b.customer_key,
	b.CustomerClassCode,
	b.OpportunityID,
	b.OpportunityItemNo,
	b.product_material_key,
	b.ProductID,
	b.ProductModel,
	b.INDUSTRY_KEY,
	b.VerticalIndustry,
	b.NetValueInIDR,
	b.NetValueInUSD,
	b.SerialNo,
	b.market_sector_key,
	b.sales_off_code,
	b.Business_Area_Key,
	b.MaterialType,
	b.CICGroups,
	b.AccountID,
	CONCAT(b.AccountName, ' / ',c.AccountName) as AccountName,
	b.ProductHierarchy,
	b.Customer_Group,
	b.area_name,
	b.Customer_Type,
	b.MarketSector,
	b.rate,
	b.Row#,
	case when c.OpportunityID is not null then 'Yes' else 'No' end as isForecast 
from (

select 
	a.*,
	ROW_NUMBER() OVER(PARTITION BY format(a.MTD, 'yyyy'),format(a.MTD, 'MM'), a.Customer_Group, a.producthierarchy ORDER BY format(a.MTD, 'yyyy') ASC,format(a.MTD, 'MM') ASC, a.Customer_Group ASC, a.producthierarchy ASC, a.billingdocument desc) AS Row#
from invoiced a
left join (select * from invoiced_forecasted union select * from invoiced_carriedover) invd on a.SalesDocument = invd.SalesDocument and a.SalesDocumentItem = invd.SalesDocumentItem
where invd.SalesDocument is null

) b
left join 
(select f.*, da.Customer_Group,
ROW_NUMBER() OVER(PARTITION BY format(f.DeliveryDate, 'yyyy') ,format(f.DeliveryDate, 'MM'),da.Customer_Group, f.producthierarchy ORDER BY format(f.DeliveryDate, 'yyyy') ASC,format(f.DeliveryDate, 'MM') ASC,da.Customer_Group ASC, f.producthierarchy ASC) AS Row#
from EDW_ANALYTICS.CRM.EC_fact_locked_forecast f
left join EDW_ANALYTICS.CRM.EC_dim_customer_group_mapping da on concat('000',cast(f.AccountID as nvarchar(10))) = da.Account_ID
where f.AccountID <> 1 and FORECAST <> 'Carried Over' and da.Customer_Group is not null and da.Customer_Group <> '') as c 
on b.Customer_Group = c.Customer_Group and b.AccountID <> c.AccountID and b.ProductHierarchy = c.ProductHierarchy and format(b.MTD, 'yyyy') = format(c.DeliveryDate, 'yyyy') and format(b.MTD, 'MM') = format(c.DeliveryDate, 'MM')and b.Row# = c.Row#
where c.OpportunityID is not null
)

--CTE Invoiced ST3/ST5 that mapped as forecasted
,st3_invoiced_forecasted as (
select b.*, case when c.OpportunityID is not null then 'Yes' else 'No' end as isForecast 
from (

select 
	a.*
	,ROW_NUMBER() OVER(PARTITION BY format(MTD, 'yyyy'),format(MTD, 'MM'), accountID, producthierarchy ORDER BY format(MTD, 'yyyy') ASC,format(MTD, 'MM') ASC, accountID ASC, producthierarchy ASC, billingdocument desc) AS Row#
from st3_invoiced a 

) b
left join 
(select *, 
ROW_NUMBER() OVER(PARTITION BY format(DeliveryDate, 'yyyy') ,format(DeliveryDate, 'MM'),accountID, producthierarchy ORDER BY format(DeliveryDate, 'yyyy') ASC,format(DeliveryDate, 'MM') ASC,accountID ASC, producthierarchy ASC) AS Row#
from EDW_ANALYTICS.CRM.EC_fact_locked_forecast where AccountID <> 1) as c 
on b.AccountID = c.AccountID and b.ProductHierarchy = c.ProductHierarchy and format(b.MTD, 'yyyy') = format(c.DeliveryDate, 'yyyy') and format(b.MTD, 'MM') = format(c.DeliveryDate, 'MM')and b.Row# = c.Row#
where c.OpportunityID is not null
)
,tba_used as (
select c.OpportunityID, c.OpportunityItemNo
from (
	select 
		a.*
		,ROW_NUMBER() OVER(PARTITION BY format(a.MTD, 'yyyy'),format(a.MTD, 'MM'),a.producthierarchy, a.area_name ORDER BY format(a.MTD, 'yyyy') ASC,format(a.MTD, 'MM') ASC,a.producthierarchy ASC, a.area_name ASC, a.billingdocument desc) AS Row#
	from invoiced a 
	left join (select * from invoiced_forecasted union select * from invoiced_carriedover union select * from invoiced_forecasted_customer_group) invd on a.SalesDocument = invd.SalesDocument and a.SalesDocumentItem = invd.SalesDocumentItem
	where invd.SalesDocument is null
	) b
left join 
(
select *, ROW_NUMBER() OVER(PARTITION BY format(c3.DeliveryDate, 'yyyy') ,format(c3.DeliveryDate, 'MM'),c3.producthierarchy, c3.area_name ORDER BY format(c3.DeliveryDate, 'yyyy') ASC,format(c3.DeliveryDate, 'MM') ASC, c3.producthierarchy ASC, c3.area_name ASC) AS Row#
from (
	select c1.*, case when c2.area_name like '%Java' and left(c1.ProductHierarchy,2) in ('M1', 'F1') then 'Java' when c2.area_name like '%Sumatera' and left(c1.ProductHierarchy,2) in ('M1', 'F1') then 'Sumatera' else c2.area_name end as area_name
	from EDW_ANALYTICS.CRM.EC_fact_locked_forecast c1 
	left join EDW_ANALYTICS.CRM.EC_dim_area_store c2 on c1.sales_off_code = c2.sales_code 
	where AccountID = 1
	) as c3
) as c
on b.ProductHierarchy = c.ProductHierarchy and b.area_name = c.area_name and format(b.MTD, 'yyyy') = format(c.DeliveryDate, 'yyyy') and format(b.MTD, 'MM') = format(c.DeliveryDate, 'MM') and b.Row# = c.Row#
where c.OpportunityID is not null
)
,tba_used_st3 as (
select c.OpportunityID, c.OpportunityItemNo
from (
	select 
		a.*
		,ROW_NUMBER() OVER(PARTITION BY format(a.MTD, 'yyyy'),format(a.MTD, 'MM'),a.producthierarchy, a.area_name ORDER BY format(a.MTD, 'yyyy') ASC,format(a.MTD, 'MM') ASC,a.producthierarchy ASC, a.area_name ASC, a.billingdocument desc) AS Row#
	from st3_invoiced a 
	left join st3_invoiced_forecasted invd on a.SalesDocument = invd.SalesDocument and a.SalesDocumentItem = invd.SalesDocumentItem
	where invd.SalesDocument is null
	) b
left join 
(
select *, ROW_NUMBER() OVER(PARTITION BY format(c3.DeliveryDate, 'yyyy') ,format(c3.DeliveryDate, 'MM'),c3.producthierarchy, c3.area_name ORDER BY format(c3.DeliveryDate, 'yyyy') ASC,format(c3.DeliveryDate, 'MM') ASC, c3.producthierarchy ASC, c3.area_name ASC) AS Row#
from (
	select c1.*, case when c2.area_name like '%Java' and left(c1.ProductHierarchy,2) in ('M1', 'F1') then 'Java' when c2.area_name like '%Sumatera' and left(c1.ProductHierarchy,2) in ('M1', 'F1') then 'Sumatera' else c2.area_name end as area_name
	from EDW_ANALYTICS.CRM.EC_fact_locked_forecast c1 
	left join EDW_ANALYTICS.CRM.EC_dim_area_store c2 on c1.sales_off_code = c2.sales_code
	left join tba_used c4 on c1.OpportunityID = c4.OpportunityID and c1.OpportunityItemNo = c4.OpportunityItemNo
	where AccountID = 1 and c4.OpportunityID is null
	) as c3
) as c 
on b.ProductHierarchy = c.ProductHierarchy and b.area_name = c.area_name and format(b.MTD, 'yyyy') = format(c.DeliveryDate, 'yyyy') and format(b.MTD, 'MM') = format(c.DeliveryDate, 'MM')and b.Row# = c.Row#
where c.OpportunityID is not null
)

-----------------------------------------------------------------------------------------------------------------------

insert into EDW_ANALYTICS.CRM.EC_fact_invoice
select *, GETDATE() AS ETL_DATE
from (

--PP Invoiced Forecasted
select * from invoiced_forecasted

--PP Invoiced Carried Over
union
select * from invoiced_carriedover

--PP Invoiced Forecasted using Customer Group
union
select * from invoiced_forecasted_customer_group

--PP Invoiced Push Sales
union
select b.*, case when c.OpportunityID is not null then 'Yes' else 'No' end as isForecast 
from (
	select 
		a.*
		,ROW_NUMBER() OVER(PARTITION BY format(a.MTD, 'yyyy'),format(a.MTD, 'MM'),a.producthierarchy, a.area_name ORDER BY format(a.MTD, 'yyyy') ASC,format(a.MTD, 'MM') ASC,a.producthierarchy ASC, a.area_name ASC, a.billingdocument desc) AS Row#
	from invoiced a 
	left join (select * from invoiced_forecasted union select * from invoiced_carriedover union select * from invoiced_forecasted_customer_group) invd on a.SalesDocument = invd.SalesDocument and a.SalesDocumentItem = invd.SalesDocumentItem
	where invd.SalesDocument is null
	) b
left join 
(
select *, ROW_NUMBER() OVER(PARTITION BY format(c3.DeliveryDate, 'yyyy') ,format(c3.DeliveryDate, 'MM'),c3.producthierarchy, c3.area_name ORDER BY format(c3.DeliveryDate, 'yyyy') ASC,format(c3.DeliveryDate, 'MM') ASC, c3.producthierarchy ASC, c3.area_name ASC) AS Row#
from (
	select c1.*, case when c2.area_name like '%Java' and left(c1.ProductHierarchy,2) in ('M1', 'F1') then 'Java' when c2.area_name like '%Sumatera' and left(c1.ProductHierarchy,2) in ('M1', 'F1') then 'Sumatera' else c2.area_name end as area_name
	from EDW_ANALYTICS.CRM.EC_fact_locked_forecast c1 
	left join EDW_ANALYTICS.CRM.EC_dim_area_store c2 on c1.sales_off_code = c2.sales_code 
	where AccountID = 1
	) as c3
) as c 
on b.ProductHierarchy = c.ProductHierarchy and b.area_name = c.area_name and format(b.MTD, 'yyyy') = format(c.DeliveryDate, 'yyyy') and format(b.MTD, 'MM') = format(c.DeliveryDate, 'MM')and b.Row# = c.Row#

--ST3 Invoiced
union
select * from st3_invoiced_forecasted

--ST3 Push Sales
union
select b.*, case when c.OpportunityID is not null then 'Yes' else 'No' end as isForecast 
from (
	select 
		a.*
		,ROW_NUMBER() OVER(PARTITION BY format(a.MTD, 'yyyy'),format(a.MTD, 'MM'),a.producthierarchy, a.area_name ORDER BY format(a.MTD, 'yyyy') ASC,format(a.MTD, 'MM') ASC,a.producthierarchy ASC, a.area_name ASC, a.billingdocument desc) AS Row#
	from st3_invoiced a 
	left join st3_invoiced_forecasted invd on a.SalesDocument = invd.SalesDocument and a.SalesDocumentItem = invd.SalesDocumentItem
	where invd.SalesDocument is null
	) b
left join 
(
select *, ROW_NUMBER() OVER(PARTITION BY format(c3.DeliveryDate, 'yyyy') ,format(c3.DeliveryDate, 'MM'),c3.producthierarchy, c3.area_name ORDER BY format(c3.DeliveryDate, 'yyyy') ASC,format(c3.DeliveryDate, 'MM') ASC, c3.producthierarchy ASC, c3.area_name ASC) AS Row#
from (
	select c1.*, case when c2.area_name like '%Java' and left(c1.ProductHierarchy,2) in ('M1', 'F1') then 'Java' when c2.area_name like '%Sumatera' and left(c1.ProductHierarchy,2) in ('M1', 'F1') then 'Sumatera' else c2.area_name end as area_name
	from EDW_ANALYTICS.CRM.EC_fact_locked_forecast c1 
	left join EDW_ANALYTICS.CRM.EC_dim_area_store c2 on c1.sales_off_code = c2.sales_code
	left join tba_used c4 on c1.OpportunityID = c4.OpportunityID and c1.OpportunityItemNo = c4.OpportunityItemNo
	where AccountID = 1 and c4.OpportunityID is null
	) as c3
) as c 
on b.ProductHierarchy = c.ProductHierarchy and b.area_name = c.area_name and format(b.MTD, 'yyyy') = format(c.DeliveryDate, 'yyyy') and format(b.MTD, 'MM') = format(c.DeliveryDate, 'MM')and b.Row# = c.Row#

--Mapping Forecasted/Unforecasted Progress
union
select b.*, 
case 
	when c.OpportunityID is not null and c.FORECAST = 'Yes' then 'Yes'
	when c.OpportunityID is not null and c.FORECAST = 'Carried Over' then 'Carried Over'
	else 'No' end as isForecast 
from (
select
	a.*
	,ROW_NUMBER() OVER(PARTITION BY format(MTD, 'yyyy'),format(MTD, 'MM'), accountID, producthierarchy ORDER BY format(MTD, 'yyyy') ASC,format(MTD, 'MM') ASC, accountID ASC, producthierarchy ASC, billingdocument desc) AS Row#
from (
	select * from pp_progress
	UNION
	select * from st3_progress
	) a
) b
left join 
(
select *, ROW_NUMBER() OVER(PARTITION BY format(c3.DeliveryDate, 'yyyy') ,format(c3.DeliveryDate, 'MM'),c3.producthierarchy, c3.area_name ORDER BY format(c3.DeliveryDate, 'yyyy') ASC,format(c3.DeliveryDate, 'MM') ASC, c3.producthierarchy ASC, c3.area_name ASC) AS Row#
from (
	select c1.*, case when c2.area_name like '%Java' and left(c1.ProductHierarchy,2) in ('M1', 'F1') then 'Java' when c2.area_name like '%Sumatera' and left(c1.ProductHierarchy,2) in ('M1', 'F1') then 'Sumatera' else c2.area_name end as area_name
	from EDW_ANALYTICS.CRM.EC_fact_locked_forecast c1 
	left join EDW_ANALYTICS.CRM.EC_dim_area_store c2 on c1.sales_off_code = c2.sales_code
	left join (select * from tba_used union select * from tba_used_st3) c4 on c1.OpportunityID = c4.OpportunityID and c1.OpportunityItemNo = c4.OpportunityItemNo
	where AccountID = 1 and c4.OpportunityID is null
	) as c3
) as c 
on b.ProductHierarchy = c.ProductHierarchy and b.area_name = c.area_name and format(b.MTD, 'yyyy') = format(c.DeliveryDate, 'yyyy') and format(b.MTD, 'MM') = format(c.DeliveryDate, 'MM')and b.Row# = c.Row#


) test
;

EXEC EDW_ANALYTICS.CRM.sp_EC_Update_RateInvoiceActual;
EXEC EDW_ANALYTICS.CRM.sp_EC_Insert_InvoiceProgress;