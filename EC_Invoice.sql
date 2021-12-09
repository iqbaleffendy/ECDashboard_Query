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
    dc.CUSTOMER_KEY Account_Key,
    dc.CUSTOMER_CODE as AccountID ,
    dc.FULL_NAME as AccountName,
    cic.cic_key,
    cic.CIC_Group CICGroup,
    cic.CIC_Description CICDescription,
    dsl.store_abbrevation_name sales_off_code,
    dsl.AREA_NAME AS SalesOfficeArea,
    dsl.sub_company_name,
    dsl.STORE_ABBREVATION_NAME AS SalesOffice,
    oppt.sales_person_key,
    case when dse.occupation_code = 'SE' then dse.FULL_NAME else dsp.FULL_NAME end as SalesRepsName,
    case when dse.occupation_code = 'SE' then oppt.SALES_EXECUTIVE_KEY else oppt.SALES_PERSON_KEY end as SalesExecutiveName,
    oppt.USER_STATUS_KEY opp_status_key,
    opst.[DESCRIPTION] as OpportunityStatus,
    CONVERT(date, CAST(oppt.OPPORTUNITY_STATUS_DATE_KEY AS varchar)) as OpportunityStatusDate,
    oppt.QUOTATION_STATUS_KEY quot_status_key,
    dqs.[DESCRIPTION] as QuotationStatus,
    oppt.SO_STATUS_KEY so_item_status_key,
    dss.[DESCRIPTION] AS SOItemStatus,
    oppt.So_rejection_key so_reject_key,
    drj.Rejection_Reason as SORejectionReason,
    oppt.OPPORTUNITY_TYPE_KEY opp_type_key,
    optp.OPPORTUNITY_TYPE_DESCRIPTION as OpportunityType,
    oppt.product_material_key,
    dpm.PRODUCT_MATERIAL_CODE ProductID,
    dpm.PRODUCT_MODEL ProductModel,
    dpm.[DESCRIPTION] as PoductDescription,
    dpm.VALID_MATERIAL ValidMaterial,
    sn.Serial_No SerialNo,
    sn.Batch_ID BatchID,
    --pwc,
    oppt.MARKET_SECTOR_KEY PWC_Key,
    oppt.MARKET_SECTOR_KEY PWC,
    dms.[DESCRIPTION] as MarketDescription,
    oppt.MARKET_SECTOR_KEY as market_sector_key,
   -- dms.[DESCRIPTION] as MarketSector,
    case when dms.[INDUSTRY_GROUP] is not null then dms.[INDUSTRY_GROUP] else dms.[INDUSTRY_SEGMENT] end  as MarketSector,
    '' as SubMarketSector,
    oppt.POINT_OF_DELIVERY_KEY delivery_key,
    dpd.[DESCRIPTION] as POINT_OF_DELIVERY_DESC,
    CONVERT(date, CAST(oppt.DELIVERY_DATE_KEY AS varchar)) as deliverydate,
    oppt.NET_VALUE NetValue,
    oppt.EXPECTED_TOTAL_VALUE ExpectedTotalVaue,
    oppt.CURRENCY_KEY opp_currency_key,
    curr.CURRENCY_CODE Currency,
    oppt.QUOT_NET_VALUE QuotNetValue,
    oppt.QUOT_CURRENCY_KEY,
    currq.CURRENCY_CODE as QOUT_CURRENCY_CODE,
    oppt.CONFIDENCE_LEVEL ConfidenceLevel,
    dpm.BASIC_SELLING_PRICE_VALID_FROM BasicSellingPriceValidFrom,
    dpm.BASIC_SELLING_PRICE_VALID_TO BasicSellingPriceValidTo,
    '' product_hie_key,
    dpmm.PRODUCT_HIERARCHY,
    da.Major_Account_Classification MajorAccountClassification,
    da.Customer_Class_Code CustomerClassCode,
    dpm.material_category ProductType, 
    da.INDUSTRY_KEY,
	da.Vertical_Industry VerticalIndustry,
    flag.[DESCRIPTION] as FORECAST,
    'PP' as PP_ST3_ST5,
    oppt.customer_key,
    dc.CUSTOMER_CODE CustomerCode,
    dc.FULL_NAME,
    oppt.net_value,
    oppt.net_value NetValueInUSD,
    oppt.net_value NetValueInIDR,
    dpm.Category_ID,
    ps.sales_type,
    --product hierarky
    CONVERT(date, CAST(oppt.REPORT_DATE_KEY AS varchar)) as LOAD_DATE
from
[LS_BI_PROD].EDW_CRM_ANALYTICS.dbo.FACT_CRM_OPPORTUNITY oppt left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_OPPORTUNITY_TYPE optp on (oppt.OPPORTUNITY_TYPE_KEY = optp.OPPORTUNITY_TYPE_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_USER_STATUS opst on (oppt.USER_STATUS_KEY = opst.USER_STATUS_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.dbo.DIM_FLAG_YN flag on (oppt.FLAG_FORECAST_KEY = flag.FLAG_YN_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_SALES_PERSON dsp on (oppt.SALES_PERSON_KEY = dsp.SALES_PERSON_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_SALES_PERSON dse on (oppt.SALES_EXECUTIVE_KEY = dse.SALES_PERSON_KEY) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_SALES_LOCATION dsl on (dsl.sales_location_code = case when dsp.OCCUPATION_CODE='SE' then dse.sales_location_code else  dsp.sales_location_code end) left join
[LS_BI_PROD].EDW_DIMENSION.CRM.DIM_PRODUCT_MATERIAL dpm on (oppt.PRODUCT_MATERIAL_KEY = dpm.PRODUCT_MATERIAL_KEY) left join
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
[LS_BI_PROD].EDW_ANALYTICS.ECC.dim_account da on (case when dc.CUSTOMER_CODE < 0 then dc.CUSTOMER_CODE else RIGHT('00000' + CAST(dc.CUSTOMER_CODE as varchar(10)), 10) end = da.account_id)
left join [LS_BI_PROD].EDW_ANALYTICS.crm.dim_price_scenario ps on oppt.price_scenario = ps.price_scenario_key
left join (select * from [LS_BI_PROD].EDW_ANALYTICS.CRM.dim_opp_product_material where product_material_code not in ('N/A', 'Unknown') and left(product_hierarchy, 2) in ('M1','E1','F1')) dpmm on CAST(dpm.PRODUCT_MATERIAL_CODE AS INT) = CAST (dpmm.PRODUCT_MATERIAL_CODE AS INT)
where
            optp.OPPORTUNITY_TYPE_DESCRIPTION not in ('N/A', 'Unknown', 'LEAD')
            --tambahkan bulan
            and format(CONVERT(date, CAST(oppt.DELIVERY_DATE_KEY AS varchar)),'yyyy')=format(CURRENT_TIMESTAMP,'yyyy')
            and dpm.Category_ID in ('M1','E1','F1')
            and ( opst.[DESCRIPTION] like '%stage 3%' or
            opst.[DESCRIPTION] like '%stage 4%' or
            opst.[DESCRIPTION] like '%stage 5%' or
            opst.[DESCRIPTION] like '%stage 6%' or
            opst.[DESCRIPTION] like '%won%' or
            opst.[DESCRIPTION] like '%delivered%' or
            opst.[DESCRIPTION] NOT in('N/A','NO DEAL (CANCEL)','LOST') )
            
),invoice as (
select 
		cast(VBELN_VBAP as int) SalesDocument, 
		POSNR_VBAP SalesDocumentItem,
		AUART SalesDocumentType,
		uepos HigerLevelItem,
		BSTKD_E SpaNo,
		a.BSTKD PurchaseOrderNo,
		BSTDK PurchaseOrderDate,
		a.VKORG SalesOrganization,
		a.SPART Division,
		a.AREA Area,
		a.VKBUR SalesOffice,
		a.KUNNR_VBAK SoldToParty,
		a.Gtext EndDestination,
		a.Name1_bilpar BilltoPartyname,
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
		a.Payment TermsofPayment,
		a.PAYMENTNOTE PaymentTermsNote,
		a.VTEXT Description,
		a.NETWR Price,
		a.MWSBP Tax ,
		a.WAERK PDC_PDG_curr,
		a.SALID SalesmanID,
		a.SALNAME SalesmanName,
		a.DSALID DeliverySalesmanID,
		a.DSALNAME DeliverySalesmanName,
		a.MKTPGM Marketingprogram,
		a.VCHR Voucher,
		a.CCR CCRNo,
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
		a.NOTEBR NotesforOverdueBranch,
		a.NOTEGRP NotesforOverdueGroup,
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
		a.ZREJECT_RSN ReasonforRejection,
		a.PDC_PDG PDC_PDG,
		a.Amount Amount ,
		a.DUE_DATE DueDate,
		a.VTWEG DistributionChannel,
		a.Region Region,
		a.NAME1_SOl SoldToPartyName ,
		a.GSBER BusinessArea,
		a.TELF1 CustomerPhoneNo,
		a.Name_ser ServiceContact,
		a.NAME_CON SalesContact,
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
		where format(a.DELDATE,'yyyy') between (format(CURRENT_TIMESTAMP,'yyyy') - 1) and (format(CURRENT_TIMESTAMP,'yyyy') + 1)
),
logscore as (
select 
	b.sernr,
	b.CUST_NAME,
	a.vbrk_fkdat, 
	b.billing_date, 
	ScoreLog.SCORE_DATE,
	case when ScoreLog.SCORE_DATE < DATEFROMPARTS(YEAR(b.BILLING_DATE), MONTH(b.BILLING_DATE)+1, 1) and format(b.billing_date, 'yyyy-MM') <> format(ScoreLog.SCORE_DATE, 'yyyy-MM') and ScoreLog.CUST_NAME_OR_RENT_CUST_NAME = b.CUST_NAME then 0
		when ScoreLog.SCORE_DATE  < DATEFROMPARTS(YEAR(b.BILLING_DATE), MONTH(b.BILLING_DATE)+1, 1) and format(b.billing_date, 'yyyy-MM') <> format(ScoreLog.SCORE_DATE, 'yyyy-MM') and ScoreLog.CUST_NAME_OR_RENT_CUST_NAME <> b.CUST_NAME then 1
	else 1 end hitung

from [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_V_ODRPP_BW a
left join [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_F_INV_SOLD b on a.VBELN_VBAP = b.VBELN and a.POSNR_VBAP =b.POSNR 
left join
(select SCORE_DATE,PRIME_PRODUCT_SERIAL_NUMBER, CUSTOMER_CODE, CUST_NAME_OR_RENT_CUST_NAME, SCORE_ERROR, 'M1' AS MATERIAL_TYPE, SALE_TYPE from [LS_BI_PROD].EDW_STG_SAP_CRM_DAILY.dbo.zldb_score_mach union
select SCORE_DATE,PRIME_PRODUCT_SERIAL_NUMBER, CUSTOMER_CODE, CUST_NAME_RENT_CUST_NAME, SCORE_ERROR, 'E1' AS MATERIAL_TYPE, SALE_TYPE  from [LS_BI_PROD].EDW_STG_SAP_CRM_DAILY.dbo.zldb_score_engn where CUSTOMER_CODE is not null
) as ScoreLog
on b.SERNR = ScoreLog.PRIME_PRODUCT_SERIAL_NUMBER
where 1=1 
--left(b.MFRPN, 2) in ('M1')
-- and FORMAT(b.billing_date, 'yyyy-MM') = '2021-08'  
and FORMAT (b.billing_date, 'yyyy') >= '2021'  
and ScoreLog.SALE_TYPE  in (1,3,5) 
--and ScoreLog.SCORE_ERROR = 'N'
)
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
left join EDW_ANALYTICS.dbo.dim_area_store c 
on b.vkbur = c.sales_code
left join (select * from [LS_BI_PROD].EDW_ANALYTICS.CRM.dim_opp_product_material where product_material_code not in ('N/A', 'Unknown') and left(product_hierarchy, 2) in ('M1','E1','F1')) dpm
on CAST(b.MATNR AS INT) = CAST (dpm.PRODUCT_MATERIAL_CODE AS INT)
where a.SCORE_ERROR = 'N' and a.SALE_TYPE in (3,5) and b.release_apprv = 'X'
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
left join EDW_ANALYTICS.dbo.dim_area_store c 
on b.vkbur = c.sales_code
left join (select * from [LS_BI_PROD].EDW_ANALYTICS.CRM.dim_opp_product_material where product_material_code not in ('N/A', 'Unknown') and left(product_hierarchy, 2) in ('M1','E1','F1')) dpm
on CAST(b.MATNR AS INT) = CAST (dpm.PRODUCT_MATERIAL_CODE AS INT)
where a.SCORE_ERROR = 'N' and a.SALE_TYPE in (3,5) and b.release_apprv = 'X'
)

select b.*, case when c.OpportunityID is not null then 'Yes' else 'No' end as isForecast 
--into EDW_ANALYTICS.dbo.invoiceDataNew
from (

select 
	distinct a.*,
	case when das.area_name like '%MA' Then 'Major Account' else 'Retail Account' end Customer_Type,
	case 
		when das.area_name like '%MA' Then CICGroups 
	else 
		case 
			when (market_sector is null or market_sector='') then CICGroups 
		else market_sector end 
	end MarketSector
	,0 rate
	,ROW_NUMBER() OVER(PARTITION BY format(MTD, 'yyyy'),format(MTD, 'MM'), accountID, producthierarchy ORDER BY format(MTD, 'yyyy') ASC,format(MTD, 'MM') ASC, accountID ASC, producthierarchy ASC, billingdocument desc) AS Row#
from (
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
					end
				else  d.sales_code
			end
		when i.SalesOrganization = '1Z02' then i.BusinessArea 
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
	i.PRODUCT_HIERARCHY ProductHierarchy
	--,CONCAT(CAST(i.SoldToParty as INT),i.ProductHierarchy, year(i.BillingDate), month(i.BillingDate)) AS forecastedkey

from invoice i 
left join forecaseDaily f on i.salesDocument = f.SOID and i.salesdocumentItem = f.SOItemNo  
--left join EDW_ANALYTICS.dbo.forecast c on  c.OpportunityID = f.OpportunityID and c.OpportunityItemNo = f.OpportunityItemNo
left join EDW_ANALYTICS.dbo.dim_area_sales d on i.SalesmanID  = d.sales_id
--left join EDW_ANALYTICS.dbo.dim_area_store das on d.sales_code = das.sales_code
left join EDW_ANALYTICS.dbo.dim_company_map dcm on dcm.CompanyName = i.Payername
left join logscore ls on ls.sernr = i.serialnumber and ls.billing_date = i.billingdate

where 1=1
--and opportunityType in ('Opp  Machine','Opp  Engine','Opp  ForkLift')
--and format(i.BillingDate,'yyyyMM') = '202108'
and format(i.BillingDate,'yyyy') =format(CURRENT_TIMESTAMP,'yyyy')
and left(MaterialNumber, 2) in ('M1','E1','F1')
and (ls.hitung=1 or left(MaterialNumber, 2) in ('E1','F1'))
--and BillingDocument is not null
--and i.serialNumber ='YJW10239'
--and pbill.qty_inv <=1
--and pbill.
--and confidence_level>=75
--and forecast ='YES'
--and (f.so_id is null or f.so_id=0)

union

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
				else f.SalesOffice
			end
		else f.SalesOffice
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
	f.PRODUCT_HIERARCHY ProductHierarchy
	--,CONCAT(f.AccountID,i.ProductHierarchy, year(f.deliverydate), month(f.deliverydate)) AS forecastedkey

from forecaseDaily f 
left join invoice i on i.salesDocument = f.SOID and i.salesdocumentItem = f.SOItemNo 
--left join EDW_ANALYTICS.dbo.forecast c on c.OpportunityID = f.OpportunityID and c.OpportunityItemNo = f.OpportunityItemNo 
left join EDW_ANALYTICS.dbo.dim_area_store das on das.sales_code = f.SalesOffice
left join EDW_ANALYTICS.dbo.dim_company_map dcm on dcm.CompanyName = f.accountname

where 1=1
and f.opportunityType in ('Opp  Machine','Opp  Engine','Opp  ForkLift')
--and format(f.deliverydate,'yyyy') = '2021'
and f.confidencelevel>=75
and f.Category_ID in ('M1','E1','F1')
and f.forecast ='YES'
and (i.salesdocument is null or i.billingDocument is null)

--start modification
UNION

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
	,0 AS HigerLevelItem
	,'-' AS SpaNo
	,'-' AS PurchaseOrderNo
	,NULL AS PurchaseOrderDate
	,NULL AS SalesOrganization
	,NULL AS Division
	,NULL AS Area
	,NULL AS SalesOffice
	,NULL AS SoldToParty
	,NULL AS EndDestination
	,NULL AS BilltoPartyname
	,NULL AS Payer
	,st3.SOLD_NM AS Payername
	,NULL AS FinancingCompany
	,NULL AS FinancingCompanyName
	,NULL AS Model
	,NULL AS Batch
	,NULL AS serialNumber
	,NULL AS MaterialNumber
	,NULL AS MaterialDescription
	,NULL AS Source
	,NULL AS Incoterms
	,NULL AS IncotermsPart2
	,NULL AS PWCCode
	,NULL AS ApplicationCode
	,NULL AS CICCode
	,NULL AS RequestDeliveryDate
	,NULL AS TermsofPayment
	,NULL AS PaymentTermsNote
	,NULL AS Description
	,NULL AS Price
	,NULL AS Tax
	,NULL AS PDC_PDG_curr
	,NULL AS SalesmanID
	,NULL AS SalesmanName
	,NULL AS DeliverySalesmanID
	,NULL AS DeliverySalesmanName
	,NULL AS Marketingprogram
	,NULL AS Voucher
	,NULL AS CCRNo
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
	,NULL AS NotesforOverdueBranch
	,NULL AS NotesforOverdueGroup
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
	,CASE
		WHEN st3.BAST_NO IS NOT NULL AND st3.BAST_NO <> '' THEN 1234
		ELSE NULL
	END AS BillingDocument
	,NULL AS BillingDate
	,NULL AS ReasonforRejection
	,NULL AS PDC_PDG
	,NULL AS Amount
	,NULL AS DueDate
	,NULL AS DistributionChannel
	,NULL AS Region
	,NULL AS SoldToPartyName
	,NULL AS BusinessArea
	,NULL AS CustomerPhoneNo
	,NULL AS ServiceContact
	,NULL AS SalesContact
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
	,f.AccountID
	,f.AccountName
	,st3.PRODUCT_HIERARCHY ProductHierarchy
	--,CONCAT(f.AccountID,st3.ProductHierarchy, year(st3.SCORE_DATE), month(st3.SCORE_DATE)) AS forecastedkey

FROM st3
LEFT JOIN forecaseDaily AS f ON st3.VBELN_VA = f.SOID AND st3.POSNR_VA = f.SOItemNo
--LEFT JOIN EDW_ANALYTICS.dbo.forecast AS c ON c.OpportunityID = f.OpportunityID AND c.OpportunityItemNo = f.OpportunityItemNo
LEFT JOIN EDW_ANALYTICS.dbo.dim_area_store das ON das.sales_code = f.SalesOffice

WHERE 1=1
--AND f.opportunityType IN ('Opp  Rental')
--AND f.Category_ID in ('M1','E1','F1')
--and format(st3.SCORE_DATE,'yyyyMM') =format(CURRENT_TIMESTAMP,'yyyyMM')

AND st3.BAST_NO IS NOT NULL AND st3.BAST_NO <> ''

UNION

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
	,0 AS HigerLevelItem
	,'-' AS SpaNo
	,'-' AS PurchaseOrderNo
	,NULL AS PurchaseOrderDate
	,NULL AS SalesOrganization
	,NULL AS Division
	,NULL AS Area
	,NULL AS SalesOffice
	,NULL AS SoldToParty
	,NULL AS EndDestination
	,NULL AS BilltoPartyname
	,NULL AS Payer
	,st3.SOLD_NM AS Payername
	,NULL AS FinancingCompany
	,NULL AS FinancingCompanyName
	,NULL AS Model
	,NULL AS Batch
	,NULL AS serialNumber
	,NULL AS MaterialNumber
	,NULL AS MaterialDescription
	,NULL AS Source
	,NULL AS Incoterms
	,NULL AS IncotermsPart2
	,NULL AS PWCCode
	,NULL AS ApplicationCode
	,NULL AS CICCode
	,NULL AS RequestDeliveryDate
	,NULL AS TermsofPayment
	,NULL AS PaymentTermsNote
	,NULL AS Description
	,NULL AS Price
	,NULL AS Tax
	,NULL AS PDC_PDG_curr
	,NULL AS SalesmanID
	,NULL AS SalesmanName
	,NULL AS DeliverySalesmanID
	,NULL AS DeliverySalesmanName
	,NULL AS Marketingprogram
	,NULL AS Voucher
	,NULL AS CCRNo
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
	,NULL AS NotesforOverdueBranch
	,NULL AS NotesforOverdueGroup
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
	,CASE
		WHEN st3.BAST_NO IS NOT NULL AND st3.BAST_NO <> '' THEN 1234
		ELSE NULL
	END AS BillingDocument
	,NULL AS BillingDate
	,NULL AS ReasonforRejection
	,NULL AS PDC_PDG
	,NULL AS Amount
	,NULL AS DueDate
	,NULL AS DistributionChannel
	,NULL AS Region
	,NULL AS SoldToPartyName
	,NULL AS BusinessArea
	,NULL AS CustomerPhoneNo
	,NULL AS ServiceContact
	,NULL AS SalesContact
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
	--,CONCAT(f.AccountID,st3.ProductHierarchy, year(f.deliverydate), month(f.deliverydate)) AS forecastedkey

FROM forecaseDaily AS f
LEFT JOIN st3 ON st3.VBELN_VA = f.SOID AND st3.POSNR_VA = f.SOItemNo
--LEFT JOIN EDW_ANALYTICS.dbo.forecast AS c ON c.OpportunityID = f.OpportunityID AND c.OpportunityItemNo = f.OpportunityItemNo
LEFT JOIN EDW_ANALYTICS.dbo.dim_area_store das ON das.sales_code = f.SalesOffice

WHERE 1=1
AND f.opportunityType IN ('Opp  Rental')
AND f.sales_type in ('ST3','ST5')
AND f.confidencelevel >= 75
AND f.Category_ID in ('M1','E1','F1')
AND f.forecast ='YES'
and format(st3.SCORE_DATE,'yyyyMM') =format(CURRENT_TIMESTAMP,'yyyyMM')
AND (st3.BAST_NO IS NULL OR st3.BAST_NO = '')
--end modification

--locked forecast join invoice
UNION

select
	i.*,
	c.deliverydate as MTD,
	c.deliverydate as DeliveryDatef,
	case when c.opportunityType in ('Opp  Machine','Opp  Engine','Opp  ForkLift') then 'PP' 
	else c.sales_type end sales_type,
	c.Forecast,
	c.ConfidenceLevel,
	c.SOID,
	c.SOItemNo,
	c.OpportunityStatus,
	c.customer_key,
	c.CustomerClassCode,
	c.OpportunityID, 
	c.OpportunityItemNo,
	c.product_material_key,
	c.ProductID,
	c.ProductModel,
	c.INDUSTRY_KEY,
	c.VerticalIndustry,
	0 NetValueInIDR,
	c.NetValueInUSD,
	c.SerialNo,
	c.market_sector_key,
	c.sales_off_code,
	c.sales_off_code Business_Area_Key,
	CASE 
		WHEN c.Category_ID='M1' THEN 'MACHINE'
		when c.Category_ID='E1' THEN 'ENGINE'
		WHEN c.Category_ID='F1' THEN 'FORK_LIFT'
	else c.Category_ID end MaterialType,
	--'Yes' isForecast,
	c.CICGroup CICGroups,
	c.AccountID,
	c.AccountName,
	c.ProductHierarchy
	--,CONCAT(c.AccountID,c.ProductHierarchy, year(c.deliverydate), month(c.deliverydate)) AS forecastedkey

from EDW_ANALYTICS.dbo.forecast c 
left join invoice i on i.salesDocument = c.SOID and i.salesdocumentItem = c.SOItemNo 
left join EDW_ANALYTICS.dbo.dim_area_store das on das.sales_code = c.SalesOffice

where 1=1
and c.opportunityType in ('Opp  Machine','Opp  Engine','Opp  ForkLift')
and format(c.deliverydate,'yyyy') = '2021'
and c.confidencelevel>=75
and c.Category_ID in ('M1','E1','F1')
and c.forecast ='YES'
and (i.salesdocument is null or i.billingDocument is null)
and c.OpportunityID not in
	(
		--subquery previous union (1,2,3,4)
		select OpportunityID
		from
		(
		select f.OpportunityID
		from invoice i 
		left join forecaseDaily f on i.salesDocument = f.SOID and i.salesdocumentItem = f.SOItemNo  
		--left join EDW_ANALYTICS.dbo.forecast c on c.OpportunityID = f.OpportunityID and c.OpportunityItemNo = f.OpportunityItemNo
		left join EDW_ANALYTICS.dbo.dim_area_sales d on i.SalesmanID  = d.sales_id
		--left join EDW_ANALYTICS.dbo.dim_area_store das on d.sales_code = das.sales_code
		left join EDW_ANALYTICS.dbo.dim_company_map dcm on dcm.CompanyName = i.Payername
		left join logscore ls on ls.sernr = i.serialnumber and ls.billing_date = i.billingdate

		where 1=1
		--and opportunityType in ('Opp  Machine','Opp  Engine','Opp  ForkLift')
		--and format(i.BillingDate,'yyyyMM') = '202108'
		and format(i.BillingDate,'yyyy') =format(CURRENT_TIMESTAMP,'yyyy')
		and left(MaterialNumber, 2) in ('M1','E1','F1')
		and (ls.hitung=1 or left(MaterialNumber, 2) in ('E1','F1'))
		--and BillingDocument is not null
		--and i.serialNumber ='YJW10239'
		--and pbill.qty_inv <=1
		--and pbill.
		--and confidence_level>=75
		--and forecast ='YES'
		--and (f.so_id is null or f.so_id=0)
		union
		select f.OpportunityID
		from forecaseDaily f 
		left join invoice i on i.salesDocument =f.SOID and i.salesdocumentItem = f.SOItemNo 
		--left join EDW_ANALYTICS.dbo.forecast c on c.OpportunityID = f.OpportunityID and c.OpportunityItemNo = f.OpportunityItemNo 
		left join EDW_ANALYTICS.dbo.dim_area_store das on das.sales_code = f.SalesOffice
		left join EDW_ANALYTICS.dbo.dim_company_map dcm on dcm.CompanyName = f.accountname

		where 1=1
		and f.opportunityType in ('Opp  Machine','Opp  Engine','Opp  ForkLift')
		--and format(f.deliverydate,'yyyy') = '2021'
		and f.confidencelevel>=75
		and f.Category_ID in ('M1','E1','F1')
		and f.forecast ='YES'
		and (i.salesdocument is null or i.billingDocument is null)
		--start modification
		UNION
		SELECT f.OpportunityID
		FROM st3
		LEFT JOIN forecaseDaily AS f ON st3.VBELN_VA = f.SOID AND st3.POSNR_VA = f.SOItemNo
		--LEFT JOIN EDW_ANALYTICS.dbo.forecast AS c ON c.OpportunityID = f.OpportunityID AND c.OpportunityItemNo = f.OpportunityItemNo
		LEFT JOIN EDW_ANALYTICS.dbo.dim_area_store das ON das.sales_code = f.SalesOffice

		WHERE 1=1
		--AND f.opportunityType IN ('Opp  Rental')
		--AND f.Category_ID in ('M1','E1','F1')
		--and format(st3.SCORE_DATE,'yyyyMM') =format(CURRENT_TIMESTAMP,'yyyyMM')

		AND st3.BAST_NO IS NOT NULL AND st3.BAST_NO <> ''
		UNION
		SELECT f.OpportunityID
		FROM forecaseDaily AS f
		LEFT JOIN st3 ON st3.VBELN_VA = f.SOID AND st3.POSNR_VA = f.SOItemNo
		--LEFT JOIN EDW_ANALYTICS.dbo.forecast AS c ON c.OpportunityID = f.OpportunityID AND c.OpportunityItemNo = f.OpportunityItemNo
		LEFT JOIN EDW_ANALYTICS.dbo.dim_area_store das ON das.sales_code = f.SalesOffice
		WHERE 1=1
		AND f.opportunityType IN ('Opp  Rental')
		AND f.sales_type in ('ST3','ST5')
		AND f.confidencelevel >= 75
		AND f.Category_ID in ('M1','E1','F1')
		AND f.forecast ='YES'
		AND (st3.BAST_NO IS NULL OR st3.BAST_NO = '')
		) as filtertabletemporary
	)
) a 
left join EDW_ANALYTICS.dbo.dim_area_store das on das.sales_code = a.sales_off_code
) b
left join 
(select *, 
ROW_NUMBER() OVER(PARTITION BY format(DeliveryDate, 'yyyy') ,format(DeliveryDate, 'MM'),accountID, producthierarchy ORDER BY format(DeliveryDate, 'yyyy') ASC,format(DeliveryDate, 'MM') ASC,accountID ASC, producthierarchy ASC) AS Row#
from EDW_ANALYTICS.dbo.forecast) as c 
on b.AccountID = c.AccountID and b.ProductHierarchy = c.ProductHierarchy and format(b.MTD, 'yyyy') = format(c.DeliveryDate, 'yyyy') and format(b.MTD, 'MM') = format(c.DeliveryDate, 'MM')and b.Row# = c.Row#;