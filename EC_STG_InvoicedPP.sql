truncate table EDW_ANALYTICS_STG.CRM.stg_EC_invoicedPP;

--Insert Invoiced PP
insert into EDW_ANALYTICS_STG.CRM.stg_EC_invoicedPP
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
	,0 rate,
	GETDATE() AS ETL_DATE
from (
select distinct * from (
select 
	i.SalesDocument,
	i.SalesDocumentItem,
	i.SalesDocumentType,
	i.SpaNo,
	i.PurchaseOrderNo,
	i.PurchaseOrderDate,
	i.SalesOrganization,
	i.Area,
	i.SalesOffice,
	i.SoldToParty,
	i.EndDestination,
	i.Payer,
	i.Payername,
	i.FinancingCompany,
	i.FinancingCompanyName,
	i.Model,
	i.Batch,
	i.serialNumber,
	i.MaterialNumber,
	i.MaterialDescription,
	i.Source,
	i.Incoterms,
	i.IncotermsPart2,
	i.PWCCode,
	i.ApplicationCode,
	i.CICGroup,
	i.RequestDeliveryDate,
	i.Description,
	i.Price,
	i.Tax,
	i.SalesmanID,
	i.SalesmanName,
	i.ReleaseApprovalNo,
	i.ReleaseApprovalDate,
	i.ReleaseApprovalBy,
	i.Created,
	i.RequestChangeStatustoWorkable,
	i.Workable,
	i.RequestToDoOverdueCheck,
	i.RequestReleaseApprovalNo,
	i.ReleaseApproval,
	i.FullyPaid,
	i.POLeasing,
	i.Remarks,
	i.NPWP,
	i.Delivery,
	i.DeliveryDate,
	i.BAST_NO,
	i.StatusPGI,
	i.PGIdate,
	i.BASTSigndate,
	i.ProofOfdeliveryStatus,
	i.ProofOfdeliverydate,
	i.BillingDocument,
	i.BillingDate,
	i.Amount,
	i.DueDate,
	i.SoldToPartyName,
	i.BusinessArea,
	i.BillToParty,
	i.Quantity,
	i.SOCurr,
	i.LeasingFundingDate,
	i.SPPKADate,
	i.ActualNetRevenue,
	i.SODeliveryDate,
	i.typeDivision,
	i.market_sector,
	i.ActualNetInIDR,
	i.ActualNetInUSD,
	i.CustomerClassCodes,
	i.ProductCode,
	i.PRODUCT_HIERARCHY,
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

from EDW_ANALYTICS_STG.CRM.stg_EC_ordertrackingPP i 
left join EDW_ANALYTICS_STG.CRM.stg_EC_forecastdaily f on i.salesDocument = f.SOID and i.salesdocumentItem = f.SOItemNo  
--left join EDW_ANALYTICS.dbo.forecast c on  c.OpportunityID = f.OpportunityID and c.OpportunityItemNo = f.OpportunityItemNo
left join EDW_ANALYTICS.CRM.EC_dim_area_sales d on i.SalesmanID  = d.sales_id
--left join EDW_ANALYTICS.CRM.EC_dim_area_store das on d.sales_code = das.sales_code
left join EDW_ANALYTICS.CRM.EC_dim_company_exception_map dcm on dcm.CompanyID = CAST(i.SoldToParty as INT)
left join EDW_ANALYTICS_STG.CRM.stg_EC_scorelog ls on ls.sernr = i.serialnumber and ls.billing_date = i.billingdate
left join EDW_ANALYTICS.CRM.EC_dim_customer_group_mapping da on i.SoldToParty = da.Account_ID

where 1=1
--and opportunityType in ('Opp  Machine','Opp  Engine','Opp  ForkLift')
and format(i.BillingDate,'yyyyMM') = (case when day(current_timestamp) = 1 then format(dateadd(month, -1, current_timestamp), 'yyyyMM') else format(CURRENT_TIMESTAMP, 'yyyyMM') end)
--and format(i.BillingDate,'yyyyMM') = '202111'
--and format(i.BillingDate,'yyyy') = format(CURRENT_TIMESTAMP,'yyyy')
and left(MaterialNumber, 2) in ('M1','E1','F1')
and (ls.hitung = 1 or left(MaterialNumber, 2) = 'F1' or i.SalesDocumentType <> 'ZEPP' or ls.SCORE_DATE is null)
--and BillingDocument is not null
--and confidence_level>=75
--and forecast ='YES'
) abc
) a 
left join EDW_ANALYTICS.CRM.EC_dim_area_store das on das.sales_code = a.sales_off_code;