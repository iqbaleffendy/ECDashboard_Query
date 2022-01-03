truncate table EDW_ANALYTICS_STG.CRM.stg_EC_progressPP;

--Insert Progress Data PP
insert into EDW_ANALYTICS_STG.CRM.stg_EC_progressPP
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

from EDW_ANALYTICS_STG.CRM.stg_EC_forecastdaily f 
left join EDW_ANALYTICS_STG.CRM.stg_EC_ordertrackingPP i on i.salesDocument = f.SOID and i.salesdocumentItem = f.SOItemNo 
--left join EDW_ANALYTICS.dbo.forecast c on c.OpportunityID = f.OpportunityID and c.OpportunityItemNo = f.OpportunityItemNo 
left join EDW_ANALYTICS.CRM.EC_dim_area_store das on das.sales_code = f.SalesOffice
left join EDW_ANALYTICS.CRM.EC_dim_company_exception_map dcm on dcm.CompanyID = f.AccountID
left join EDW_ANALYTICS.CRM.EC_dim_customer_group_mapping da on concat('000',cast(f.AccountID as nvarchar(10))) = da.Account_ID

where 1=1
and f.opportunityType in ('Opp  Machine','Opp  Engine','Opp  ForkLift')
and format(f.deliverydate,'yyyyMM') = (case when day(current_timestamp) = 1 then format(dateadd(month, -1, current_timestamp), 'yyyyMM') else format(CURRENT_TIMESTAMP, 'yyyyMM') end)
--and format(f.deliverydate,'yyyyMM') = '202111'
--and format(f.deliverydate,'yyyy') = format(CURRENT_TIMESTAMP, 'yyyy')
and f.confidencelevel>=75
and left(f.product_hierarchy,2) in ('M1','E1','F1')
and f.forecast ='YES'
and (i.salesdocument is null or i.billingDocument is null)
) a 
left join EDW_ANALYTICS.CRM.EC_dim_area_store das on das.sales_code = a.sales_off_code;