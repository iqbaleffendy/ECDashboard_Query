truncate table EDW_ANALYTICS_STG.CRM.stg_EC_progressrental;

--Insert ST3/ST5 Progress Data
insert into EDW_ANALYTICS_STG.CRM.stg_EC_progressrental
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

FROM EDW_ANALYTICS_STG.CRM.stg_EC_forecastdaily AS f
LEFT JOIN EDW_ANALYTICS_STG.CRM.stg_EC_ordertrackingrental st3 ON st3.VBELN_VA = f.SOID AND st3.POSNR_VA = f.SOItemNo
--LEFT JOIN EDW_ANALYTICS.dbo.forecast AS c ON c.OpportunityID = f.OpportunityID AND c.OpportunityItemNo = f.OpportunityItemNo
LEFT JOIN EDW_ANALYTICS.CRM.EC_dim_area_store das ON das.sales_code = f.SalesOffice
left join EDW_ANALYTICS.CRM.EC_dim_customer_group_mapping da on concat('000',cast(f.AccountID as nvarchar(10))) = da.Account_ID


WHERE 1=1
AND f.opportunityType IN ('Opp  Rental')
AND f.sales_type in ('ST3','ST5')
AND f.confidencelevel >= 75
and left(f.product_hierarchy,2) in ('M1','E1','F1')
AND f.forecast ='YES'
and format(f.deliverydate,'yyyyMM') = (case when day(current_timestamp) = 1 then format(dateadd(month, -1, current_timestamp), 'yyyyMM') else format(CURRENT_TIMESTAMP, 'yyyyMM') end)
--and format(f.deliverydate,'yyyyMM') ='202111'
--and format(f.deliverydate,'yyyy') = format(CURRENT_TIMESTAMP, 'yyyy')
AND (st3.BAST_NO IS NULL OR st3.BAST_NO = '')
) a 
left join EDW_ANALYTICS.CRM.EC_dim_area_store das on das.sales_code = a.sales_off_code;