delete from EDW_ANALYTICS.CRM.EC_fact_invoice where format(MTD,'yyyyMM') = format(CURRENT_TIMESTAMP, 'yyyyMM');
--delete from EDW_ANALYTICS.CRM.EC_fact_invoice where format(MTD,'yyyyMM') = '202111';


--CTE Invoiced PP that mapped as carried over
with invoiced_carriedover as (
select b.*, case when c.OpportunityID is not null then 'Carried Over' else 'No' end as isForecast 
from (

select 
	a.*
	,ROW_NUMBER() OVER(PARTITION BY format(a.MTD, 'yyyy'),format(a.MTD, 'MM'), a.accountID, a.producthierarchy ORDER BY format(a.MTD, 'yyyy') ASC,format(a.MTD, 'MM') ASC, a.accountID ASC, a.producthierarchy ASC, a.billingdocument desc) AS Row#
from EDW_ANALYTICS_STG.CRM.stg_EC_invoicedPP a

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
from EDW_ANALYTICS_STG.CRM.stg_EC_invoicedPP a
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
from EDW_ANALYTICS_STG.CRM.stg_EC_invoicedPP a
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
from EDW_ANALYTICS_STG.CRM.stg_EC_invoicedrental a 

) b
left join 
(select *, 
ROW_NUMBER() OVER(PARTITION BY format(DeliveryDate, 'yyyy') ,format(DeliveryDate, 'MM'),accountID, producthierarchy ORDER BY format(DeliveryDate, 'yyyy') ASC,format(DeliveryDate, 'MM') ASC,accountID ASC, producthierarchy ASC) AS Row#
from EDW_ANALYTICS.CRM.EC_fact_locked_forecast where AccountID <> 1) as c 
on b.AccountID = c.AccountID and b.ProductHierarchy = c.ProductHierarchy and format(b.MTD, 'yyyy') = format(c.DeliveryDate, 'yyyy') and format(b.MTD, 'MM') = format(c.DeliveryDate, 'MM')and b.Row# = c.Row#
where c.OpportunityID is not null
)

--CTE Invoiced PP that mapped as forecasted from Push Sales
,pp_push_sales as (
select b.*, case when c.OpportunityID is not null then 'Yes' else 'No' end as isForecast 
from (
	select 
		a.*
		,ROW_NUMBER() OVER(PARTITION BY format(a.MTD, 'yyyy'),format(a.MTD, 'MM'),a.producthierarchy, a.area_name ORDER BY format(a.MTD, 'yyyy') ASC,format(a.MTD, 'MM') ASC,a.producthierarchy ASC, a.area_name ASC, a.billingdocument desc) AS Row#
	from EDW_ANALYTICS_STG.CRM.stg_EC_invoicedPP a 
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
)

--CTE Invoiced ST3/ST5 that mapped as forecasted from Push Sales
,st3_push_sales as (
select b.*, case when c.OpportunityID is not null then 'Yes' else 'No' end as isForecast 
from (
	select 
		a.*
		,ROW_NUMBER() OVER(PARTITION BY format(a.MTD, 'yyyy'),format(a.MTD, 'MM'),a.producthierarchy, a.area_name ORDER BY format(a.MTD, 'yyyy') ASC,format(a.MTD, 'MM') ASC,a.producthierarchy ASC, a.area_name ASC, a.billingdocument desc) AS Row#
	from EDW_ANALYTICS_STG.CRM.stg_EC_invoicedrental a 
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
	left join (select OpportunityID, OpportunityItemNo from pp_push_sales where isForecast = 'Yes') c4 on c1.OpportunityID = c4.OpportunityID and c1.OpportunityItemNo = c4.OpportunityItemNo
	where AccountID = 1 and c4.OpportunityID is null
	) as c3
) as c 
on b.ProductHierarchy = c.ProductHierarchy and b.area_name = c.area_name and format(b.MTD, 'yyyy') = format(c.DeliveryDate, 'yyyy') and format(b.MTD, 'MM') = format(c.DeliveryDate, 'MM')and b.Row# = c.Row#
)

--CTE Progress that mapped as forecasted from Push Sales
,progress_push_sales as (
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
	select * from EDW_ANALYTICS_STG.CRM.stg_EC_progressPP
	UNION
	select * from EDW_ANALYTICS_STG.CRM.stg_EC_progressrental
	) a
) b
left join 
(
select *, ROW_NUMBER() OVER(PARTITION BY format(c3.DeliveryDate, 'yyyy') ,format(c3.DeliveryDate, 'MM'),c3.producthierarchy, c3.area_name ORDER BY format(c3.DeliveryDate, 'yyyy') ASC,format(c3.DeliveryDate, 'MM') ASC, c3.producthierarchy ASC, c3.area_name ASC) AS Row#
from (
	select c1.*, case when c2.area_name like '%Java' and left(c1.ProductHierarchy,2) in ('M1', 'F1') then 'Java' when c2.area_name like '%Sumatera' and left(c1.ProductHierarchy,2) in ('M1', 'F1') then 'Sumatera' else c2.area_name end as area_name
	from EDW_ANALYTICS.CRM.EC_fact_locked_forecast c1 
	left join EDW_ANALYTICS.CRM.EC_dim_area_store c2 on c1.sales_off_code = c2.sales_code
	left join (select OpportunityID, OpportunityItemNo from pp_push_sales where isForecast = 'Yes' union select OpportunityID, OpportunityItemNo from st3_push_sales where isForecast = 'Yes') c4 on c1.OpportunityID = c4.OpportunityID and c1.OpportunityItemNo = c4.OpportunityItemNo
	where AccountID = 1 and c4.OpportunityID is null
	) as c3
) as c 
on b.ProductHierarchy = c.ProductHierarchy and b.area_name = c.area_name and format(b.MTD, 'yyyy') = format(c.DeliveryDate, 'yyyy') and format(b.MTD, 'MM') = format(c.DeliveryDate, 'MM')and b.Row# = c.Row#
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
select * from pp_push_sales

--ST3 Invoiced
union
select * from st3_invoiced_forecasted

--ST3 Push Sales
union
select * from st3_push_sales

--Mapping Forecasted/Unforecasted Progress
union
select * from progress_push_sales

) test
;

EXEC EDW_ANALYTICS.CRM.sp_EC_Update_RateInvoiceActual;
EXEC EDW_ANALYTICS.CRM.sp_EC_Insert_InvoiceProgress;