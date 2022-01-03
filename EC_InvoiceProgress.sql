DELETE FROM EDW_ANALYTICS.CRM.EC_fact_invoice_progress where MonthDeliveryDate = (case when day(current_timestamp) = 1 then format(dateadd(month, -1, current_timestamp), 'yyyyMM') else format(CURRENT_TIMESTAMP, 'yyyyMM') end);

INSERT INTO EDW_ANALYTICS.CRM.EC_fact_invoice_progress
SELECT
	CatID,
	MonthDeliveryDate,
	area_name_,
	sales_type,
	Customer_Type, 
	ProductModel, 
	MaterialType,
	INDUSTRY_KEY,
	MappingID,
	CatGroup,
	Comparison As RunningTotals,
	null_Forecasted AS Forecasted,
	null_Unforecasted AS Unforcasted,
	null_Carried_Over AS Carried_Over,
	GETDATE() AS ETL_DATE

FROM
(
--comparison_step
SELECT
	*,
	CASE
		WHEN CatID = 9 THEN 0
		ELSE Running_Total
	END AS Comparison
FROM
(
--cumsum_step
SELECT
	*,
	SUM(Lag_Column_Change_Null) OVER (PARTITION BY MonthDeliveryDate, area_name_, sales_type, Customer_Type, ProductModel, MaterialType, INDUSTRY_KEY, MappingID ORDER BY CatID) AS Running_Total
FROM
(
--change_null_lag
SELECT
	*,
	ISNULL(Lag_Column,0) AS Lag_Column_Change_Null
FROM
(
--lag_step
SELECT 
	*,
	LAG(Total,1) OVER (PARTITION BY MonthDeliveryDate, area_name_, sales_type, Customer_Type, ProductModel, MaterialType, INDUSTRY_KEY, MappingID ORDER BY CatID) AS Lag_Column
FROM
(
--total_step
SELECT
	*,
	null_Forecasted + null_Unforecasted + null_Carried_Over AS Total
FROM
(
--change_null_step
SELECT
	*,
	ISNULL(Forecasted,0) AS null_Forecasted,
	ISNULL(Unforecasted,0) AS null_Unforecasted,
	ISNULL(Carried_Over,0) AS null_Carried_Over
FROM
(
--Start of table_before_calculations
SELECT
	table_mapping.CatID,
	table_mapping.MonthDeliveryDate,
	table_mapping.CatGroup,
	table_mapping.area_name_,
	table_mapping.sales_type,
	table_mapping.Customer_Type,
	table_mapping.ProductModel,
	table_mapping.MaterialType,
	table_mapping.INDUSTRY_KEY,
	table_mapping.MappingID,
	table_union1.Forecasted,
	table_union1.Unforecasted,
	table_union1.Carried_Over
FROM
(
--Start of table_mapping
SELECT
	*,
	CONCAT(CatID, MonthDeliveryDate, area_name_, sales_type, Customer_Type, ProductModel, MaterialType, INDUSTRY_KEY, MappingID) AS joinkey
FROM
(
SELECT 
	u2.CatID,
	u2.CatGroup,
	u3.MonthDeliveryDate,
	u3.area_name_,
	u3.sales_type,
	u3.Customer_Type,
	u3.ProductModel,
	u3.MaterialType,
	u3.INDUSTRY_KEY,
	u3.MappingID
FROM 
(
SELECT 
	CatID, 
	CatGroup 
FROM EDW_ANALYTICS.CRM.EC_dim_invoice_progress_category
) AS u2
CROSS JOIN (
	SELECT 
		FORMAT(MTD, 'yyyyMM') AS MonthDeliveryDate,  
		CASE 
			WHEN (area_name LIKE '%Java' and MaterialType IN ('MACHINE', 'FORK_LIFT')) THEN 'Java'
			WHEN (area_name LIKE '%Sumatera' and MaterialType IN ('MACHINE', 'FORK_LIFT')) THEN 'Sumatera' 
			WHEN area_name = 'Trakindo Utama' then 'TUS' ELSE area_name
		END AS area_name_,
		sales_type,
		Customer_Type,
		ProductModel,
		MaterialType,
		INDUSTRY_KEY,
		CONCAT(MarketSector, '-', MaterialType) AS MappingID 
	FROM EDW_ANALYTICS.CRM.EC_fact_invoice
	WHERE FORMAT(MTD, 'yyyyMM') = (case when day(current_timestamp) = 1 then format(dateadd(month, -1, current_timestamp), 'yyyyMM') else format(CURRENT_TIMESTAMP, 'yyyyMM') end)
	AND CASE WHEN (area_name LIKE '%Java' and MaterialType IN ('MACHINE', 'FORK_LIFT')) THEN 'Java' WHEN (area_name LIKE '%Sumatera' and MaterialType IN ('MACHINE', 'FORK_LIFT')) THEN 'Sumatera' WHEN area_name = 'Trakindo Utama' then 'TUS' ELSE area_name END IS NOT NULL
	GROUP BY 
		FORMAT(MTD, 'yyyyMM'), 
		CASE WHEN (area_name LIKE '%Java' and MaterialType IN ('MACHINE', 'FORK_LIFT')) THEN 'Java' WHEN (area_name LIKE '%Sumatera' and MaterialType IN ('MACHINE', 'FORK_LIFT')) THEN 'Sumatera' WHEN area_name = 'Trakindo Utama' then 'TUS' ELSE area_name END,
		sales_type,
		Customer_Type,
		ProductModel,
		MaterialType,
		INDUSTRY_KEY,
		CONCAT(MarketSector, '-', MaterialType)
) AS u3
) AS table_mapping1
) AS table_mapping

LEFT JOIN

(
--Start of table_union1
SELECT 
	*,
	CONCAT(CatID, MonthDeliveryDate, area_name_, sales_type, Customer_Type, ProductModel, MaterialType, INDUSTRY_KEY, MappingID) AS join_key
FROM
--Start of table_union
(
--Start of TableInvoice
SELECT 
	CASE
		WHEN invProgress = 'High Confidence' THEN 1
		WHEN invProgress = 'Workable SO (DP Paid)' THEN 2
		WHEN invProgress = 'PO Leasing' THEN 3
		WHEN invProgress = 'RA' THEN 4
		WHEN invProgress = 'GI' THEN 5
		WHEN invProgress = 'Delivery to Cust Site' THEN 6
		WHEN invProgress = 'BAST' THEN 7
		WHEN invProgress = 'Invoiced' THEN 8
	END AS CatID,
	MonthDeliveryDate,
	area_name_,
	sales_type,
	Customer_Type,
	ProductModel,
	MaterialType,
	INDUSTRY_KEY,
	MappingID,
	invProgress AS CatGroup,
	Forecasted,
	Unforecasted,
	Carried_Over
FROM
(
--Start of pivottable
SELECT
	MonthDeliveryDate,
	area_name_,
	sales_type,
	Customer_Type,
	ProductModel,
	MaterialType,
	INDUSTRY_KEY,
	MappingID,
	invProgress,
	[Forecasted], [Unforecasted], [Carried_Over]
FROM
(
--Start of TableSource
SELECT
	CASE
		WHEN SalesDocument IS NULL THEN 1111111
		ELSE SalesDocument
		END AS SalesDocument_,
	CASE
		WHEN isForecast = 'Yes' THEN 'Forecasted'
		WHEN isForecast = 'Carried Over' THEN 'Carried_Over'
		ELSE 'Unforecasted'
		END AS invStatus,
	CASE
		WHEN Workable = 'Yes'
			AND (ConfidenceLevel >= 75 or ConfidenceLevel IS NULL)
			AND StatusPGI = 'C'
			AND DeliveryDate IS NOT NULL
			AND BASTSigndate IS NOT NULL
			AND BillingDocument <> '' THEN 'Invoiced'
			
		WHEN Workable = 'Yes'
			AND (ConfidenceLevel >= 75 or ConfidenceLevel IS NULL)
			AND ReleaseApproval = 'Yes'
			AND StatusPGI = 'C'
			AND DeliveryDate IS NOT NULL
			AND BASTSigndate IS NOT NULL THEN 'BAST'

		WHEN sales_type IN ('ST3','ST5')
			AND BillingDocument <> ''
			AND BillingDocument IS NOT NULL THEN 'Invoiced'

		WHEN Workable = 'Yes'
			AND (ConfidenceLevel >= 75 or ConfidenceLevel IS NULL)
			AND ReleaseApproval = 'Yes'
			AND StatusPGI = 'C'
			AND DeliveryDate IS NOT NULL THEN 'Delivery to Cust Site'

		WHEN Workable = 'Yes'
			AND (ConfidenceLevel >= 75 or ConfidenceLevel IS NULL)
			AND ReleaseApproval = 'Yes'
			AND StatusPGI = 'C' THEN 'GI'

		WHEN Workable = 'Yes'
			AND (ConfidenceLevel >= 75 or ConfidenceLevel IS NULL)
			AND ReleaseApproval = 'Yes' THEN 'RA'

		WHEN POLeasing = 'Yes' THEN 'PO Leasing'

		WHEN Workable = 'Yes'
			AND (ConfidenceLevel >= 75 or ConfidenceLevel IS NULL) THEN 'Workable SO (DP Paid)'

		ELSE 'High Confidence' END AS invProgress,
	FORMAT(MTD, 'yyyyMM') AS MonthDeliveryDate,
	CASE 
		WHEN (area_name LIKE '%Java' and MaterialType IN ('MACHINE', 'FORK_LIFT')) THEN 'Java'
		WHEN (area_name LIKE '%Sumatera' and MaterialType IN ('MACHINE', 'FORK_LIFT')) THEN 'Sumatera' 
		WHEN area_name = 'Trakindo Utama' then 'TUS'
		ELSE area_name
	END AS area_name_,
	sales_type,
	Customer_Type,
	ProductModel,
	MaterialType,
	INDUSTRY_KEY,
	CONCAT(MarketSector, '-', MaterialType) AS MappingID

FROM EDW_ANALYTICS.CRM.EC_fact_invoice
WHERE FORMAT(MTD, 'yyyyMM') = (case when day(current_timestamp) = 1 then format(dateadd(month, -1, current_timestamp), 'yyyyMM') else format(CURRENT_TIMESTAMP, 'yyyyMM') end) --End of TableSource
) AS TableSource
PIVOT(
	COUNT(SalesDocument_)
	FOR invStatus IN([Forecasted], [Unforecasted], [Carried_Over])
) AS pivottable --End of pivottable
) AS TableInvoice --End of TableInvoice

UNION

--Start of PivotTableTotalSource
SELECT
	9 AS CatID,
	MonthDeliveryDate,
	area_name_,
	sales_type,
	Customer_Type,
	ProductModel,
	MaterialType,
	INDUSTRY_KEY,
	MappingID,
	'Total' AS CatGroup,
	[Forecasted], [Unforecasted], [Carried_Over]
FROM
(
--Start of TableTotalSource
SELECT
	CASE
		WHEN SalesDocument IS NULL THEN 1111111
		ELSE SalesDocument
		END AS SalesDocument_,
	CASE
		WHEN isForecast = 'Yes' THEN 'Forecasted'
		WHEN isForecast = 'Carried Over' THEN 'Carried_Over'
		ELSE 'Unforecasted'
		END AS invStatus,
	FORMAT(MTD, 'yyyyMM') AS MonthDeliveryDate,
	CASE 
		WHEN (area_name LIKE '%Java' and MaterialType IN ('MACHINE', 'FORK_LIFT')) THEN 'Java'
		WHEN (area_name LIKE '%Sumatera' and MaterialType IN ('MACHINE', 'FORK_LIFT')) THEN 'Sumatera' 
		WHEN area_name = 'Trakindo Utama' then 'TUS'
		ELSE area_name
	END AS area_name_,
	sales_type,
	Customer_Type,
	ProductModel,
	MaterialType,
	INDUSTRY_KEY,
	CONCAT(MarketSector, '-', MaterialType) AS MappingID

FROM EDW_ANALYTICS.CRM.EC_fact_invoice
WHERE FORMAT(MTD, 'yyyyMM') = (case when day(current_timestamp) = 1 then format(dateadd(month, -1, current_timestamp), 'yyyyMM') else format(CURRENT_TIMESTAMP, 'yyyyMM') end) --End of TableTotalSource
) AS TableTotalSource
PIVOT(
	COUNT(SalesDocument_)
	FOR invStatus IN([Forecasted], [Unforecasted], [Carried_Over])
) AS PivotTableTotalSource --End of PivotTableTotalSource
) AS table_union
) AS table_union1 --End of table_union1
ON table_mapping.joinkey = table_union1.join_key --End of table_before_calculations
) AS table_before_calculations
) AS change_null_step
) AS total_step
) AS lag_step
) AS change_null_lag
) AS cumsum_step
) AS comparison_step
ORDER BY MonthDeliveryDate, area_name_, sales_type, Customer_Type, ProductModel, MaterialType, INDUSTRY_KEY, MappingID, CatID