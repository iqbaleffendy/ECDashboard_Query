truncate table EDW_ANALYTICS_STG.CRM.stg_EC_ordertrackingPP;

insert into EDW_ANALYTICS_STG.CRM.stg_EC_ordertrackingPP
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
	b.ACTUAL_NETREV * (ABS(d.kurrf)) ActualNetInIDR,
	b.ACTUAL_NETREV ActualNetInUSD,
	da.customer_class_code CustomerClassCodes,
	a.MATNR_VBAP as ProductCode,
	dpm.PRODUCT_HIERARCHY,
	GETDATE() AS ETL_DATE

from [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_V_ODRPP_BW a left join 
[LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_F_INV_SOLD b on a.VBELN_VBAP = b.VBELN and a.POSNR_VBAP =b.POSNR
left join [LS_BI_PROD].EDW_ANALYTICS.ECC.dim_account da on da.account_id = a.KUNNR_VBAK
left join [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.VBRK d on b.BILLING_DOC = d.VBELN
left join [LS_BI_PROD].EDW_STG_SAP_ECC_DAILY.dbo.ZLDB_V_SO_OPPTY po on cast(VBELN_VBAP as int) = po.SO_NUMBER and POSNR_VBAP = po.SO_ITEM
left join (select DISTINCT cic_group_id,cic_group from ls_bi_prod.EDW_DIMENSION.CRM.Dim_CIC) cic on cic.cic_group_id = b.brsch
left join (select * from [LS_BI_PROD].EDW_ANALYTICS.CRM.dim_opp_product_material where product_material_code not in ('N/A', 'Unknown') and left(product_hierarchy, 2) in ('M1','E1','F1')) dpm on CAST(a.MATNR_VBAP AS INT) = CAST (dpm.PRODUCT_MATERIAL_CODE AS INT)
where 1=1
and format(a.DELDATE,'yyyy') in ((format(CURRENT_TIMESTAMP,'yyyy') - 1), format(CURRENT_TIMESTAMP,'yyyy'))
--and format(a.DELDATE,'yyyyMM') in (format(DATEADD(MONTH, -1, CURRENT_TIMESTAMP),'yyyyMM'), format(CURRENT_TIMESTAMP,'yyyyMM'))
;