--diagnostic helpstats on for session;
--drop table tdavis1_ltm_trans_a;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_trans_a AS
(
SELECT	tld.payment_transid
	,EXTRACT(MONTH FROM tld.transaction_created_date) + EXTRACT(YEAR FROM tld.transaction_Created_date)*12 AS month_math
	,tld.transaction_created_ts
	,tld.transaction_created_date
	,tld.customer_id AS sender_id
	,tld.customer_counterparty AS receiver_id
	,tld.funding_source

	,tld.transaction_flag3/128 MOD 2 AS spoof_flag
	,
CASE	WHEN tld.atkc_bp_program IS NOT NULL THEN 1 
ELSE	0 
END	AS pp_merch_claim_exists
	,flow.flow_family
	,flow.primary_flow
	,
CASE	WHEN primary_flow = 'MS PF Virtual Terminal' THEN 'VT'
			WHEN primary_flow = 'MS PF Direct Payments' THEN 'DCC'
			ELSE 'Other' 
END	AS DCC_VT_ACTUAL
	,tld.transaction_created_date - customer_created_date AS sender_tof
	,tld.transaction_created_date - counterparty_created_date AS receiver_tof
	,
CASE	
		WHEN ttddp.tld_manifestation = 'ACH Return'
			 
	AND	tld.tfc_ar_reason_group_desc NOT IN ('fraud','nsf')
		THEN 'Highrisk'
		ELSE ttddp.TLD_Risk_category    														
	END	AS pp_category
		,
CASE	
		WHEN ttddp.tld_manifestation = 'ACH Return'
			 
	AND	tld.tfc_ar_reason_group_desc NOT IN ('fraud','nsf')
		THEN 'Highrisk'
		ELSE ttddp.TLD_Risk_subcategory    														
	END	AS pp_sub_category
	,ttddp.tld_manifestation AS pp_manifestation
	,
CASE	
		WHEN ttdds.tld_manifestation = 'ACH Return'
			 
	AND	tld.tfc_ar_reason_group_desc NOT IN ('fraud','nsf')
		THEN 'Highrisk'
		ELSE ttdds.TLD_Risk_category    														
	END	AS seller_category
	,ttdds.tld_manifestation  AS seller_manifestation
	,
CASE	
		WHEN ttddb.tld_manifestation = 'ACH Return'
			 
	AND	tld.tfc_ar_reason_group_desc NOT IN ('fraud','nsf')
		THEN 'Highrisk'
		ELSE ttddb.TLD_Risk_category    														
	END	AS buyer_category
	,ttddb.tld_manifestation  AS buyer_manifestation
	,tld.counterparty_category
	,
CASE	
		WHEN	tld.atkc_dispute_reason IN ('Non-Receipt','Guarantee: Non-Receipt') THEN 'INR'
		WHEN	tld.atkc_dispute_reason IN ('Not as Described','Guarantee: Defective or Incorrec') THEN 'SNAD'
		ELSE	'Other'
	END	AS Dispute_Reason
 ,
CASE	
		WHEN tld.atkc_workflow_code='F' 
	AND	(ZEROIFNULL(tld.pymt_reversal_adj_amt_usd)+ZEROIFNULL(pymt_reversal_user_amt_usd))<-0.01
		THEN trans_amt_usd
		WHEN tld.atkc_workflow_code IS NULL 
	AND	ZEROIFNULL(tld.pymt_reversal_adj_amt_usd)<-0.01
		THEN trans_amt_usd
		ELSE 0
	END 																									AS gross_loss_agent
 , 
CASE	WHEN tld.atkc_workflow_code IN ('D','C','S') THEN tld.atkc_dispute_amt_usd 
ELSE	0 
END	AS  gross_loss_user 
 , 
CASE	WHEN tld.ach_return_cnt >= 1 THEN tld.ach_trans_amt_usd 
ELSE	0 
END	AS gross_loss_ach 
 , 
CASE	WHEN tld.cbe_gross_amt_usd*-1>0 THEN tld.cbe_gross_amt_usd*-1 
ELSE	0 
END	AS gross_loss_cb
 , (ZEROIFNULL(gross_loss_agent) + ZEROIFNULL(gross_loss_user) + ZEROIFNULL(gross_loss_ach) + ZEROIFNULL(gross_loss_cb)) AS paypal_gross_loss
 , 
CASE	WHEN ((ZEROIFNULL(gross_loss_agent) + ZEROIFNULL(gross_loss_user) + ZEROIFNULL(gross_loss_ach) + ZEROIFNULL(gross_loss_cb)) > tld.trans_amt_usd) THEN tld.trans_amt_usd 
  
ELSE	(ZEROIFNULL(gross_loss_agent) + ZEROIFNULL(gross_loss_user) + ZEROIFNULL(gross_loss_ach) + ZEROIFNULL(gross_loss_cb)) 
END	 AS paypal_gross_lossddp
	,
CASE	
		WHEN tld.atkc_workflow_code='F'
	AND	tld.atkc_dispute_reason IN (' ', 'Non-Receipt', 'Not as Described',
		'Probable Not as Described Seller','Probable Non-Receipt Seller',
		'Confirmed Marketplace Item Remov','Confirmed Other')
		THEN 'Merch'
		WHEN tld.atkc_workflow_code='F'
		THEN 'Unauth'
		WHEN tld.atkc_workflow_code IS NULL 
	AND	tld.pymt_reversal_reason_desc IN ('Buyer Complaint','EBAY SPPP')
		THEN 'Merch'
		WHEN tld.atkc_workflow_code IS NULL 
	AND	tld.pymt_reversal_reason_desc IN ('Admin Reversal','Buyer Spoof')
		THEN 'Unauth'
		ELSE 'OT'
	END 																										AS risk_cat_agent

	,
CASE	
		WHEN tld.cbe_reason_group_desc='Unauth'
		THEN 'Unauth'
		WHEN tld.cbe_reason_group_desc IN ('Merch','Non_Receipt')
		THEN 'Merch'
		ELSE 'OT'
	END 																										AS risk_cat_cb

	,
CASE	
		WHEN tld.tfc_ar_reason_group_desc='fraud'
		THEN 'Unauth'
		WHEN tld.tfc_ar_reason_group_desc='nsf'
		THEN 'NSF'
		ELSE 'Highrisk'
	END 																										AS risk_cat_ach

	,
CASE	
		WHEN tld.atkc_workflow_code IN ('D','C')
		THEN 'Merch'
		WHEN tld.atkc_workflow_code ='S'
		THEN 'Unauth'
		ELSE 'OT'
	END 																										AS risk_cat_user
	,
CASE	WHEN ttddp.TLD_Risk_category='Unauth' 
		OR (ZEROIFNULL(atkc_dispute_amt_usd)<>0 
	AND	atkc_workflow_code IN ('S'))
		OR (ZEROIFNULL(cbe_gross_amt_usd)<>0 
	AND	cbe_reason_group_desc IN ('Unauth'))
		OR (ZEROIFNULL(ach_trans_amt_usd)<>0 
	AND	tfc_ar_reason_group_desc IN ('fraud'))
		OR ((ZEROIFNULL(pymt_reversal_adj_amt_usd)<>0 
	OR	ZEROIFNULL(pymt_reversal_cancel_amt_usd)<>0) 
			AND pymt_reversal_reason_desc IN ('Fraud Reversal', 'Admin Reversal',
		'Buyer Spoof'))
		THEN 1
		ELSE 0 
END	AS unauth_exists
	,
CASE	WHEN ttddp.TLD_Risk_category='Merchant' 
		OR (ZEROIFNULL(cbe_gross_amt_usd)<>0 
	AND	cbe_reason_group_desc IN ('Merch','Non_Receipt'))
		OR (ZEROIFNULL(atkc_dispute_amt_usd)<>0 
	AND	atkc_workflow_code IN ('D','C'))
		THEN 1
		ELSE 0 
END	AS merch_exists
	,
CASE	WHEN ttddp.TLD_Risk_category='NSF' 
		OR (ZEROIFNULL(ach_trans_amt_usd)<>0 
	AND	tfc_ar_reason_group_desc IN ('nsf'))
		THEN 1
		ELSE 0 
END	AS nsf_exists
	,
CASE	WHEN (ZEROIFNULL(cbe_gross_amt_usd)<>0 
	AND	cbe_reason_group_desc IN ('Other','Special'))
		OR (ZEROIFNULL(ach_trans_amt_usd)<>0 
	AND	tfc_ar_reason_group_desc IN ('admin','highrisk','user'))
		OR ((ZEROIFNULL(pymt_reversal_adj_amt_usd)<>0 
	OR	ZEROIFNULL(pymt_reversal_cancel_amt_usd)<>0) 
	AND	pymt_reversal_reason_desc IS NULL) 
		THEN 1 
		ELSE 0 
END	AS other_exists
	,ZEROIFNULL(tld.trans_amt_usd) AS trans_amt_usd
		,(
CASE	
			WHEN tld.paypal_loss_primary_reason = ttddp.TLD_trans_desc
			THEN tld.paypal_net_loss_amt_usd*-1
			ELSE 0
		END)																							AS paypal_net_loss_amt_usd
		,(
CASE	
			WHEN tld.buyer_loss_primary_reason = ttddb.TLD_trans_desc
			THEN tld.buyer_rm_net_loss_amt_usd*-1
			ELSE 0
		END)																							AS buyer_net_loss
	,(
CASE	
			WHEN tld.seller_loss_primary_reason = ttdds.TLD_trans_desc
			THEN tld.seller_rms_net_loss_amt_usd*-1
			ELSE 0
		END)		AS seller_net_loss
	,(
CASE	
			WHEN tld.paypal_loss_primary_reason = ttddp.TLD_trans_desc
			THEN tld.pp_nb_net_loss_amt_usd*-1
			ELSE 0
		END) AS pp_net_nb_loss_amt_usd
	,(
CASE	
			WHEN tld.paypal_loss_primary_reason = ttddp.TLD_trans_desc
			THEN tld.pp_dp_net_loss_amt_usd*-1
			ELSE 0
		END) AS pp_net_dp_loss_amt_usd
			,ZEROIFNULL(
CASE	WHEN tld.tfc_wacko_flag='Y' THEN 0 
ELSE	tld.nb_confirm_counter_amt_usd 
END	) AS receiver_nb
	,ZEROIFNULL(
CASE	WHEN tld.tfc_wacko_flag='Y' THEN 0 
ELSE	tld.nb_recover_counter_amt_usd 
END	) AS receiver_nb_recover
	 ,
CASE	 
	WHEN	tld.funding_source='CC' 
	AND	(flow2.FLAG1/'1000'x MOD 2=1 
	OR	flow2.FLAG1/'2000'x MOD 2=1 )
                THEN 'Y'
                
ELSE	'N'
        
END	AS cc_Ctrl_Grp_Type
     ,
CASE	 
	WHEN	tld.funding_source IN ( 'EL' , 'IA') 
	AND	(flow2.FLAG1/'800000'x MOD 2=1 
	OR	flow2.FLAG1/'1000000'x MOD 2=1 )
                THEN 'Y'
                
ELSE	'N'
                
END	AS iACH_Ctrl_Grp_Type
     ,
CASE	WHEN tld.trans_amt_usd <=25 THEN '$0-$25'
		WHEN tld.trans_amt_usd <=50 THEN '$25-$50'
		WHEN tld.trans_amt_usd <=100 THEN '$50-$100'
		WHEN tld.trans_amt_usd <= 250 THEN '$100-$250'
		WHEN tld.trans_amt_usd <= 500 THEN '$250-$500'
		WHEN tld.trans_amt_usd <= 1000 THEN '$500-$1K'
		WHEN tld.trans_amt_usd <= 1500 THEN '$1K-$1.5K'
		WHEN tld.trans_amt_usd <= 2000 THEN '$1.5K-$2K'
		WHEN tld.trans_amt_usd <= 2500 THEN '$2K-$2.5K'
		WHEN tld.trans_amt_usd <= 3000 THEN '$2.5-$3K'
		ELSE '>$3K'
	END ASP_bucket
,
CASE	WHEN	tld.transaction_subtype='I' THEN 'On-Ebay'  
ELSE	'Off-Ebay' 	END	AS OnOff_EBay
	,
CASE		WHEN	tld.flow_to_country = '99'  
	OR	 tld.flow_from_country = tld.flow_to_country THEN 'IB'
		ELSE	'XB' 	END AS intra_cross
,
CASE	WHEN tld.flow_to_country  = 'AT' THEN 'AT'
			WHEN tld.flow_to_country IN ('AU','NZ') THEN 'AU'
			WHEN tld.flow_to_country = 'BE' THEN 'BE'
			WHEN tld.flow_to_country = 'CA' THEN 'CA' 
			WHEN tld.flow_to_country = 'CH' THEN 'CH' 
			WHEN tld.flow_to_country IN ('CN' , 'C2' ) THEN 'CN'
			WHEN tld.flow_to_country = 'DE' THEN 'DE'
			WHEN tld.flow_to_country = 'ES' THEN 'ES'
			WHEN tld.flow_to_country = 'FR' THEN 'FR'
			WHEN tld.flow_to_country = 'HK' THEN 'HK'
			WHEN tld.flow_to_country = 'IT' THEN 'IT'
			WHEN tld.flow_to_country = 'JP' THEN 'JP'
			WHEN tld.flow_to_country = 'NL' THEN 'NL'
			WHEN tld.flow_to_country = 'PL' THEN 'PL'
			WHEN tld.flow_to_country IN ('BD','BT','KH','IN','ID','KP',
		'KR','LA','MO','MY','MV','FM','MN','NP','PK','PH','WS','LK',
		'TH','TO','VN') THEN 'RAP'
			WHEN tld.flow_to_country IN ('BG','CY','CZ','DK','EE','FI',
		'GR','HU','LV','LT','LU','MT','PT','RO','RS','SK','SI','SE') THEN 'REU'
			WHEN tld.flow_to_country IN ('AX','AL','AD','BA','HR','GG',
		'IS','IM','LI','MK','MD','MC','NO','ME','RU','SM','CS','TR',
		'UA','VA','YU') THEN 'RME'
			WHEN tld.flow_to_country IN ('AF','DZ','AS','AO','AI','AQ',
		'AG','AR','AM','AW','AZ','BS','BH','BB','BY','BZ','BJ','BM',
		'BO','BW','BV','BR','IO','BN','BF','BI','CM','CV','KY','CF',
		'TD','CL','CX','CC','CO','KM','CD','CG','CK','CR','CI','CU',
		'DJ','DM',
										'DO','TL','TP','EC','EG','SV','GQ','ER','ET','FK','FO',
		'FJ','FX','GF','PF','TF','GA','GM','GE','GH','GI','GL','GD',
		'GP','GU','GT','GN','GW','GY','HT','HM','HN','IR','IQ','IL',
		'JM','JE','JO','KZ','KE','KI','KW','KG','LB'
										,'LS','LR','LY','MG','MW','ML','MH','MQ','MR','MU','YT',
		'MX','MS','MA','MZ','MM','NA','NR','AN','NC','NI','NE','NG',
		'NU','NF','MP','OM','PW','PS','PA','PG','PY','PE','PN','PR',
		'QA','RE','RW','KN','LC','VC',
										'ST','SA','SN','SC','SL','SB','SO','ZA','GS','SH','PM',
		'SD','SR','SJ','SZ','SY','TJ','TZ','TG','TK','TT','TN','TM',
		'TC','TV','UG','AE','UM','UY','UZ','VU','VE','VG','VI','WF',
		'EH','YE','ZM','ZW') THEN 'ROW'
			WHEN tld.flow_to_country = 'SG' THEN 'SG'
			WHEN tld.flow_to_country = 'TW' THEN 'TW'
			WHEN tld.flow_to_country IN ('IE' , 'GB') THEN 'UK'
			WHEN tld.flow_to_country IN ('-1', 'US' , '99') THEN 'US'
			ELSE 'not_defined'
			END AS receiver_geography
			
,
CASE	WHEN tld.flow_from_country  = 'AT' THEN 'AT'
			WHEN tld.flow_from_country IN ('AU','NZ') THEN 'AU'
			WHEN tld.flow_from_country = 'BE' THEN 'BE'
			WHEN tld.flow_from_country = 'CA' THEN 'CA' 
			WHEN tld.flow_from_country = 'CH' THEN 'CH' 
			WHEN tld.flow_from_country IN ('CN' , 'C2' ) THEN 'CN'
			WHEN tld.flow_from_country = 'DE' THEN 'DE'
			WHEN tld.flow_from_country = 'ES' THEN 'ES'
			WHEN tld.flow_from_country = 'FR' THEN 'FR'
			WHEN tld.flow_from_country = 'HK' THEN 'HK'
			WHEN tld.flow_from_country = 'IT' THEN 'IT'
			WHEN tld.flow_from_country = 'JP' THEN 'JP'
			WHEN tld.flow_from_country = 'NL' THEN 'NL'
			WHEN tld.flow_from_country = 'PL' THEN 'PL'
			WHEN tld.flow_from_country IN ('BD','BT','KH','IN','ID','KP',
		'KR','LA','MO','MY','MV','FM','MN','NP','PK','PH','WS','LK',
		'TH','TO','VN') THEN 'RAP'
			WHEN tld.flow_from_country IN ('BG','CY','CZ','DK','EE','FI',
		'GR','HU','LV','LT','LU','MT','PT','RO','RS','SK','SI','SE') THEN 'REU'
			WHEN tld.flow_from_country IN ('AX','AL','AD','BA','HR','GG',
		'IS','IM','LI','MK','MD','MC','NO','ME','RU','SM','CS','TR',
		'UA','VA','YU') THEN 'RME'
			WHEN tld.flow_from_country IN ('AF','DZ','AS','AO','AI','AQ',
		'AG','AR','AM','AW','AZ','BS','BH','BB','BY','BZ','BJ','BM',
		'BO','BW','BV','BR','IO','BN','BF','BI','CM','CV','KY','CF',
		'TD','CL','CX','CC','CO','KM','CD','CG','CK','CR','CI','CU',
		'DJ','DM',
										'DO','TL','TP','EC','EG','SV','GQ','ER','ET','FK','FO',
		'FJ','FX','GF','PF','TF','GA','GM','GE','GH','GI','GL','GD',
		'GP','GU','GT','GN','GW','GY','HT','HM','HN','IR','IQ','IL',
		'JM','JE','JO','KZ','KE','KI','KW','KG','LB'
										,'LS','LR','LY','MG','MW','ML','MH','MQ','MR','MU','YT',
		'MX','MS','MA','MZ','MM','NA','NR','AN','NC','NI','NE','NG',
		'NU','NF','MP','OM','PW','PS','PA','PG','PY','PE','PN','PR',
		'QA','RE','RW','KN','LC','VC',
										'ST','SA','SN','SC','SL','SB','SO','ZA','GS','SH','PM',
		'SD','SR','SJ','SZ','SY','TJ','TZ','TG','TK','TT','TN','TM',
		'TC','TV','UG','AE','UM','UY','UZ','VU','VE','VG','VI','WF',
		'EH','YE','ZM','ZW') THEN 'ROW'
			WHEN tld.flow_from_country = 'SG' THEN 'SG'
			WHEN tld.flow_from_country = 'TW' THEN 'TW'
			WHEN tld.flow_from_country IN ('IE' , 'GB') THEN 'UK'
			WHEN tld.flow_from_country IN ('-1', 'US' , '99') THEN 'US'
			ELSE 'not_defined'
			END AS sender_geography
			,tld.flow_from_country
			,tld.flow_to_country
FROM	pp_access_views.tld_negative_payment tld
	LEFT JOIN pp_mstr_access_views.tld_trans_desc_dim ttddp
		ON tld.paypal_loss_primary_reason=ttddp.TLD_trans_desc
	LEFT JOIN pp_mstr_access_views.tld_trans_desc_dim ttdds
		ON tld.seller_loss_primary_reason=ttdds.TLD_trans_desc
	LEFT JOIN pp_mstr_access_views.tld_trans_desc_dim ttddb
		ON tld.buyer_loss_primary_reason=ttddb.TLD_trans_desc
	LEFT JOIN pp_access_views.cdim_payment_flow2 flow
		ON tld.pmt_flow_key2=flow.pmt_flow_key2
	LEFT JOIN pp_risk_views.dw_payment_flow flow2
		ON tld.payment_transid=flow2.trans_id
WHERE	 (tld.tfc_wacko_flag IS NULL 
	OR	tld.tfc_wacko_flag IN ('N'))	
) 
WITH	DATA  UNIQUE PRIMARY INDEX (payment_transid);

--drop table pp_scratch_gba.tdavis1_ltm_trans;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_trans AS
(
SEL	
	tld.*
	,sender.sndr_type_grp_name AS buyer_type
FROM	pp_scratch_gba.tdavis1_ltm_trans_a tld
	LEFT JOIN pp_access_views.dw_payment_sent_adhoc adhoc
		ON tld.payment_transid=adhoc.payment_transid
	JOIN pp_access_views.cdim_sender_type sender
		ON adhoc.sndr_type_key=sender.sndr_type_key
QUALIFY	RANK() OVER (PARTITION BY tld.payment_transid 
ORDER	BY adhoc.transaction_reference_date DESC)=1
) 
WITH	DATA  UNIQUE PRIMARY INDEX (payment_transid);

--drop table pp_scratch_gba.tdavis1_ltm_nb_losses;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_nb_losses AS
(
SELECT	receiver_id
	,SUM(ZEROIFNULL(
CASE	WHEN lts.unauth_exists=1
		THEN lts.receiver_nb+lts.receiver_nb_recover 
ELSE	0 
END	)) AS unauth_nb
	,SUM(ZEROIFNULL(
CASE	WHEN lts.merch_exists=1
		THEN lts.receiver_nb+lts.receiver_nb_recover 
ELSE	0 
END	)) AS merch_nb
	,SUM(ZEROIFNULL(
CASE	WHEN lts.nsf_exists=1
		THEN lts.receiver_nb+lts.receiver_nb_recover 
ELSE	0 
END	)) AS nsf_nb 
FROM	pp_scratch_gba.tdavis1_ltm_trans lts
GROUP	BY 1
) 
WITH	DATA UNIQUE PRIMARY INDEX (receiver_id)
;
--drop table pp_scratch_gba.tdavis1_ltm_receivers;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_receivers AS
(
SELECT	receiver_id
	,month_math
FROM	pp_scratch_gba.tdavis1_ltm_trans
GROUP	BY 1,2
) 
WITH	DATA UNIQUE PRIMARY INDEX (receiver_id, month_math)
;
--drop table  pp_scratch_gba.tdavis1_ltm_senders;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_senders AS
(
SELECT	sender_id
FROM	pp_scratch_gba.tdavis1_ltm_trans
GROUP	BY 1
) 
WITH	DATA UNIQUE PRIMARY INDEX (sender_id)
;
--drop table pp_scratch_gba.tdavis1_ltm_receiver_sum1;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_receiver_sum1 AS 
(
SELECT	lts.receiver_id
	,lts.month_math
	,SUM(ZEROIFNULL(lts.trans_amt_usd)) AS bad_tpv
	,SUM(ZEROIFNULL(
CASE	WHEN lts.unauth_exists=1
		THEN lts.trans_amt_usd 
ELSE	0 
END	)) AS unauth_tpv
	,SUM(ZEROIFNULL(
CASE	WHEN lts.merch_exists=1
		THEN lts.trans_amt_usd 
ELSE	0 
END	)) AS merch_tpv
	,SUM(ZEROIFNULL(
CASE	WHEN lts.nsf_exists=1
		THEN lts.trans_amt_usd 
ELSE	0 
END	)) AS nsf_tpv
FROM	pp_scratch_gba.tdavis1_ltm_trans lts
GROUP	BY 1,2
) 
WITH	DATA UNIQUE PRIMARY INDEX (receiver_id, month_math)
;

/*
DROP TABLE pp_scratch_gba.vk_sent_adhoc ;
create table pp_scratch_gba.vk_sent_adhoc as
(select dwps.tran_customer_id as sender_id
,dwps.customer_counterparty as receiver_id
	,dwps.payment_transid
	,dwps.transaction_usd_equiv_amt 
	,dwps.transaction_created_date
from pp_access_views.dw_payment_sent dwps
		
where (dwps.transaction_status in ('S', 'V', 'P') 
		or (dwps.transaction_status = 'D' 
			and dwps.transaction_status_reason is null))
--group by 1
) with data unique primary index (payment_transid)
;
create table vk_sent_adhoc1 as (
SELECT  sender_id,
			SUM(transaction_usd_equiv_amt)  AS tpv,
			extract(month from adhoc.transaction_created_date) + extract(year from adhoc.transaction_Created_date)*12 as month_math
from vk_sent_adhoc adhoc
group by 1,3
)WITH DATA UNIQUE PRIMARY INDEX (sender_id, month_math);
SEL EXTRACT(MONTH FROM '2008-01-01') + EXTRACT(YEAR FROM  '2008-01-01')*12 
SEL month_id,calendar_dt AS month_date FROM pp_access_views.calendar_day_dim  WHERE calendar_dt='2008-02-01'
SEL TOP 10 * FROM pp_discovery_views.agg_SNDR_pmt_mth ;
*/
--DROP TABLE vk_sent_adhoc1;
CREATE	TABLE vk_sent_adhoc1 AS (
SELECT	
			sndr_id AS sender_id,		
			SUM(ZEROIFNULL(gtpv_usd_amt))AS tpv
FROM	pp_discovery_views.agg_SNDR_pmt_mth
GROUP	BY 1
)
WITH	DATA UNIQUE PRIMARY INDEX (sender_id);

--DROP TABLE vk_sent_adhoc2;
CREATE	TABLE vk_sent_adhoc2 AS (
SELECT	rcvr_id AS receiver_id,
			SUM(ZEROIFNULL(gtpv_usd_amt))AS tpv
FROM	pp_discovery_views.AGG_RCVR_PMT_MTH adhoc
GROUP	BY 1
)
WITH	DATA UNIQUE PRIMARY INDEX (receiver_id);

COLLECT	STATISTICS pp_scratch_gba.vk_sent_adhoc1 COLUMN SENDER_ID;
--drop table pp_scratch_gba.tdavis1_ltm_sender_sum1;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_sender_sum1 AS 
(
SELECT	ls.sender_id
	,tpv
FROM	pp_scratch_gba.tdavis1_ltm_senders ls
	LEFT JOIN vk_sent_adhoc1 adhoc
		ON ls.sender_id=adhoc.sender_id
) 
WITH	DATA UNIQUE PRIMARY INDEX (sender_id);

--drop table pp_scratch_gba.tdavis1_ltm_sender_sum2 ;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_sender_sum2 AS 
(
SELECT	lts.sender_id
	,lts.month_math
	,SUM(ZEROIFNULL(lts.trans_amt_usd)) AS bad_tpv
	,SUM(ZEROIFNULL(
CASE	WHEN lts.unauth_exists=1
		THEN lts.trans_amt_usd 
ELSE	0 
END	)) AS unauth_tpv
	,SUM(ZEROIFNULL(
CASE	WHEN lts.merch_exists=1
		THEN lts.trans_amt_usd 
ELSE	0 
END	)) AS merch_tpv
	,SUM(ZEROIFNULL(
CASE	WHEN lts.nsf_exists=1
		THEN lts.trans_amt_usd 
ELSE	0 
END	)) AS nsf_tpv
FROM	pp_scratch_gba.tdavis1_ltm_trans lts
GROUP	BY 1,2
) 
WITH	DATA UNIQUE PRIMARY INDEX (sender_id, month_math);
--drop table pp_scratch_gba.tdavis1_ltm_sender_sum3;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_sender_sum3 AS 
(
SELECT	lts.sender_id
	,lts.month_math
	,SUM(ZEROIFNULL(lts.paypal_net_loss_amt_usd)) AS net_loss
	,SUM(ZEROIFNULL(
CASE	WHEN lts.pp_category = 'Unauth'
		THEN lts.paypal_net_loss_amt_usd 
ELSE	0 
END	)) AS unauth_netloss
	,SUM(ZEROIFNULL(
CASE	WHEN lts.pp_category='Merchant'
		THEN lts.paypal_net_loss_amt_usd 
ELSE	0 
END	)) AS merch_netloss
	,SUM(ZEROIFNULL(
CASE	WHEN lts.pp_category='NSF'
		THEN lts.paypal_net_loss_amt_usd 
ELSE	0 
END	)) AS nsf_netloss
			,SUM(ZEROIFNULL(
CASE	WHEN lts.pp_category= 'Highrisk'
		THEN lts.paypal_net_loss_amt_usd 
ELSE	0 
END	)) AS highrisk_netloss
FROM	pp_scratch_gba.tdavis1_ltm_trans lts
GROUP	BY 1,2
) 
WITH	DATA UNIQUE PRIMARY INDEX (sender_id, month_math);

COLLECT	STATISTICS pp_scratch_gba.vk_sent_adhoc2 COLUMN receiver_id;
--drop table pp_scratch_gba.tdavis1_ltm_receiver_sum2;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_receiver_sum2 AS 
(
SELECT	ls.receiver_id
	,tpv
FROM	pp_scratch_gba.tdavis1_ltm_receivers ls
	LEFT JOIN vk_sent_adhoc2 adhoc
		ON ls.receiver_id=adhoc.receiver_id
) 
WITH	DATA UNIQUE PRIMARY INDEX (receiver_id)
;
--DROP TABLE pp_scratch_gba.tdavis1_ltm_receiver_sum_sum;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_receiver_sum_sum AS 
(
SELECT	adhoc.receiver_id
	,SUM(ZEROIFNULL(tpv)) AS tpv
FROM	pp_scratch_gba.tdavis1_ltm_receiver_sum2 adhoc
GROUP	BY 1
) 
WITH	DATA UNIQUE PRIMARY INDEX (receiver_id);

--DROP TABLE pp_scratch_gba.tdavis1_ltm_receiver_sum_sum1;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_receiver_sum_sum1 AS 
(
SELECT	lts.receiver_id
	,SUM(ZEROIFNULL(bad_tpv)) AS bad_tpv
	,SUM(ZEROIFNULL(unauth_tpv)) AS unauth_tpv
	,SUM(ZEROIFNULL(merch_tpv)) AS merch_tpv
	,SUM(ZEROIFNULL(nsf_tpv)) AS nsf_tpv
FROM	pp_scratch_gba.tdavis1_ltm_receiver_sum1 lts
GROUP	BY 1
) 
WITH	DATA UNIQUE PRIMARY INDEX (receiver_id)
;
--drop table  pp_scratch_gba.tdavis1_ltm_sender_sum_sum1;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_sender_sum_sum1 AS 
(
SELECT	adhoc.sender_id
	,SUM(ZEROIFNULL(tpv)) AS tpv
FROM	pp_scratch_gba.tdavis1_ltm_sender_sum1 adhoc
GROUP	BY 1
) 
WITH	DATA UNIQUE PRIMARY INDEX (sender_id);
--drop table  pp_scratch_gba.tdavis1_ltm_sender_sum_sum2;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_sender_sum_sum2 AS 
(
SELECT	lts.sender_id
	,SUM(ZEROIFNULL(bad_tpv)) AS bad_tpv
	,SUM(ZEROIFNULL(unauth_tpv)) AS unauth_tpv
	,SUM(ZEROIFNULL(merch_tpv)) AS merch_tpv
	,SUM(ZEROIFNULL(nsf_tpv)) AS nsf_tpv
FROM	pp_scratch_gba.tdavis1_ltm_sender_sum2 lts
GROUP	BY 1
) 
WITH	DATA UNIQUE PRIMARY INDEX (sender_id);
--drop table pp_scratch_gba.tdavis1_ltm_sender_sum_sum3;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_sender_sum_sum3 AS 
(
SELECT	lts.sender_id
	,SUM(ZEROIFNULL(net_loss)) AS net_loss
	,SUM(ZEROIFNULL(unauth_netloss)) AS unauth_netloss
	,SUM(ZEROIFNULL(merch_netloss)) AS merch_netloss
	,SUM(ZEROIFNULL(nsf_netloss)) AS nsf_netloss
	,SUM(ZEROIFNULL(highrisk_netloss)) AS highrisk_netloss
FROM	pp_scratch_gba.tdavis1_ltm_sender_sum3 lts
GROUP	BY 1
) 
WITH	DATA UNIQUE PRIMARY INDEX (sender_id);
--drop table pp_scratch_gba.tdavis1_ltm_ato;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_ato AS
(
SELECT	cases.atkc_case_id AS child_case_id
	,orig_txn.atkcfl_val_ullong AS original_transaction
	,tmp_credit.atkcfl_val_ullong AS temp_credit_transid
	,parent_case.atkcfl_val_ullong AS parent_case_id
	,parent_case_data.outcome AS outcome
FROM	
	(
SELECT	atkc_case_id
		,atkc_close_date
	FROM pp_access_views.dw_atk_case_general
	WHERE atkc_wkfl_id=43) cases
	LEFT JOIN 
		(
SELECT	*
		FROM pp_access_views.dw_atk_case_field_value
		WHERE atkcfl_field_id=18001
		) orig_txn
		ON cases.atkc_case_id=orig_txn.atkcfl_case_id
	LEFT JOIN
		(
SELECT	*
		FROM pp_access_views.dw_atk_case_field_value
		WHERE atkcfl_field_id=18039
			AND atkcfl_val_ullong>0
		) tmp_credit
		ON cases.atkc_case_id=tmp_credit.atkcfl_case_id
	LEFT JOIN 
		(
SELECT	*
		FROM pp_access_views.dw_atk_case_field_value
		WHERE atkcfl_field_id=18018
		) parent_case
		ON cases.atkc_case_id=parent_case.atkcfl_case_id
	LEFT JOIN 
		(
SELECT	cases.atkc_case_id
			,
CASE	WHEN outcome.atkcfl_val_dropdown=85001 THEN 'Accepted'
				WHEN outcome.atkcfl_val_dropdown=85002 THEN 'Denied'
				WHEN outcome.atkcfl_val_dropdown=85003 THEN 'All Children Closed'
				WHEN outcome.atkcfl_val_dropdown=85004 THEN 'Not Set'
				ELSE outcome.atkcfl_val_dropdown 
END	AS outcome
		FROM 
			(
SELECT	atkc_case_id
			FROM pp_access_views.dw_atk_case_general
			WHERE atkc_wkfl_id=42) cases
			
			LEFT JOIN 
				(
SELECT	*
				FROM pp_access_views.dw_atk_case_field_value
				WHERE atkcfl_field_id=17008
				) outcome
				ON cases.atkc_case_id=outcome.atkcfl_case_id
		) parent_case_data
		ON parent_case.atkcfl_val_ullong=parent_case_data.atkc_case_id
WHERE	orig_txn.atkcfl_val_ullong>0
	AND orig_txn.atkcfl_val_ullong IS NOT NULL
QUALIFY	ROW_NUMBER() OVER (PARTITION BY orig_txn.atkcfl_val_ullong 
ORDER	BY atkc_close_date DESC)=1
) 
WITH	DATA UNIQUE PRIMARY INDEX (child_case_id)
	INDEX (original_transaction)
	INDEX (temp_credit_transid)
;
--drop table pp_scratch_gba.tdavis1_ltm_afr;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_afr AS
(
SELECT	cases.atkc_case_id AS case_id
	,trxn.atkcfl_val_ullong AS trans_id
	,
CASE	WHEN reason.atkcfl_val_dropdown=39001 THEN 'None'
		WHEN reason.atkcfl_val_dropdown=39002 THEN 'Suspicious Credit Card Use'
		WHEN reason.atkcfl_val_dropdown=39003 THEN 'Suspicious Bank Account Use'
		WHEN reason.atkcfl_val_dropdown=39004 THEN 'Suspicious Balance Payment'
		WHEN reason.atkcfl_val_dropdown=39005 THEN 'Probable Not as Described Seller'
		WHEN reason.atkcfl_val_dropdown=39006 THEN 'Probable Non-Receipt Seller'
		WHEN reason.atkcfl_val_dropdown=39007 THEN 'Probable Unauthorized Account Access'
		WHEN reason.atkcfl_val_dropdown=39008 THEN 'Confirmed Stolen Credit Card'
		WHEN reason.atkcfl_val_dropdown=39009 THEN 'Confirmed Stolen Bank Account'
		WHEN reason.atkcfl_val_dropdown=39010 THEN 'Confirmed Other' 
			ELSE reason.atkcfl_val_dropdown 
END	AS reversal_reason
FROM	
	(
SELECT	atkc_case_id
		,atkc_close_date
	FROM pp_access_views.dw_atk_case_general
	WHERE atkc_wkfl_id=46) cases

	LEFT JOIN 
		(
SELECT	*
		FROM pp_access_views.dw_atk_case_field_value
		WHERE atkcfl_field_id=24001
		) trxn
		ON cases.atkc_case_id=trxn.atkcfl_case_id
	LEFT JOIN
		(
SELECT	*
		FROM pp_access_views.dw_atk_case_field_value
		WHERE atkcfl_field_id=24002
		) reason
		ON cases.atkc_case_id=reason.atkcfl_case_id
QUALIFY	ROW_NUMBER() OVER (PARTITION BY trxn.atkcfl_val_ullong 
ORDER	BY atkc_close_date DESC)=1
) 
WITH	DATA UNIQUE PRIMARY INDEX (case_id)
	UNIQUE INDEX (trans_id)
;
--drop table pp_scratch_gba.tdavis1_ltm_ato_payout;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_ato_payout AS
(
SELECT	parent_transid AS payment_transid
	,SUM(
CASE	WHEN ZEROIFNULL(transaction_usd_equiv_amt)>0 
		THEN ZEROIFNULL(CAST(transaction_usd_equiv_amt AS DECIMAL(18,
		2)))/100 
ELSE	0 
END	) AS ato_payouts
	,SUM(
CASE	WHEN ZEROIFNULL(transaction_usd_equiv_amt)<0 
		THEN ZEROIFNULL(CAST(transaction_usd_equiv_amt AS DECIMAL(18,
		2)))/100 
ELSE	0 
END	) AS ato_recoveries
FROM	pp_access_views.dw_earned_deposit
WHERE	transaction_type='N'
	AND transaction_status_reason IN ('O','T')
GROUP	BY 1
)
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);

--DROP TABLE  pp_scratch_gba.tdavis1_ltm_ach_payout;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_ach_payout AS
(
SELECT	parent_transid AS payment_transid
	,SUM(
CASE	WHEN ZEROIFNULL(transaction_usd_equiv_amt)>0 
		THEN ZEROIFNULL(CAST(transaction_usd_equiv_amt AS DECIMAL(18,
		2)))/100 
ELSE	0 
END	) AS ach_payouts
	,SUM(
CASE	WHEN ZEROIFNULL(transaction_usd_equiv_amt)<0 
		THEN ZEROIFNULL(CAST(transaction_usd_equiv_amt AS DECIMAL(18,
		2)))/100 
ELSE	0 
END	) AS ach_recoveries
FROM	pp_access_views.dw_earned_deposit
WHERE	transaction_type='N'
	AND transaction_status_reason='V'
GROUP	BY 1
)
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);
--drop table pp_scratch_gba.tdavis1_ltm_pf;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_pf AS
(
SELECT	lts.payment_transid
	,lts.sender_id
	,dwpf.visitor_id
	,dwpf.actor_ip
FROM	pp_scratch_gba.tdavis1_ltm_trans lts
	LEFT JOIN pp_risk_views.dw_payment_flow dwpf
		ON lts.payment_transid=dwpf.trans_id
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);
--drop table pp_scratch_gba.tdavis1_ltm_vids;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_vids AS 
(
SELECT	lpf.payment_transid
	,lpf.visitor_id
	,dwvi.visitor_crtd_ts
	,dwvi.visitor_crtd_date
FROM	pp_scratch_gba.tdavis1_ltm_pf lpf
	LEFT JOIN pp_access_views.dw_visitor_info dwvi
		ON lpf.sender_id=dwvi.cust_id 
	AND	lpf.visitor_id=dwvi.visitor_id
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);
--drop table pp_scratch_gba.tdavis1_ltm_ips;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_ips AS 
(
SELECT	lpf.payment_transid
	,lpf.actor_ip
	,dwim.first_ts
	,dwim.first_date
FROM	pp_scratch_gba.tdavis1_ltm_pf lpf
	LEFT JOIN pp_access_views.dw_ip_map dwim
		ON lpf.sender_id=dwim.cust_id 
	AND	lpf.actor_ip=dwim.ip_address
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);
--drop table pp_scratch_gba.tdavis1_ltm_ato_rest;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_ato_rest AS
(
SELECT	lts.payment_transid
	,MAX(
CASE	WHEN dwur.urt_date-lts.transaction_created_date>0
			AND dwur.urt_date-lts.transaction_created_date<=7
			AND rt_type_id IN   (65,63,66,33,61,49,91,12 , 52)
			THEN 1 
ELSE	0 
END	) AS ato_rest
FROM	pp_scratch_gba.tdavis1_ltm_trans lts
	LEFT JOIN pp_access_views.dw_user_restriction dwur
		ON lts.sender_id=dwur.customer_id
GROUP	BY 1
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);

COLLECT	STATISTICS PP_SCRATCH_GBA.tdavis1_ltm_trans COLUMN     SENDER_ID;
--DROP TABLE  vk_Ato_flag;
CREATE	TABLE vk_Ato_flag AS (
SELECT		lts.payment_transid,
					lts.unauth_exists,
	lts.merch_exists,
	lts.nsf_exists,
	lts.pp_category,
			CASE 
	WHEN	ato.outcome='Accepted' 
	AND	lts.pp_category='Unauth' THEN 'ATO'
		WHEN lssa.tpv=0 THEN 'B'
		WHEN (lssb.bad_tpv/lssa.tpv)>=0.95 THEN 'B' 
		WHEN ((lts.transaction_created_date-dwc.customer_created_date>30
				AND lts.transaction_created_date-lv.visitor_crtd_date<3
				AND lts.transaction_created_date-li.first_date<3
				AND lts.primary_flow<>'MS PF Subscription' 
	AND	lts.primary_flow<>'MS PF Pre-Approved Payments' 
				AND lts.buyer_type NOT LIKE 'Guest%' 
	AND	lts.flow_family<>'MS FF Website Payments Pro' 
				AND lts.flow_family<>'MS FF Virtual Terminal'
				AND lts.flow_family<>'MS FF eBay Express' 
	AND	lts.flow_family<>'MS FF EC A La Carte'
			)
			OR afr.reversal_reason='Probable Unauthorized Account Access'
			OR (ato.outcome<>'Denied')
			OR lar.ato_rest=1
			OR lts.spoof_flag=1
			) 
	AND	lts.pp_category='Unauth'
			THEN 'ATO'
		ELSE 'U' 
END	AS ATO_tag
FROM	tdavis1_ltm_trans lts
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_ato ato
		ON lts.payment_transid=ato.original_transaction
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_sender_sum_sum1 lssa
		ON lts.sender_id=lssa.sender_id
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_sender_sum_sum2 lssb
		ON lts.sender_id=lssb.sender_id
LEFT JOIN pp_scratch_gba.tdavis1_ltm_afr afr
		ON lts.payment_transid=afr.trans_id	
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_ato_rest lar
		ON lts.payment_transid=lar.payment_transid	
	LEFT JOIN pp_access_views.dw_customer dwc
		ON lts.sender_id=dwc.customer_id
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_vids lv
		ON lts.payment_transid=lv.payment_transid
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_ips li
		ON lts.payment_transid=li.payment_transid
)
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);

--drop  table pp_scratch_gba.tdavis1_ltm_sender_sum_sum_2;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_sender_sum_sum_2 AS 
(
SELECT	lts.sender_id
	,SUM(ZEROIFNULL(lts.trans_amt_usd)) AS bad_tpv
	,SUM(ZEROIFNULL(
CASE	WHEN lts.unauth_exists=1 
	AND	ATO_tag <> 'ATO'
		THEN lts.trans_amt_usd 
ELSE	0 
END	)) AS unauth_tpv
	,SUM(ZEROIFNULL(
CASE	WHEN lts.merch_exists=1
		THEN lts.trans_amt_usd 
ELSE	0 
END	)) AS merch_tpv
	,SUM(ZEROIFNULL(
CASE	WHEN lts.nsf_exists=1
		THEN lts.trans_amt_usd 
ELSE	0 
END	)) AS nsf_tpv
FROM	pp_scratch_gba.tdavis1_ltm_trans lts
LEFT JOIN vk_Ato_flag b
	ON	lts.payment_transid = b.payment_transid
GROUP	BY 1
) 
WITH	DATA UNIQUE PRIMARY INDEX (sender_id);

--drop table pp_scratch_gba.tdavis1_ltm_sender_sum_sum_3;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_sender_sum_sum_3 AS 
(
SELECT	lts.sender_id
	,SUM(ZEROIFNULL(lts.paypal_net_loss_amt_usd)) AS net_loss 
	,SUM(ZEROIFNULL(
CASE	WHEN lts.pp_category = 'Unauth' 
	AND	ATO_tag <> 'ATO'
		THEN lts.paypal_net_loss_amt_usd 
ELSE	0 
END	)) AS unauth_netloss
	,SUM(ZEROIFNULL(
CASE	WHEN lts.pp_category='Merchant'
		THEN lts.paypal_net_loss_amt_usd 
ELSE	0 
END	)) AS merch_netloss
	,SUM(ZEROIFNULL(
CASE	WHEN lts.pp_category='NSF'
		THEN lts.paypal_net_loss_amt_usd 
ELSE	0 
END	)) AS nsf_netloss
			,SUM(ZEROIFNULL(
CASE	WHEN lts.pp_category= 'Highrisk'
		THEN lts.paypal_net_loss_amt_usd 
ELSE	0 
END	)) AS highrisk_netloss
FROM	pp_scratch_gba.tdavis1_ltm_trans lts
LEFT JOIN vk_Ato_flag b
	ON	lts.payment_transid = b.payment_transid
GROUP	BY 1
) 
WITH	DATA UNIQUE PRIMARY INDEX (sender_id);


COLLECT	STATISTICS PP_SCRATCH_GBA.tdavis1_ltm_trans COLUMN     TRANSACTION_CREATED_DATE;
--DROP TABLE vk_lmt_trans;
CREATE	TABLE vk_lmt_trans AS (
SELECT	*
FROM	tdavis1_ltm_trans
WHERE	transaction_created_date >= '2009-01-01'
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);
--drop table pp_scratch_gba.tdavis1_ltm_tags_add_0;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_tags_add_0 AS
(
SEL	
	a.payment_transid
	, 
CASE	WHEN card_usage =1 THEN 'CC'
	    WHEN card_usage=2 THEN 'DC' 
ELSE	card_usage 
END	AS DCorCC
FROM	
	(
SEL	payment_transid FROM pp_scratch_gba.vk_lmt_trans 
WHERE	funding_source='CC') a
LEFT JOIN
	pp_access_views.dw_cc_transaction b
	ON	
	a.payment_transid=b.payment_transid
LEFT JOIN
	pp_access_views.dw_credit_card c
	ON	
	b.cc_id=c.cc_id
UNION	
SEL	
payment_transid,
'' AS DCorCC
FROM	pp_scratch_gba.vk_lmt_trans 
WHERE	
funding_source<>'CC'
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);
--drop table pp_scratch_gba.tdavis1_ltm_tags0
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_tags0 AS
(
SELECT	lts.payment_transid
	,lts.month_math
	,EXTRACT(MONTH FROM lts.transaction_created_date)+EXTRACT(YEAR FROM lts.transaction_created_date)*100 AS month_
	,lts.sender_id
	,lts.receiver_id
	,lts.transaction_created_date
	,lts.counterparty_category
	,lts.pp_category
	,lts.pp_manifestation
	,lts.seller_category
	,lts.seller_manifestation
	,lts.buyer_category
	,lts.buyer_manifestation
	,lts.pp_sub_category
	,lts.Dispute_Reason
	,lts.primary_flow
	,lts.flow_family
	,DCC_VT_ACTUAL
	,sender_tof
	,receiver_tof
	,DCorCC
	,cc_Ctrl_Grp_Type
	,iACH_Ctrl_Grp_Type
	,lts.buyer_type
	,lts.unauth_exists
	,lts.merch_exists
	,lts.nsf_exists
	,lts.paypal_net_loss_amt_usd
	,lts.buyer_net_loss
	,lts.seller_net_loss
	,lts.pp_net_nb_loss_amt_usd
	,lts.pp_net_dp_loss_amt_usd
	,lap.ato_payouts
	,lap.ato_recoveries
	,ach.ach_payouts
	,ach.ach_recoveries	
	,lv.visitor_crtd_date
	,li.first_date
	,afr.reversal_reason
	,lar.ato_rest
	,lts.spoof_flag
	,gross_loss_agent
	,gross_loss_user
	,gross_loss_ach
	,gross_loss_cb
	,risk_cat_agent
	,risk_cat_cb
	,risk_cat_ach
	,risk_cat_user
	,lts.ASP_bucket
	,lts.OnOff_EBay
	,
CASE	WHEN funding_source='ME' THEN 'mEFT'
	 
	WHEN funding_source='EL' THEN 'ELV '
	WHEN funding_source='UA' THEN 'uACH'
	WHEN funding_source='BC' THEN 'PPBC'
	WHEN funding_source='EC' THEN 'eCheck'	
 	WHEN funding_source='CC' AND	DCorCC='CC' THEN 'CC'	
 	WHEN funding_source='CC' AND	DCorCC='DC' THEN 'DC'
 	WHEN funding_source='CC' THEN 'CC Other'	
	WHEN funding_source='IA' THEN 'iACH'
	WHEN funding_source='IE' THEN 'iEFT'
	ELSE 'Balance' 
END	funding_source1
	,
CASE	WHEN Funding_Source1 IN ('iEFT', 'mEFT', 'ELV', 'uACH', 'iACH','eCheck') THEN 'BA' 
ELSE	Funding_Source1 
END	AS Funding_Source2
	,lts.intra_cross
	,lts.receiver_geography
	,lts.sender_geography
	,lts.flow_from_country
	,lts.flow_to_country
FROM	pp_scratch_gba.vk_lmt_trans lts
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_ato_payout lap
		ON lts.payment_transid=lap.payment_transid
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_ach_payout ach
		ON lts.payment_transid=ach.payment_transid
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_vids lv
		ON lts.payment_transid=lv.payment_transid
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_ips 	li
		ON lts.payment_transid=li.payment_transid
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_afr 	afr
		ON lts.payment_transid=afr.trans_id
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_ato_rest lar
		ON lts.payment_transid=lar.payment_transid
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_tags_add_0 Ad
	ON lts.payment_transid=AD.payment_transid
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);

COLLECT	STATISTICS pp_scratch_gba.tdavis1_ltm_tags0 COLUMN SENDER_ID;
--drop table  pp_scratch_gba.tdavis1_ltm_tags1a;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_tags1a AS
(
SELECT	lts.*,
	ato.outcome,
	dwc.customer_created_date
FROM	pp_scratch_gba.tdavis1_ltm_tags0 lts
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_ato ato
		ON lts.payment_transid=ato.original_transaction
	LEFT JOIN pp_access_views.dw_customer 	dwc
		ON lts.sender_id=dwc.customer_id
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);

COLLECT	STATISTICS pp_scratch_gba.tdavis1_ltm_tags1a COLUMN RECEIVER_ID;

--drop table  pp_scratch_gba.tdavis1_ltm_tags1b;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_tags1b AS
(
SELECT	lts.*,
		lnl.unauth_nb,
		lnl.merch_nb,
		lnl.nsf_nb
FROM	pp_scratch_gba.tdavis1_ltm_tags1a  lts
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_nb_losses lnl
		ON lts.receiver_id=lnl.receiver_id
WHERE	transaction_created_date <'2010-01-01'	
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);

--drop table  pp_scratch_gba.tdavis1_ltm_tags1c;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_tags1c AS
(
SELECT	lts.*,
		lnl.unauth_nb,
		lnl.merch_nb,
		lnl.nsf_nb
FROM	pp_scratch_gba.tdavis1_ltm_tags1a  lts
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_nb_losses lnl
		ON lts.receiver_id=lnl.receiver_id
WHERE	transaction_created_date  >='2010-01-01'
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);

--drop table pp_scratch_gba.tdavis1_ltm_tags1c ;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_tags1 AS
(
SELECT	* 
FROM	pp_scratch_gba.tdavis1_ltm_tags1b
UNION	
SELECT	* 
FROM	pp_scratch_gba.tdavis1_ltm_tags1c
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);

--SEL	TOP 100 * FROM	pp_scratch_gba.tdavis1_ltm_tags1 ;

COLLECT	STATISTICS pp_scratch_gba.tdavis1_ltm_tags1 COLUMN     RECEIVER_ID;
--drop table pp_scratch_gba.tdavis1_ltm_tags2a1;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_tags2a1 AS
(
SELECT	lts.*,
ZEROIFNULL(lrs.tpv) AS  receiver_tpv
FROM	pp_scratch_gba.tdavis1_ltm_tags1 lts
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_receiver_sum_sum lrs
		ON lts.receiver_id=lrs.receiver_id
		WHERE transaction_created_date <'2010-01-01'
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);
--drop table pp_scratch_gba.tdavis1_ltm_tags2a2;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_tags2a2 AS
(
SELECT	lts.*,
ZEROIFNULL(lrs.tpv) AS  receiver_tpv
FROM	pp_scratch_gba.tdavis1_ltm_tags1 lts
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_receiver_sum_sum lrs
		ON lts.receiver_id=lrs.receiver_id
		WHERE transaction_created_date >='2010-01-01'
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);
--drop table pp_scratch_gba.tdavis1_ltm_tags2a ;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_tags2a AS
(
SELECT	* 
FROM	tdavis1_ltm_tags2a1
UNION	
SELECT	* 
FROM	tdavis1_ltm_tags2a2
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);

COLLECT	STATISTICS pp_scratch_gba.tdavis1_ltm_tags2a COLUMN     sender_id;
--drop table pp_scratch_gba.tdavis1_ltm_tags2;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_tags2 AS
(
SELECT	lts.*,
ZEROIFNULL(lssa.tpv)  AS  sender_tpv
FROM	pp_scratch_gba.tdavis1_ltm_tags2a lts
	LEFT JOIN pp_scratch_gba.tdavis1_ltm_sender_sum_sum1 lssa
		ON lts.sender_id=lssa.sender_id
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);

COLLECT	STATISTICS pp_scratch_gba.tdavis1_ltm_tags2 COLUMN     SENDER_ID;
--drop table pp_scratch_gba.tdavis1_ltm_tags3a;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_tags3a AS
(
SELECT	ltx.*,
		lssb.bad_tpv AS sndr_bad_tpv,
		lssb.merch_tpv,
		lssb.unauth_tpv
FROM	pp_scratch_gba.tdavis1_ltm_tags2 ltx
LEFT JOIN pp_scratch_gba.tdavis1_ltm_sender_sum_sum_2 lssb
		ON ltx.sender_id=lssb.sender_id
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);

COLLECT	STATISTICS pp_scratch_gba.tdavis1_ltm_tags3a COLUMN     receiver_id;
--drop table pp_scratch_gba.tdavis1_ltm_tags3b;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_tags3b AS
(
SELECT	ltx.*,
		lrsb.bad_tpv
FROM	pp_scratch_gba.tdavis1_ltm_tags3a ltx
LEFT JOIN pp_scratch_gba.tdavis1_ltm_receiver_sum_sum1 lrsb
		ON ltx.receiver_id=lrsb.receiver_id
WHERE	transaction_created_date <'2010-01-01'
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);

COLLECT	STATISTICS pp_scratch_gba.tdavis1_ltm_tags3a COLUMN TRANSACTION_CREATED_DATE;
--drop table  pp_scratch_gba.tdavis1_ltm_tags3c;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_tags3c AS
(
SELECT	ltx.*,
		lrsb.bad_tpv
FROM	pp_scratch_gba.tdavis1_ltm_tags3a ltx
LEFT JOIN pp_scratch_gba.tdavis1_ltm_receiver_sum_sum1 lrsb
		ON ltx.receiver_id=lrsb.receiver_id
WHERE	transaction_created_date >= '2010-01-01'
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);
--drop table pp_scratch_gba.tdavis1_ltm_tags3 ;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_tags3 AS
(
SELECT	* 
FROM	pp_scratch_gba.tdavis1_ltm_tags3c
UNION	
SELECT	* 
FROM	pp_scratch_gba.tdavis1_ltm_tags3b
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);

COLLECT	STATISTICS pp_scratch_gba.tdavis1_ltm_tags3 COLUMN     RECEIVER_ID;
COLLECT	STATISTICS pp_scratch_gba.tdavis1_ltm_tags3 COLUMN     SENDER_ID;


--DROP table pp_scratch_gba.tdavis1_ltm_tags_test_new;
CREATE	TABLE pp_scratch_gba.tdavis1_ltm_tags_test_new AS
(
SELECT	 ltx.payment_transid
	,ltx.month_math
	,ltx.month_
	,ltx.sender_id
	,ltx.receiver_id
	,ltx.transaction_created_date
	,ltx.counterparty_category
	,ltx.pp_category
	,ltx.pp_manifestation
	,ltx.seller_category
	,ltx.seller_manifestation
	,ltx.buyer_category
	,ltx.buyer_manifestation
	,ltx.pp_sub_category
	,ltx.Dispute_Reason
	,ltx.primary_flow
	,ltx.flow_family
	,DCC_VT_ACTUAL
	,sender_tof
	,receiver_tof
	,DCorCC
	,cc_Ctrl_Grp_Type
	,iACH_Ctrl_Grp_Type
	,ltx.buyer_type
	,ltx.unauth_exists
	,ltx.merch_exists
	,ltx.nsf_exists
	,ltx.paypal_net_loss_amt_usd
	,ltx.buyer_net_loss
	,ltx.seller_net_loss
	,ltx.pp_net_nb_loss_amt_usd
	,ltx.pp_net_dp_loss_amt_usd
	,ltx.ato_payouts
	,ltx.ato_recoveries
	,ltx.ach_payouts
	,ltx.ach_recoveries	
	,ltx.visitor_crtd_date
	,ltx.first_date
	,ltx.reversal_reason
	,ltx.ato_rest
	,ltx.spoof_flag
	,gross_loss_agent
	,gross_loss_user
	,gross_loss_ach
	,gross_loss_cb
	,risk_cat_agent
	,risk_cat_cb
	,risk_cat_ach
	,risk_cat_user
	,ltx.ASP_bucket
	,ltx.OnOff_EBay
	,ltx.Funding_Source1
	,ltx.Funding_Source2
	,ltx.intra_cross
	,ltx.receiver_geography
	,ltx.sender_geography
	,ltx.flow_from_country
	,ltx.flow_to_country
	,
CASE	WHEN ltx.reversal_reason='Probable Unauthorized Account Access' THEN 1 
ELSE	0 
END	AS admin_ato_reversal
	,
CASE	WHEN (ltx.transaction_created_date-ltx.customer_created_date>30 
	AND	ltx.transaction_created_date-ltx.visitor_crtd_date<3 
			AND ltx.transaction_created_date-ltx.first_date<3 
	AND	ltx.primary_flow<>'MS PF Subscription' 
	AND	ltx.primary_flow<>'MS PF Pre-Approved Payments' 
				AND ltx.buyer_type NOT LIKE 'Guest%' 
	AND	ltx.flow_family<>'MS FF Website Payments Pro' 
	AND	ltx.flow_family<>'MS FF Virtual Terminal'
				AND ltx.flow_family<>'MS FF eBay Express' 
	AND	ltx.flow_family<>'MS FF EC A La Carte' ) THEN 1
		ELSE 0 
END	AS ATO_mo
		,ltx.outcome
		
,
CASE	WHEN receiver_tpv=0 THEN 'B'
		WHEN (bad_tpv)/(receiver_tpv)>=0.95 THEN 'B' 
		WHEN ltx.unauth_nb>-0.01 
	AND	ltx.merch_nb>-0.01 
	AND	ltx.nsf_nb>-0.01 THEN 'G'
		ELSE 'U' 
END	AS receiver_total_tag95
	,
CASE	WHEN ltx.outcome='Accepted' 
	AND	ltx.pp_category='Unauth' THEN 'ATO'
		WHEN sender_tpv=0 THEN 'B'
		WHEN (sndr_bad_tpv/sender_tpv)>=0.95 THEN 'B' 
		WHEN ((ltx.transaction_created_date-ltx.customer_created_date>30
				AND ltx.transaction_created_date-ltx.visitor_crtd_date<3
				AND ltx.transaction_created_date-ltx.first_date<3
				AND ltx.primary_flow<>'MS PF Subscription' 
	AND	ltx.primary_flow<>'MS PF Pre-Approved Payments' 
				AND ltx.buyer_type NOT LIKE 'Guest%' 
	AND	ltx.flow_family<>'MS FF Website Payments Pro' 
				AND ltx.flow_family<>'MS FF Virtual Terminal'
				AND ltx.flow_family<>'MS FF eBay Express' 
	AND	ltx.flow_family<>'MS FF EC A La Carte'
			)
			OR ltx.reversal_reason='Probable Unauthorized Account Access'
			OR (ltx.outcome<>'Denied')
			OR ltx.ato_rest=1
			OR ltx.spoof_flag=1
			) 
	AND	ltx.pp_category='Unauth'
			THEN 'ATO'
		WHEN (merch_tpv/sender_tpv)<=0.05 
	AND	unauth_tpv=0 
	AND	lssc.nsf_netloss=0 
	AND	lssc.highrisk_netloss = 0 THEN 'G'
		ELSE 'U' 
END	AS sender_total_tag95
		
,
CASE	WHEN receiver_tpv=0 THEN 'B'
		WHEN (bad_tpv)/(receiver_tpv)>=0.80 THEN 'B' 
		WHEN ltx.unauth_nb>-0.01 
	AND	ltx.merch_nb>-0.01 
	AND	ltx.nsf_nb>-0.01 THEN 'G'
		ELSE 'U' 
END	AS receiver_total_tag80
	,
CASE	WHEN ltx.outcome='Accepted' 
	AND	ltx.pp_category='Unauth' THEN 'ATO'
		WHEN sender_tpv=0 THEN 'B'
		WHEN (sndr_bad_tpv/sender_tpv)>=0.80 THEN 'B' 
		WHEN ((ltx.transaction_created_date-ltx.customer_created_date>30
				AND ltx.transaction_created_date-ltx.visitor_crtd_date<3
				AND ltx.transaction_created_date-ltx.first_date<3
				AND ltx.primary_flow<>'MS PF Subscription' 
	AND	ltx.primary_flow<>'MS PF Pre-Approved Payments' 
				AND ltx.buyer_type NOT LIKE 'Guest%' 
	AND	ltx.flow_family<>'MS FF Website Payments Pro' 
				AND ltx.flow_family<>'MS FF Virtual Terminal'
				AND ltx.flow_family<>'MS FF eBay Express' 
	AND	ltx.flow_family<>'MS FF EC A La Carte'
			)
			OR ltx.reversal_reason='Probable Unauthorized Account Access'
			OR (ltx.outcome<>'Denied')
			OR ltx.ato_rest=1
			OR ltx.spoof_flag=1
			) 
	AND	ltx.pp_category='Unauth'
			THEN 'ATO'
		WHEN (merch_tpv/sender_tpv)<=0.05 
	AND	unauth_tpv=0 
	AND	lssc.nsf_netloss=0 
	AND	lssc.highrisk_netloss = 0 THEN 'G'
		ELSE 'U' 
END	AS sender_total_tag80		
		
		,
CASE	WHEN  ltx.outcome='Accepted' 
	AND	ltx.pp_category='Unauth' THEN 'Confirmed ATO'
							WHEN ((ltx.transaction_created_date-ltx.customer_created_date>30
									AND ltx.transaction_created_date-ltx.visitor_crtd_date<3
									AND ltx.transaction_created_date-ltx.first_date<3
									AND ltx.primary_flow<>'MS PF Subscription' 
	AND	ltx.primary_flow<>'MS PF Pre-Approved Payments' 
									AND ltx.buyer_type NOT LIKE 'Guest%' 
	AND	ltx.flow_family<>'MS FF Website Payments Pro' 
									AND ltx.flow_family<>'MS FF Virtual Terminal'
									AND ltx.flow_family<>'MS FF eBay Express' 
	AND	ltx.flow_family<>'MS FF EC A La Carte'
								)
								OR ltx.reversal_reason='Probable Unauthorized Account Access'
								OR (ltx.outcome<>'Denied')
								OR ltx.ato_rest=1
								OR ltx.spoof_flag=1
								) 
	AND	ltx.pp_category='Unauth'
								THEN 'Suspected ATO' 
END	AS ATO_C_S
			
,
CASE	WHEN receiver_tpv=0 THEN 'B'
		WHEN (bad_tpv)/(receiver_tpv)>=0.50 THEN 'B' 
		WHEN ltx.unauth_nb>-0.01 
	AND	ltx.merch_nb>-0.01 
	AND	ltx.nsf_nb>-0.01 THEN 'G'
		ELSE 'U' 
END	AS receiver_total_tag50
	,
CASE	WHEN ltx.outcome='Accepted' 
	AND	ltx.pp_category='Unauth' THEN 'ATO'
		WHEN sender_tpv=0 THEN 'B'
		WHEN (sndr_bad_tpv/sender_tpv)>=0.50 THEN 'B' 
		WHEN ((ltx.transaction_created_date-ltx.customer_created_date>30
				AND ltx.transaction_created_date-ltx.visitor_crtd_date<3
				AND ltx.transaction_created_date-ltx.first_date<3
				AND ltx.primary_flow<>'MS PF Subscription' 
	AND	ltx.primary_flow<>'MS PF Pre-Approved Payments' 
				AND ltx.buyer_type NOT LIKE 'Guest%' 
	AND	ltx.flow_family<>'MS FF Website Payments Pro' 
				AND ltx.flow_family<>'MS FF Virtual Terminal'
				AND ltx.flow_family<>'MS FF eBay Express' 
	AND	ltx.flow_family<>'MS FF EC A La Carte'
			)
			OR ltx.reversal_reason='Probable Unauthorized Account Access'
			OR (ltx.outcome<>'Denied')
			OR ltx.ato_rest=1
			OR ltx.spoof_flag=1
			) 
	AND	ltx.pp_category='Unauth'
			THEN 'ATO'
		WHEN (merch_tpv/sender_tpv)<=0.05 
	AND	unauth_tpv=0 
	AND	lssc.nsf_netloss=0 
	AND	lssc.highrisk_netloss = 0 THEN 'G'
		ELSE 'U' 
END	AS sender_total_tag50		
		
,
CASE	WHEN receiver_tpv=0 THEN 'B'
		WHEN (bad_tpv)/(receiver_tpv)>=0.30 THEN 'B' 
		WHEN ltx.unauth_nb>-0.01 
	AND	ltx.merch_nb>-0.01 
	AND	ltx.nsf_nb>-0.01 THEN 'G'
		ELSE 'U' 
END	AS receiver_total_tag30
	,
CASE	WHEN ltx.outcome='Accepted' 
	AND	ltx.pp_category='Unauth' THEN 'ATO'
		WHEN sender_tpv=0 THEN 'B'
		WHEN (sndr_bad_tpv/sender_tpv)>=0.30 THEN 'B' 
		WHEN ((ltx.transaction_created_date-ltx.customer_created_date>30
				AND ltx.transaction_created_date-ltx.visitor_crtd_date<3
				AND ltx.transaction_created_date-ltx.first_date<3
				AND ltx.primary_flow<>'MS PF Subscription' 
	AND	ltx.primary_flow<>'MS PF Pre-Approved Payments' 
				AND ltx.buyer_type NOT LIKE 'Guest%' 
	AND	ltx.flow_family<>'MS FF Website Payments Pro' 
				AND ltx.flow_family<>'MS FF Virtual Terminal'
				AND ltx.flow_family<>'MS FF eBay Express' 
	AND	ltx.flow_family<>'MS FF EC A La Carte'
			)
			OR ltx.reversal_reason='Probable Unauthorized Account Access'
			OR (ltx.outcome<>'Denied')
			OR ltx.ato_rest=1
			OR ltx.spoof_flag=1
			) 
	AND	ltx.pp_category='Unauth'
			THEN 'ATO'
		WHEN (merch_tpv/sender_tpv) <= 0.05 
	AND	unauth_tpv=0 
	AND	lssc.nsf_netloss=0 
	AND	lssc.highrisk_netloss = 0 THEN 'G'
		ELSE 'U' 
END	AS sender_total_tag30		
	

FROM	pp_scratch_gba.tdavis1_ltm_tags3 ltx
		LEFT JOIN pp_scratch_gba.tdavis1_ltm_sender_sum_sum_3 lssc
		ON ltx.sender_id=lssc.sender_id
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);

/*
drop table tdavis1_ltm_tags0;
SEL TOP 100 * FROM vk_gross_catg;
create table vk_gross_catg as (
select a.*

	,case
		when tld.atkc_workflow_code='F'and tld.atkc_dispute_reason in (' ', 'Non-Receipt', 'Not as Described','Probable Not as Described Seller','Probable Non-Receipt Seller', 'Confirmed Marketplace Item Remov','Confirmed Other')
		then 'Merch'
		when tld.atkc_workflow_code='F'
		then 'Unauth'
		when tld.atkc_workflow_code is null and tld.pymt_reversal_reason_desc in ('Buyer Complaint','EBAY SPPP')
		then 'Merch'
		when tld.atkc_workflow_code is null and tld.pymt_reversal_reason_desc in ('Admin Reversal','Buyer Spoof')
		then 'Unauth'
		else 'OT'
	end 																										as risk_cat_agent1

	,case
		when tld.cbe_reason_group_desc='Unauth'
		then 'Unauth'
		when tld.cbe_reason_group_desc in ('Merch','Non_Receipt')
		then 'Merch'
		else 'OT'
	end 																										as risk_cat_cb1

	,case
		when tld.tfc_ar_reason_group_desc='fraud'
		then 'Unauth'
		when tld.tfc_ar_reason_group_desc='nsf'
		then 'NSF'
		else 'Highrisk'
	end 																										as risk_cat_ach1

	,case
		when tld.atkc_workflow_code in ('D','C')
		then 'Merch'
		when tld.atkc_workflow_code ='S'
		then 'Unauth'
		else 'OT'
	end 																										as risk_cat_user1
	
	,case
		when tld.atkc_workflow_code='F' and (zeroifnull(tld.pymt_reversal_adj_amt_usd)+zeroifnull(pymt_reversal_user_amt_usd))<-0.01
		then trans_amt_usd
		when tld.atkc_workflow_code is null and zeroifnull(tld.pymt_reversal_adj_amt_usd)<-0.01
		then trans_amt_usd
		else 0
	end 																									as gross_loss_agent1
	,CASE
		when tld.atkc_workflow_code in ('D','C','S')
		then tld.atkc_dispute_amt_usd
		else 0 
	end 																									AS gross_loss_user1

	,case
		when tld.ach_return_cnt >= 1
		then tld.ach_trans_amt_usd
		else 0
	end 																									AS gross_loss_ach1

	,case
		when tld.cbe_gross_amt_usd*-1>0
		then tld.cbe_gross_amt_usd*-1
		else 0
	end 																									AS gross_loss_cb1
	
	
 ,Case When tld.trans_amt_usd <=25 Then '$0-$25'
		when tld.trans_amt_usd <=50 Then '$25-$50'
		when tld.trans_amt_usd <=100 Then '$50-$100'
		when tld.trans_amt_usd <= 250 Then '$100-$250'
		when tld.trans_amt_usd <= 500 Then '$250-$500'
		when tld.trans_amt_usd <= 1000 Then '$500-$1K'
		when tld.trans_amt_usd <= 1500 Then '$1K-$1.5K'
		when tld.trans_amt_usd <= 2000 Then '$1.5K-$2K'
		when tld.trans_amt_usd <= 2500 Then '$2K-$2.5K'
		when tld.trans_amt_usd <= 3000 Then '$2.5-$3K'
		else '>$3K'
	End ASP_bucket
,Case When	tld.transaction_subtype='I' Then 'On-Ebay'  Else	'Off-Ebay' 	End	As OnOff_EBay
,case when funding_source='ME' then 'mEFT'
	 when funding_source='EL' then 'ELV '
	when funding_source='UA' then 'uACH'
	when funding_source='OT' then 'Balance'
	when funding_source='BC' then 'PPBC'
	when funding_source='EC' then 'eCheck'	
 	when funding_source='CC' then 'CC'	
	when funding_source='IA' then 'iACH'
	when funding_source='IE' then 'iEFT'
	else 'Other' end as Funding_Source1	
	,case when Funding_Source1 in ('iEFT', 'mEFT', 'ELV', 'uACH', 'iACH', 'eCheck') then 'BA' else Funding_Source1 end as Funding_Source2
	,CASE 	WHEN	tld.flow_to_country = '99'  OR	 tld.flow_from_country = tld.flow_to_country THEN 'IB'
		ELSE	'XB' 	END as intra_cross
,case when tld.flow_to_country  = 'AT' then 'AT'
			when tld.flow_to_country in ('AU','NZ') then 'AU'
			when tld.flow_to_country = 'BE' then 'BE'
			when tld.flow_to_country = 'CA' then 'CA' 
			when tld.flow_to_country = 'CH' then 'CH' 
			when tld.flow_to_country in ('CN' , 'C2' ) then 'CN'
			when tld.flow_to_country = 'DE' then 'DE'
			when tld.flow_to_country = 'ES' then 'ES'
			when tld.flow_to_country = 'FR' then 'FR'
			when tld.flow_to_country = 'HK' then 'HK'
			when tld.flow_to_country = 'IT' then 'IT'
			when tld.flow_to_country = 'JP' then 'JP'
			when tld.flow_to_country = 'NL' then 'NL'
			when tld.flow_to_country = 'PL' then 'PL'
			when tld.flow_to_country in ('BD','BT','KH','IN','ID','KP','KR','LA','MO','MY','MV','FM','MN','NP','PK','PH','WS','LK','TH','TO','VN') then 'RAP'
			when tld.flow_to_country in ('BG','CY','CZ','DK','EE','FI','GR','HU','LV','LT','LU','MT','PT','RO','RS','SK','SI','SE') then 'REU'
			when tld.flow_to_country in ('AX','AL','AD','BA','HR','GG','IS','IM','LI','MK','MD','MC','NO','ME','RU','SM','CS','TR','UA','VA','YU') then 'RME'
			when tld.flow_to_country in ('AF','DZ','AS','AO','AI','AQ','AG','AR','AM','AW','AZ','BS','BH','BB','BY','BZ','BJ','BM','BO','BW','BV','BR','IO','BN','BF','BI','CM','CV','KY','CF','TD','CL','CX','CC','CO','KM','CD','CG','CK','CR','CI','CU','DJ','DM',
										'DO','TL','TP','EC','EG','SV','GQ','ER','ET','FK','FO','FJ','FX','GF','PF','TF','GA','GM','GE','GH','GI','GL','GD','GP','GU','GT','GN','GW','GY','HT','HM','HN','IR','IQ','IL','JM','JE','JO','KZ','KE','KI','KW','KG','LB'
										,'LS','LR','LY','MG','MW','ML','MH','MQ','MR','MU','YT','MX','MS','MA','MZ','MM','NA','NR','AN','NC','NI','NE','NG','NU','NF','MP','OM','PW','PS','PA','PG','PY','PE','PN','PR','QA','RE','RW','KN','LC','VC',
										'ST','SA','SN','SC','SL','SB','SO','ZA','GS','SH','PM','SD','SR','SJ','SZ','SY','TJ','TZ','TG','TK','TT','TN','TM','TC','TV','UG','AE','UM','UY','UZ','VU','VE','VG','VI','WF','EH','YE','ZM','ZW') then 'ROW'
			when tld.flow_to_country = 'SG' then 'SG'
			when tld.flow_to_country = 'TW' then 'TW'
			when tld.flow_to_country in ('IE' , 'GB') then 'UK'
			when tld.flow_to_country in ('-1', 'US' , '99') then 'US'
			else 'not_defined'
			end as receiver_geography
			
,case when tld.flow_from_country  = 'AT' then 'AT'
			when tld.flow_from_country in ('AU','NZ') then 'AU'
			when tld.flow_from_country = 'BE' then 'BE'
			when tld.flow_from_country = 'CA' then 'CA' 
			when tld.flow_from_country = 'CH' then 'CH' 
			when tld.flow_from_country in ('CN' , 'C2' ) then 'CN'
			when tld.flow_from_country = 'DE' then 'DE'
			when tld.flow_from_country = 'ES' then 'ES'
			when tld.flow_from_country = 'FR' then 'FR'
			when tld.flow_from_country = 'HK' then 'HK'
			when tld.flow_from_country = 'IT' then 'IT'
			when tld.flow_from_country = 'JP' then 'JP'
			when tld.flow_from_country = 'NL' then 'NL'
			when tld.flow_from_country = 'PL' then 'PL'
			when tld.flow_from_country in ('BD','BT','KH','IN','ID','KP','KR','LA','MO','MY','MV','FM','MN','NP','PK','PH','WS','LK','TH','TO','VN') then 'RAP'
			when tld.flow_from_country in ('BG','CY','CZ','DK','EE','FI','GR','HU','LV','LT','LU','MT','PT','RO','RS','SK','SI','SE') then 'REU'
			when tld.flow_from_country in ('AX','AL','AD','BA','HR','GG','IS','IM','LI','MK','MD','MC','NO','ME','RU','SM','CS','TR','UA','VA','YU') then 'RME'
			when tld.flow_from_country in ('AF','DZ','AS','AO','AI','AQ','AG','AR','AM','AW','AZ','BS','BH','BB','BY','BZ','BJ','BM','BO','BW','BV','BR','IO','BN','BF','BI','CM','CV','KY','CF','TD','CL','CX','CC','CO','KM','CD','CG','CK','CR','CI','CU','DJ','DM',
										'DO','TL','TP','EC','EG','SV','GQ','ER','ET','FK','FO','FJ','FX','GF','PF','TF','GA','GM','GE','GH','GI','GL','GD','GP','GU','GT','GN','GW','GY','HT','HM','HN','IR','IQ','IL','JM','JE','JO','KZ','KE','KI','KW','KG','LB'
										,'LS','LR','LY','MG','MW','ML','MH','MQ','MR','MU','YT','MX','MS','MA','MZ','MM','NA','NR','AN','NC','NI','NE','NG','NU','NF','MP','OM','PW','PS','PA','PG','PY','PE','PN','PR','QA','RE','RW','KN','LC','VC',
										'ST','SA','SN','SC','SL','SB','SO','ZA','GS','SH','PM','SD','SR','SJ','SZ','SY','TJ','TZ','TG','TK','TT','TN','TM','TC','TV','UG','AE','UM','UY','UZ','VU','VE','VG','VI','WF','EH','YE','ZM','ZW') then 'ROW'
			when tld.flow_from_country = 'SG' then 'SG'
			when tld.flow_from_country = 'TW' then 'TW'
			when tld.flow_from_country in ('IE' , 'GB') then 'UK'
			when tld.flow_from_country in ('-1', 'US' , '99') then 'US'
			else 'not_defined'
			end as sender_geography
		
from tdavis1_ltm_tags_test_new a 
left join pp_access_views.tld_negative_payment tld
on a.payment_transid = tld.payment_transid
) WITH DATA UNIQUE PRIMARY INDEX (payment_transid); 
*/

--SEL TOP 1000 * FROM vk_tagging_gross_logic ;
COLLECT	STATISTICS pp_scratch_gba.tdavis1_ltm_tags_test_new       COLUMN risk_cat_ach;
COLLECT	STATISTICS pp_scratch_gba.tdavis1_ltm_tags_test_new     COLUMN PP_CATEGORY;
COLLECT	STATISTICS pp_scratch_gba.tdavis1_ltm_tags_test_new      COLUMN risk_cat_user;
COLLECT	STATISTICS pp_scratch_gba.tdavis1_ltm_tags_test_new     COLUMN risk_cat_agent;
COLLECT	STATISTICS pp_scratch_gba.tdavis1_ltm_tags_test_new     COLUMN risk_cat_cb;
---drop table vk_tagging_gross_logic ;
CREATE	TABLE vk_tagging_gross_logic AS (
SELECT	
	 ltx.payment_transid
	,ltx.month_
	,ltx.sender_id
	,ltx.receiver_id
	,ASP_bucket
	,OnOff_EBay
	,Funding_Source1
	,
CASE	WHEN Funding_Source2 IN ('CC','DC','CC Other') THEN 'CC' 
ELSE	Funding_Source2 
END	AS Funding_Source2
	,intra_cross
	,receiver_geography
	,sender_geography
	,flow_from_country
	,flow_to_country
	,ltx.transaction_created_date
	,ltx.counterparty_category
	,
CASE	WHEN risk_cat_ach = 'Merch' THEN 'Merchant' 
	   
	WHEN	risk_cat_ach = 'OT' THEN 'Other' 
ELSE	risk_cat_ach 
END	 AS risk_category
	,
CASE	WHEN risk_category = 'Merchant' 
	AND	dispute_reason = 'INR' THEN 'INR'
      
	WHEN	risk_category = 'Merchant' 
	AND	dispute_reason = 'SNAD' THEN 'SNAD'
      
	WHEN	risk_category = 'Merchant'  THEN 'Merchant-Other'
      
	WHEN	risk_category = 'Unauth' 
	AND	sender_total_tag95='ATO'  
	AND	ATO_C_S='Suspected ATO' THEN 'Suspected ATO'
      
	WHEN	risk_category = 'Unauth' 
	AND	sender_total_tag95='ATO'  
	AND	ATO_C_S='Confirmed ATO' THEN 'Confirmed ATO'
       
	WHEN	risk_category = 'Unauth' 
	AND	sender_total_tag95='B'  
	AND	receiver_total_tag95 IN ('G','U','B')THEN 'Stolen Financials'
      
	WHEN	risk_category = 'Unauth' THEN 'Others'
     
ELSE	risk_category 
END	AS risk_sub_category
	,'ACH return' AS manifestation
	,sender_total_tag95
	,receiver_total_tag95
	,ltx.primary_flow
	,ltx.flow_family
	,ltx.buyer_type
	,DCC_VT_ACTUAL
	,sender_tof
	,receiver_tof
	,cc_Ctrl_Grp_Type
	,iACH_Ctrl_Grp_Type
	,queued
	,dismissed
	,denied
	,restricted
	,CAST(0 AS FLOAT) AS paypal_net_loss
	,CAST(0 AS FLOAT) AS pp_net_nb_loss
	,CAST(0 AS FLOAT) AS pp_net_dp_loss
	,gross_loss_ach AS gross_loss
	,1 AS gross
FROM	pp_scratch_gba.tdavis1_ltm_tags_test_new  ltx

UNION	

SELECT	
 ltx.payment_transid
	,ltx.month_
	,ltx.sender_id
	,ltx.receiver_id
	,ASP_bucket
	,OnOff_EBay
	,Funding_Source1
	,
CASE	WHEN Funding_Source2 IN ('CC','DC','CC Other') THEN 'CC' 
ELSE	Funding_Source2 
END	AS Funding_Source2
	,intra_cross
	,receiver_geography
	,sender_geography
	,flow_from_country
	,flow_to_country
	,ltx.transaction_created_date
	,ltx.counterparty_category
	,
CASE	WHEN risk_cat_cb = 'Merch' THEN 'Merchant' 
		WHEN risk_cat_cb = 'OT' THEN 'Other' 
ELSE	risk_cat_cb 
END	 AS risk_category	
	,
CASE	WHEN risk_category = 'Merchant' 
	AND	dispute_reason = 'INR' THEN 'INR'
      
	WHEN	risk_category = 'Merchant' 
	AND	dispute_reason = 'SNAD' THEN 'SNAD'
      
	WHEN	risk_category = 'Merchant'  THEN 'Merchant-Other'
      
	WHEN	risk_category = 'Unauth' 
	AND	sender_total_tag95='ATO'  
	AND	ATO_C_S='Suspected ATO' THEN 'Suspected ATO'
      
	WHEN	risk_category = 'Unauth' 
	AND	sender_total_tag95='ATO'  
	AND	ATO_C_S='Confirmed ATO' THEN 'Confirmed ATO'
       
	WHEN	risk_category = 'Unauth' 
	AND	sender_total_tag95='B'  
	AND	receiver_total_tag95 IN ('G','U','B')THEN 'Stolen Financials'
      
	WHEN	risk_category = 'Unauth' THEN 'Others'
     
ELSE	risk_category 
END	AS risk_sub_category
	,'Chargeback' AS manifestation
		,sender_total_tag95
	,receiver_total_tag95
	,ltx.primary_flow
	,ltx.flow_family
	,ltx.buyer_type
	,DCC_VT_ACTUAL
	,sender_tof
	,receiver_tof
	,cc_Ctrl_Grp_Type
	,iACH_Ctrl_Grp_Type
		,queued
	,dismissed
	,denied
	,restricted
,CAST(0 AS FLOAT) AS paypal_net_loss
	,CAST(0 AS FLOAT) AS pp_net_nb_loss
	,CAST(0 AS FLOAT) AS pp_net_dp_loss

	,gross_loss_cb AS gross_loss
	,1 AS gross
FROM	pp_scratch_gba.tdavis1_ltm_tags_test_new  ltx

UNION	

SELECT	
 ltx.payment_transid
	,ltx.month_
	,ltx.sender_id
	,ltx.receiver_id
	,ASP_bucket
	,OnOff_EBay
	,Funding_Source1
	,
CASE	WHEN Funding_Source2 IN ('CC','DC','CC Other') THEN 'CC' 
ELSE	Funding_Source2 
END	AS Funding_Source2
	,intra_cross
	,receiver_geography
	,sender_geography
	,flow_from_country
	,flow_to_country
	,ltx.transaction_created_date
	,ltx.counterparty_category
	,
CASE	WHEN risk_cat_agent = 'Merch' THEN 'Merchant'
		WHEN risk_cat_agent = 'OT' THEN 'Other'	ELSE risk_cat_agent 
END	 AS risk_category	
	,
CASE	WHEN risk_category = 'Merchant' 
	AND	dispute_reason = 'INR' THEN 'INR'
      
	WHEN	risk_category = 'Merchant' 
	AND	dispute_reason = 'SNAD' THEN 'SNAD'
      
	WHEN	risk_category = 'Merchant'  THEN 'Merchant-Other'
      
	WHEN	risk_category = 'Unauth' 
	AND	sender_total_tag95='ATO'  
	AND	ATO_C_S='Suspected ATO' THEN 'Suspected ATO'
      
	WHEN	risk_category = 'Unauth' 
	AND	sender_total_tag95='ATO'  
	AND	ATO_C_S='Confirmed ATO' THEN 'Confirmed ATO'
      
	WHEN	risk_category = 'Unauth' 
	AND	sender_total_tag95='B'  
	AND	receiver_total_tag95 IN ('G','U','B')THEN 'Stolen Financials'
      
	WHEN	risk_category = 'Unauth' THEN 'Others'
     
ELSE	risk_category 
END	AS risk_sub_category
	,'Agent Action' AS manifestation
		,sender_total_tag95
	,receiver_total_tag95
	,ltx.primary_flow
	,ltx.flow_family
	,ltx.buyer_type
	,DCC_VT_ACTUAL
	,sender_tof
	,receiver_tof
	,cc_Ctrl_Grp_Type
	,iACH_Ctrl_Grp_Type
		,queued
	,dismissed
	,denied
	,restricted
,CAST(0 AS FLOAT) AS paypal_net_loss
	,CAST(0 AS FLOAT) AS pp_net_nb_loss
	,CAST(0 AS FLOAT) AS pp_net_dp_loss

	,gross_loss_agent AS gross_loss
	,1 AS gross
FROM	pp_scratch_gba.tdavis1_ltm_tags_test_new  ltx

UNION	

SELECT	
 ltx.payment_transid
	,ltx.month_
	,ltx.sender_id
	,ltx.receiver_id
	,ASP_bucket
	,OnOff_EBay
	,Funding_Source1
	,
CASE	WHEN Funding_Source2 IN ('CC','DC','CC Other') THEN 'CC' 
ELSE	Funding_Source2 
END	AS Funding_Source2
	,intra_cross
	,receiver_geography
	,sender_geography
	,flow_from_country
	,flow_to_country
	,ltx.transaction_created_date
	,ltx.counterparty_category
	,
CASE	WHEN risk_cat_user = 'Merch' THEN 'Merchant'
		WHEN risk_cat_user = 'OT' THEN 'Other'
ELSE	risk_cat_user 
END	 AS risk_category
	,
CASE	WHEN risk_category = 'Merchant' 
	AND	dispute_reason = 'INR' THEN 'INR'
      
	WHEN	risk_category = 'Merchant' 
	AND	dispute_reason = 'SNAD' THEN 'SNAD'
      
	WHEN	risk_category = 'Merchant'  THEN 'Merchant-Other'
      
	WHEN	risk_category = 'Unauth' 
	AND	sender_total_tag95='ATO'  
	AND	ATO_C_S='Suspected ATO' THEN 'Suspected ATO'
      
	WHEN	risk_category = 'Unauth' 
	AND	sender_total_tag95='ATO'  
	AND	ATO_C_S='Confirmed ATO' THEN 'Confirmed ATO'
      
	WHEN	risk_category = 'Unauth' 
	AND	sender_total_tag95='B'  
	AND	receiver_total_tag95 IN ('G','U','B')THEN 'Stolen Financials'
      
	WHEN	risk_category = 'Unauth' THEN 'Others'
     
ELSE	risk_category 
END	AS risk_sub_category
	,'User' AS manifestation
		,sender_total_tag95
	,receiver_total_tag95
	,ltx.primary_flow
	,ltx.flow_family
	,ltx.buyer_type
	,DCC_VT_ACTUAL
	,sender_tof
	,receiver_tof
	,cc_Ctrl_Grp_Type
	,iACH_Ctrl_Grp_Type
		,queued
	,dismissed
	,denied
	,restricted
,CAST(0 AS FLOAT) AS paypal_net_loss
	,CAST(0 AS FLOAT) AS pp_net_nb_loss
	,CAST(0 AS FLOAT) AS pp_net_dp_loss

	,gross_loss_user AS gross_loss
	,1 AS gross
FROM	pp_scratch_gba.tdavis1_ltm_tags_test_new  ltx

UNION	

SELECT	
 ltx.payment_transid
	,ltx.month_
	,ltx.sender_id
	,ltx.receiver_id
	,ASP_bucket
	,OnOff_EBay
	,Funding_Source1
	,
CASE	WHEN Funding_Source2 IN ('CC','DC','CC Other') THEN 'CC' 
ELSE	Funding_Source2 
END	AS Funding_Source2
	,intra_cross
	,receiver_geography
	,sender_geography
	,flow_from_country
	,flow_to_country
	,ltx.transaction_created_date
	,ltx.counterparty_category
	,pp_category AS risk_category
	,
CASE	WHEN pp_sub_category = 'Unauth' 
	AND	sender_total_tag95='ATO'  
	AND	ATO_C_S='Suspected ATO' THEN 'Suspected ATO'
      				WHEN pp_sub_category= 'Unauth' 
	AND	sender_total_tag95='ATO'  
	AND	ATO_C_S='Confirmed ATO' THEN 'Confirmed ATO'
     					 
	WHEN	pp_sub_category = 'Unauth' 
	AND	sender_total_tag95='B'  
	AND	receiver_total_tag95 IN ('G','U','B')THEN 'Stolen Financials'
      				WHEN pp_sub_category = 'Unauth' THEN 'Others'
    					 
ELSE	pp_sub_category 
END	AS risk_sub_category
	,pp_manifestation AS manifestation
	,sender_total_tag95
	,receiver_total_tag95
	,ltx.primary_flow
	,ltx.flow_family
	,ltx.buyer_type
	,DCC_VT_ACTUAL
	,sender_tof
	,receiver_tof
	,cc_Ctrl_Grp_Type
	,iACH_Ctrl_Grp_Type
		,queued
	,dismissed
	,denied
	,restricted
	,paypal_net_loss_amt_usd AS paypal_net_loss
	,pp_net_nb_loss_amt_usd AS pp_net_nb_loss
	,pp_net_dp_loss_amt_usd AS pp_net_dp_loss
	,0.0 AS gross_loss
	,0 AS gross
FROM	pp_scratch_gba.tdavis1_ltm_tags_test_new  ltx
WHERE	pp_category IS NOT NULL

)
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid, gross, manifestation);

------------------------------End of table creation-----------------------------------------------------------------------------
/*
drop table vk_tagging_Unauth_data;
create table vk_tagging_Unauth_data as (

select 
	 ltx.payment_transid
	 ,ltx.outcome
	,ltx.month_
	,ltx.sender_id
	,ltx.receiver_id
	,ASP_bucket
	,OnOff_EBay
	,Funding_Source2
	,intra_cross
	,receiver_geography
	,sender_geography
	,ltx.transaction_created_date
	,ltx.counterparty_category
	,case when risk_cat_ach1 = 'Merch' then 'Merchant' 
	   when risk_cat_ach1 = 'OT' then 'Other' else risk_cat_ach1 end  as risk_category		
	,'ACH return' as manifestation
	,sender_total_tag95
	,receiver_total_tag95
	,ltx.primary_flow
	,ltx.flow_family
	,ltx.buyer_type
	,cast(0 as float) as paypal_net_loss
	,cast(0 as float) as pp_net_nb_loss
	,cast(0 as float) as pp_net_dp_loss
	,gross_loss_ach1 as gross_loss
	,1 as gross
from vk_gross_catg ltx
where risk_cat_ach1='Unauth'
union

select 
 ltx.payment_transid
 ,ltx.outcome
	,ltx.month_
	,ltx.sender_id
	,ltx.receiver_id
	,ASP_bucket
	,OnOff_EBay
	,Funding_Source2
	,intra_cross
	,receiver_geography
	,sender_geography
	,ltx.transaction_created_date
	,ltx.counterparty_category
	,case when risk_cat_cb1 = 'Merch' then 'Merchant' 
		when risk_cat_cb1 = 'OT' then 'Other' else risk_cat_cb1 end  as risk_category	
	,'Chargeback' as manifestation
		,sender_total_tag95
	,receiver_total_tag95
	,ltx.primary_flow
	,ltx.flow_family
	,ltx.buyer_type
,cast(0 as float) as paypal_net_loss
	,cast(0 as float) as pp_net_nb_loss
	,cast(0 as float) as pp_net_dp_loss

	,gross_loss_cb1 as gross_loss
	,1 as gross
from vk_gross_catg ltx
where risk_cat_cb1='Unauth'
union

select 
 ltx.payment_transid
 ,ltx.outcome
	,ltx.month_
	,ltx.sender_id
	,ltx.receiver_id
	,ASP_bucket
	,OnOff_EBay
	,Funding_Source2
	,intra_cross
	,receiver_geography
	,sender_geography
	,ltx.transaction_created_date
	,ltx.counterparty_category
	,case when risk_cat_agent1 = 'Merch' then 'Merchant'
		when risk_cat_agent1 = 'OT' then 'Other'	else risk_cat_agent1 end  as risk_category	
	,'Agent Action' as manifestation
		,sender_total_tag95
	,receiver_total_tag95
	,ltx.primary_flow
	,ltx.flow_family
	,ltx.buyer_type
,cast(0 as float) as paypal_net_loss
	,cast(0 as float) as pp_net_nb_loss
	,cast(0 as float) as pp_net_dp_loss

	,gross_loss_agent1 as gross_loss
	,1 as gross
from vk_gross_catg ltx
where risk_cat_agent1='Unauth'
union

select 
 ltx.payment_transid
 ,ltx.outcome
	,ltx.month_
	,ltx.sender_id
	,ltx.receiver_id
	,ASP_bucket
	,OnOff_EBay
	,Funding_Source2
	,intra_cross
	,receiver_geography
	,sender_geography
	,ltx.transaction_created_date
	,ltx.counterparty_category
	,case when risk_cat_user1 = 'Merch' then 'Merchant'
		when risk_cat_user1 = 'OT' then 'Other'else risk_cat_user1 end  as risk_category
	,'User' as manifestation
		,sender_total_tag95
	,receiver_total_tag95
	,ltx.primary_flow
	,ltx.flow_family
	,ltx.buyer_type
,cast(0 as float) as paypal_net_loss
	,cast(0 as float) as pp_net_nb_loss
	,cast(0 as float) as pp_net_dp_loss

	,gross_loss_user1 as gross_loss
	,1 as gross
from vk_gross_catg ltx
where risk_cat_user1='Unauth'

union

select 
 ltx.payment_transid
 ,ltx.outcome
	,ltx.month_
	,ltx.sender_id
	,ltx.receiver_id
	,ASP_bucket
	,OnOff_EBay
	,Funding_Source2
	,intra_cross
	,receiver_geography
	,sender_geography
	,ltx.transaction_created_date
	,ltx.counterparty_category
	,pp_category as risk_category
	,pp_manifestation as manifestation
	,sender_total_tag95
	,receiver_total_tag95
	,ltx.primary_flow
	,ltx.flow_family
	,ltx.buyer_type
	,paypal_net_loss_amt_usd as paypal_net_loss
	,pp_net_nb_loss_amt_usd as pp_net_nb_loss
	,pp_net_dp_loss_amt_usd as pp_net_dp_loss
	,0.0 as gross_loss
	,0 as gross
from vk_gross_catg ltx
where pp_category ='Unauth'

)with data unique primary index (payment_transid, gross, manifestation);



create table vk_unauth_new_final as (
select a.*
				, case when counterparty_category in (97,99) then 'Y' else 'N' end as cat_97_99
				,case when sender_total_tag95='ATO' then 'ATO'
							when receiver_total_tag95='ATO' then 'ATO'
							when sender_total_tag95='B' then 'Stolen Financials'
							when sender_total_tag95='U' then 'Deception'
							when sender_total_tag95='B' and receiver_total_tag95='B' then 'Stolen Financials'
					else 'Other' end as classification
				,case when sender_total_tag95='B' then 'Stolen Financials'
							when sender_total_tag95='ATO'	and a.outcome ='Accepted' then 'Confirmed ATO'
							when sender_total_tag95='ATO'	then 'Suspected ATO'
							when sender_total_tag95='U'and ( a.outcome = 'Denied' ) 	then 'Buyer Abuse' when sender_total_tag95='U'	then 'Uncategorized'
					else 'Other' end as sub_classification
from 	vk_tagging_Unauth_data a
)with data unique primary index  (payment_transid, gross, manifestation);




drop table zst_Qdata_Subset ;
create table zst_Qdata_Subset as(
select fq.FRAUD_DATA_ID
        , fq.FQ_ID
        , fq.CUST_ID
        , fq.FRAUD_DATA_COMMENTS
        , fq.FRAUD_DATA_CRTD_TS
        , fq.FRAUD_DATA_CRTD_DATE
        , fq.FRAUD_DATA_STATUS
        , fq.FRAUD_DATA_MODEL_NAME
        , fq.FRAUD_TRANS_LIKE_ID
        , fsr.FQ_SUSPCT_RSN_DESC
        , fq.FRAUD_ACTN_TS
        , fsr.FRAUD_SUSPCT_CREATOR_MODEL
        , fql.fql_actn_type
        , case when fql.fql_actn_type in ('R','L') then 1 else 0 end restricted
        , case when fql.fql_actn_type='D' then 1 else 0 end dismissed
        , case when fql.fql_actn_type='Q' then 1 else 0 end queued
        , case when fql.fql_actn_type='E' then 1 else 0 end expired
from pp_access_views.dw_fraud_data fq
        left join pp_access_views.dw_fraud_suspect_reason_new fsr
                on fsr.fq_suspct_rsn_id = fq.fq_suspct_rsn_id
        left join (select * from pp_access_views.dw_fraud_queue_log where fql_actn_type in ('R','L','D','Q','E')) fql
                on fq.fq_id=fql.fql_fq_id
where fq.FRAUD_DATA_CRTD_DATE >= '2008-12-01'
        and FQ_SUSPCT_RSN_DESC not like 'SVC%'
) with data unique primary index(FRAUD_DATA_ID, fql_actn_type);


drop table vk_Qdata_TS;
create table vk_Qdata_TS as(
select FRAUD_TRANS_LIKE_ID
        , max(fqt.restricted) as T_rest
        , max(fqt.dismissed) as T_dism
        , max(fqt.queued) as T_qd
        , max(fqt.expired) as T_exp
from  zst_Qdata_Subset fqt
where FRAUD_TRANS_LIKE_ID is not null
group by 1
) with data unique primary index (FRAUD_TRANS_LIKE_ID);

-- diagnostic helpstats on for session;
-- diagnostic helpstats on for session;

COLLECT STATISTICS PP_SCRATCH_GBA.vk_tagging_gross_logic COLUMN     MONTH_;
COLLECT STATISTICS PP_SCRATCH_GBA.vk_tagging_gross_logic COLUMN     PAYMENT_TRANSID;
select risk_category,
				case when t_rest > 0 then 'restricted'
					when t_dism > 0 then 'dismissed'
					when t_qd>0 or t_exp >0 then 'queued not reviewed'
				else 'not queued' end as qed_rvwd,
				Funding_Source2,
				case when risk_category = 'Unauth' and sender_total_tag95='ATO' and receiver_total_tag95 = 'B' then 'Collusion'
						when risk_category = 'Unauth' and sender_total_tag95='ATO' then 'ATO'
						when risk_category = 'Unauth' and  receiver_total_tag95='ATO' then 'ATO'
						when risk_category = 'Unauth' and sender_total_tag95='B' and receiver_total_tag95 = 'B' then 'Collusion'
						when risk_category = 'Unauth' and sender_total_tag95='B' then 'Stolen Financials'
						when risk_category = 'Unauth' and sender_total_tag95='U' then 'Other'
						when risk_category = 'Unauth' then 'Other'
						when risk_category in ('NSF' , 'Highrisk') and sender_total_tag95 in ('G' , 'U') then 'Sender Credit'
						when risk_category in ('NSF' , 'Highrisk') and sender_total_tag95 ='B' and  receiver_total_tag95 in ('G', 'U' ) then 'Sender Fraud'
						when risk_category in ('NSF' , 'Highrisk') and sender_total_tag95='B'  and receiver_total_tag95='B' then 'Collusion'
						when risk_category in ('NSF' , 'Highrisk') then 'Other'
				else 'Other' end as MOs,
				month_,
				case when counterparty_category  in (97,99) then 'Y' else 'N' end as cat_97_99,
				sum(paypal_net_loss) as paypal_net_loss,
				sum(gross_loss) as gross_loss
	from vk_tagging_gross_logic a		
	left join vk_Qdata_TS b
	on a.payment_transid =b.FRAUD_TRANS_LIKE_ID
	where month_ >= '200901'
	group by 1,2,3,4,5,6;
	*/
-- diagnostic helpstats on for session;
-- drop table vk_tagging_dataset;
-- diagnostic helpstats on for session;   

--COLLECT STATISTICS PP_SCRATCH_GBA.vk_tagging_gross_logic COLUMN PAYMENT_TRANSID;
--DROP TABLE vk_tagging_dataset_temp;
/*
CREATE TABLE vk_tagging_dataset_temp AS (
select a.*,
				case when funding_source='ME' then 'mEFT'
						 when funding_source='EL' then 'ELV '
						when funding_source='UA' then 'uACH'
						when funding_source='OT' then 'Balance'
						when funding_source='BC' then 'PPBC'
						when funding_source='EC' then 'eCheck'	
 						WHEN funding_source='CC' AND (DCorCC='CC'  OR  DCorCC IS NULL) THEN 'CC'	
 						WHEN funding_source='CC' AND DCorCC='DC' THEN 'DC'	
						WHEN funding_source='IA' THEN 'iACH'
						when funding_source='IE' then 'iEFT'
					else 'Balance' end as Funding_Source1,
					flow_to_country
from vk_tagging_gross_logic a
left join pp_access_views.tld_negative_payment b
on a.payment_transid= b.payment_transid
)with data unique primary index (payment_transid, gross, manifestation);
SEL TOP 1000 * FROM vk_tagging_dataset_temp;
*/

/********check direct payout**********************************************
*********************************************************************************
***********************************************************************************/
COLLECT	STATISTICS PP_SCRATCH_GBA.vk_tagging_gross_logic COLUMN FLOW_TO_COUNTRY;
--DROP TABLE vk_tagging_dataset;
CREATE	TABLE vk_tagging_dataset AS (
	SELECT month_,
					CASE 
	WHEN	ASP_bucket IN ( '$0-$25', '$25-$50') THEN '$0-$50'
							WHEN ASP_bucket = '$50-$100' THEN '$50-$100'
							WHEN ASP_bucket = '$100-$250' THEN '$100-$250'
							WHEN ASP_bucket = '$250-$500' THEN '$250-$500'
								WHEN ASP_bucket = '$500-$1K' THEN '$500-$1K'
							WHEN ASP_bucket IN ( '$1K-$1.5K' ,  '$1.5K-$2K'  ) THEN '$1K-$2K'
							WHEN ASP_bucket IN ( '$2K-$2.5K' ,  '$2.5K-$3K'  )  THEN '$2K-$3K'
					ELSE '>$3K'
					END ASP_bucket,
					OnOff_EBay,
					sender_total_tag95,
					receiver_total_tag95,
					Funding_Source1,
					Funding_Source2,
					intra_cross,
					DCC_VT_ACTUAL,
					PPREV_ROLLUP1 AS receiver_geography,
					CASE 
	WHEN	sender_geography = 'US' THEN 'US' 
ELSE	'Intl' 
END	AS sender_geography,
					risk_category,
					risk_sub_category,
					manifestation,
					flow_family,
					CASE 
	WHEN	risk_category = 'Unauth' 
	AND	sender_total_tag95='ATO' 
	AND	receiver_total_tag95 = 'B' THEN 'UCF:Collusion'
						WHEN risk_category = 'Unauth' 
	AND	sender_total_tag95='ATO' THEN 'USF:ATO'
						WHEN risk_category = 'Unauth' 
	AND	 receiver_total_tag95='ATO' THEN 'USF:ATO'
						WHEN risk_category = 'Unauth' 
	AND	sender_total_tag95='B' 
	AND	receiver_total_tag95 = 'B' THEN 'UCF:Collusion'
						WHEN risk_category = 'Unauth' 
	AND	sender_total_tag95='B' THEN 'USF:Stolen Financials'
						WHEN risk_category = 'Unauth' 
	AND	sender_total_tag95='U' THEN 'UOO:Other'
						WHEN risk_category = 'Unauth' THEN 'UOO:Other'
						WHEN risk_category = 'NSF' 
	AND	sender_total_tag95 IN ('G' , 'U') THEN 'NSC:Sender Credit'
						WHEN risk_category = 'NSF' 
	AND	sender_total_tag95 ='B' 
	AND	 receiver_total_tag95 IN ('G', 'U' ) THEN 'NSF:Sender Fraud'
						WHEN risk_category = 'NSF'  
	AND	sender_total_tag95='B'  
	AND	receiver_total_tag95='B' THEN 'NCF:Collusion'
						WHEN risk_category = 'NSF'  THEN 'NOO:Other'
						WHEN risk_category =   'Highrisk' 
	AND	sender_total_tag95 IN ('G' , 'U') THEN 'HSC:Sender Credit'
						WHEN risk_category =   'Highrisk'  
	AND	sender_total_tag95 ='B' 
	AND	 receiver_total_tag95 IN ('G', 'U' ) THEN 'HSF:Sender Fraud'
						WHEN risk_category =   'Highrisk'  
	AND	sender_total_tag95='B'  
	AND	receiver_total_tag95='B' THEN 'HCF:Collusion'
						WHEN risk_category =   'Highrisk'  THEN 'HOO:Other'
						WHEN risk_category = 'Merchant' 
	AND	sender_total_tag95 IN ('G', 'U' ) 
	AND	receiver_total_tag95 = 'G' THEN 'MSC:Friendly Fraud'
						WHEN risk_category = 'Merchant' 
	AND	sender_total_tag95 ='B' 
	AND	receiver_total_tag95 = 'B' THEN 'MCF:Collusion'
						WHEN risk_category = 'Merchant' 
	AND	sender_total_tag95 ='B'  THEN 'MSF:Buyer Fraud'
						WHEN risk_category = 'Merchant' 
	AND	receiver_total_tag95 ='B'  THEN 'MRF:Seller Fraud'
						WHEN risk_category = 'Merchant' 
	AND	receiver_total_tag95 ='U' 
	AND	sender_total_tag95 IN ('G', 'U' ) THEN 'MRC:Seller Credit'
						WHEN risk_category = 'Merchant' THEN 'MOO:Other'
				ELSE 'Other' 
END	AS MOs,
				CASE 
	WHEN	receiver_total_tag95 = 'B' 
	OR	sender_total_tag95 IN ('ATO', 'B' ) THEN 'Fraud' 
ELSE	'Credit' 
END	AS classification,
				SUM(gross_loss) AS gross_loss,
				SUM(
CASE	WHEN a.counterparty_category IN (99,97) THEN 0 
ELSE	paypal_net_loss 
END	) AS net_loss,
				SUM(
CASE	WHEN a.counterparty_category IN (99,97) THEN 0 
ELSE	pp_net_nb_loss 
END	) AS net_nb_loss,
				--SUM(CASE WHEN a.counterparty_category IN (99,97) THEN 0 ELSE pp_net_dp_loss END) AS net_dp_loss
				SUM(
CASE	WHEN a.gross_loss> 0.01 
	AND	gross=1 THEN 1 
ELSE	0 
END	) AS gross_cnt,
				SUM(
CASE	WHEN paypal_net_loss>0.01 
	AND	gross=0 THEN 1 
ELSE	0 
END	) AS net_cnt,
				ZEROIFNULL(net_loss) -  ZEROIFNULL(net_nb_loss) AS net_dp_loss
FROM	vk_tagging_gross_logic a
LEFT JOIN fpa_region_definition c
	ON	a.flow_to_country = c.country_code
GROUP	BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)
WITH	DATA PRIMARY INDEX (month_, asp_bucket, onoff_ebay, funding_source1,
		funding_source2,
																										intra_cross, receiver_geography, sender_geography,
		risk_category,risk_sub_category);
																										
SEL	
month_,risk_category, classification,SUM(gross_loss),SUM(net_loss),
		SUM(net_nb_loss),SUM(net_dp_loss)
FROM	vk_tagging_dataset
GROUP	BY 
1,2,3
;

SEL	TOP 100 * 
FROM	vk_tagging_dataset;

DROP	TABLE vk_tagging_dataset_temp;

SELECT	COUNT(*) 
FROM	vk_tagging_dataset;
COUNT(*)
2821444
COUNT(*)
3016659

SEL	TOP 1000 * 
FROM	vk_tagging_dataset 
WHERE	funding_source1='DC';

DROP	TABLE pp_scratch_gba.VK_tagging_TPV_a;
CREATE	TABLE pp_scratch_gba.VK_tagging_TPV_a AS
(
SEL	
payment_transid,
cc_id
FROM	
 pp_access_views.dw_payment_sent_adhoc adhoc
WHERE	transaction_created_date >= '2009-01-01' 
	AND	ZEROIFNULL(cc_id) > 0
	AND	TRANSACTION_REFERENCE_STATUS IN ('S','OS','FV','PV')

        /* Fix USPS bug on PP site */
        
	AND	NOT ( COALESCE(adhoc.CUSTOMER_COUNTERPARTY,'0') = '2025189576733869774'
                
	AND	COALESCE(adhoc.TRANSACTION_CREATED_DATE, DATE '1969-12-31') BETWEEN DATE '2005-02-16' 
	AND	DATE '2005-03-23' )

        /* Payment was NOT flagged as going to CAT-99 merchant*/
        
	AND	ZEROIFNULL(adhoc.TRANSACTION_FLAG3) / 256 MOD 2 NOT = 1

        /* Receiver was not in cat-97 nor cat-99 */
        
	AND	ZEROIFNULL(COUNTERPARTY_CUST_CATEG) NOT IN (97,99)

        /* Not a USPS or Gift Certificate Loading Transaction */
        
	AND	COALESCE(adhoc.TRANSACTION_SUBTYPE,'#') NOT IN ('U', 'P')
) 
WITH	DATA UNIQUE PRIMARY INDEX(payment_transid);

SELECT	COUNT(*) 
FROM	pp_scratch_gba.VK_tagging_TPV_a;
COUNT(*)
1174724806
COUNT(*)
766398218
COUNT(*)
886464197


COLLECT	STATISTICS pp_scratch_gba.VK_tagging_TPV_a COLUMN CC_ID;
CREATE	TABLE pp_scratch_gba.VK_tagging_TPV_b AS
(
SELECT	
adhoc.payment_transid,
card_usage
FROM	pp_scratch_gba.VK_tagging_TPV_a   adhoc
LEFT JOIN
	pp_access_views.dw_credit_card d
	ON	
	adhoc.cc_id=d.cc_id
) 
WITH	DATA UNIQUE PRIMARY INDEX(payment_transid);

SEL	card_usage,COUNT(*) 
FROM	pp_scratch_gba.VK_tagging_TPV_b 
GROUP	BY 1;
card_usage	COUNT(*)
?	11
-99	1600173
-1	33274
0	51853712
1	701420822
2	419816814

CREATE	TABLE pp_scratch_gba.VK_tagging_TPV_c AS
(
SELECT	
CASE	
	   
	WHEN	((adhoc.transaction_flag4/16384) MOD 2) = 1 THEN 'iEFT '
	   
	WHEN	((adhoc.transaction_flag3/16777216) MOD 2) = 1 THEN 'mEFT'
	   
	WHEN	((adhoc.transaction_flag4/16) MOD 2) = 1 THEN 'ELV '
	   
	WHEN	((adhoc.transaction_flag3/32) MOD 2) = 1 THEN 'uACH'
	   
	WHEN	((adhoc.transaction_flag1/33554432) MOD 2) = 1 THEN 'iACH'
	   
	WHEN	((adhoc.transaction_flag1/67108864) MOD 2) = 1 THEN 'eCheck'
	   
	WHEN	((adhoc.transaction_flag3/134217728) MOD 2) = 1 THEN 'PPBC' 
	   
	WHEN	adhoc.cc_transid_y_n ='Y' AND	card_usage =1 THEN 'CC'
	   
	WHEN	adhoc.cc_transid_y_n ='Y' AND	card_usage=2 THEN 'DC'
	
	WHEN adhoc.cc_transid_y_n ='Y' THEN 'CC other'
	   
ELSE	'Balance' 
	   
END	funding_source1
,
CASE	WHEN Funding_Source1 IN ('iEFT', 'mEFT', 'ELV', 'uACH', 'iACH',
		'eCheck') THEN 'BA' 
					WHEN Funding_Source1 IN ('CC','DC','CC other') THEN 'CC'
					ELSE Funding_Source1 
END	AS Funding_Source2	   
,
CASE		WHEN	adhoc.flow_to_country = '99'  
	OR	 adhoc.flow_from_country = adhoc.flow_to_country THEN 'IB'		ELSE	'XB' 	END AS intra_cross
,
CASE	WHEN flow_from_country = 'US' THEN 'US' 
ELSE	'Intl' 
END	AS sender_geography
,		PPREV_ROLLUP1 AS receiver_geography

,	CASE
		WHEN	adhoc.transaction_usd_equiv_amt > -5000 THEN '$0-$50'
		WHEN	adhoc.transaction_usd_equiv_amt > -10000 THEN '$50-$100'
		WHEN	adhoc.transaction_usd_equiv_amt > -25000 THEN '$100-$250'
		WHEN	adhoc.transaction_usd_equiv_amt > -50000 THEN '$250-$500'
		WHEN	adhoc.transaction_usd_equiv_amt > -100000 THEN '$500-$1K'
		WHEN	adhoc.transaction_usd_equiv_amt > -200000 THEN '$1K-$2K'
		WHEN	adhoc.transaction_usd_equiv_amt > -300000 THEN '$2K-$3K'
		ELSE '>$3K'
	END ASP_bucket
	
,	CASE		WHEN	adhoc.transaction_subtype='I' THEN 'On-Ebay' 
ELSE	'Off-Ebay' 
END	AS OnOff_EBay
,	flow_family
,	CASE 
	WHEN	primary_flow = 'MS PF Virtual Terminal' THEN 'VT'
			WHEN primary_flow = 'MS PF Direct Payments' THEN 'DCC'
			ELSE 'Other' 
END	AS DCC_VT_ACTUAL
	,adhoc.transaction_created_date (FORMAT 'YYYY/MM') (CHAR(7)) AS month_
	,COUNT(adhoc.payment_transid) AS Txn_cnt
	,SUM( (TRANSACTION_REFERENCE_AMT)/CAST(-100 AS FLOAT)) AS ntpv
FROM	 pp_access_views.dw_payment_sent_adhoc adhoc
	LEFT JOIN cdim_payment_flow2 flow
	ON	adhoc.pmt_flow_key2=flow.pmt_flow_key2
LEFT JOIN fpa_region_definition c
	ON	adhoc.flow_to_country = c.country_code
LEFT JOIN
	pp_scratch_gba.VK_tagging_TPV_b b
	ON	
	adhoc.payment_transid=b.payment_transid
GROUP	BY 1,2,3,4,5,6,7,8,9,10
WHERE	
           adhoc.transaction_created_date BETWEEN '2009-01-01' AND '2009-06-30' --(Set the month you want to segment the merchant TPV by here)

	AND	TRANSACTION_REFERENCE_STATUS IN ('S','OS','FV','PV')

        /* Fix USPS bug on PP site */
        
	AND	NOT ( COALESCE(adhoc.CUSTOMER_COUNTERPARTY,'0') = '2025189576733869774'
                
	AND	COALESCE(adhoc.TRANSACTION_CREATED_DATE, DATE '1969-12-31') BETWEEN DATE '2005-02-16' 
	AND	DATE '2005-03-23' )

        /* Payment was NOT flagged as going to CAT-99 merchant*/
        
	AND	ZEROIFNULL(adhoc.TRANSACTION_FLAG3) / 256 MOD 2 NOT = 1

        /* Receiver was not in cat-97 nor cat-99 */
        
	AND	ZEROIFNULL(COUNTERPARTY_CUST_CATEG) NOT IN (97,99)

        /* Not a USPS or Gift Certificate Loading Transaction */
        
	AND	COALESCE(adhoc.TRANSACTION_SUBTYPE,'#') NOT IN ('U', 'P')
) 
WITH	DATA PRIMARY INDEX(funding_source1,funding_source2,intra_cross,
		Receiver_geography, Sender_geography, month_, flow_family,onoff_ebay);


CREATE	TABLE pp_scratch_gba.VK_tagging_TPV_d AS
(
SELECT	
CASE	
	   
	WHEN	((adhoc.transaction_flag4/16384) MOD 2) = 1 THEN 'iEFT '
	   
	WHEN	((adhoc.transaction_flag3/16777216) MOD 2) = 1 THEN 'mEFT'
	   
	WHEN	((adhoc.transaction_flag4/16) MOD 2) = 1 THEN 'ELV '
	   
	WHEN	((adhoc.transaction_flag3/32) MOD 2) = 1 THEN 'uACH'
	   
	WHEN	((adhoc.transaction_flag1/33554432) MOD 2) = 1 THEN 'iACH'
	   
	WHEN	((adhoc.transaction_flag1/67108864) MOD 2) = 1 THEN 'eCheck'
	   
	WHEN	((adhoc.transaction_flag3/134217728) MOD 2) = 1 THEN 'PPBC' 
	   
	WHEN	adhoc.cc_transid_y_n ='Y' AND	card_usage =1 THEN 'CC'
	   
	WHEN	adhoc.cc_transid_y_n ='Y' AND	card_usage=2 THEN 'DC'
	
	WHEN adhoc.cc_transid_y_n ='Y' THEN 'CC other'
	   
ELSE	'Balance' 
	   
END	funding_source1
,
CASE	WHEN Funding_Source1 IN ('iEFT', 'mEFT', 'ELV', 'uACH', 'iACH',
		'eCheck') THEN 'BA' 
					WHEN Funding_Source1 IN  ('CC','DC','CC other') THEN 'CC'
					ELSE Funding_Source1 
END	AS Funding_Source2	   
,
CASE		WHEN	adhoc.flow_to_country = '99'  
	OR	 adhoc.flow_from_country = adhoc.flow_to_country THEN 'IB'		ELSE	'XB' 	END AS intra_cross
,
CASE	WHEN flow_from_country = 'US' THEN 'US' 
ELSE	'Intl' 
END	AS sender_geography
,		PPREV_ROLLUP1 AS receiver_geography

,	CASE
		WHEN	adhoc.transaction_usd_equiv_amt > -5000 THEN '$0-$50'
		WHEN	adhoc.transaction_usd_equiv_amt > -10000 THEN '$50-$100'
		WHEN	adhoc.transaction_usd_equiv_amt > -25000 THEN '$100-$250'
		WHEN	adhoc.transaction_usd_equiv_amt > -50000 THEN '$250-$500'
		WHEN	adhoc.transaction_usd_equiv_amt > -100000 THEN '$500-$1K'
		WHEN	adhoc.transaction_usd_equiv_amt > -200000 THEN '$1K-$2K'
		WHEN	adhoc.transaction_usd_equiv_amt > -300000 THEN '$2K-$3K'
		ELSE '>$3K'
	END ASP_bucket
	
,	CASE		WHEN	adhoc.transaction_subtype='I' THEN 'On-Ebay' 
ELSE	'Off-Ebay' 
END	AS OnOff_EBay
,	flow_family
,	CASE 
	WHEN	primary_flow = 'MS PF Virtual Terminal' THEN 'VT'
			WHEN primary_flow = 'MS PF Direct Payments' THEN 'DCC'
			ELSE 'Other' 
END	AS DCC_VT_ACTUAL
	,adhoc.transaction_created_date (FORMAT 'YYYY/MM') (CHAR(7)) AS month_
	,COUNT(adhoc.payment_transid) AS Txn_cnt
	,SUM( (TRANSACTION_REFERENCE_AMT)/CAST(-100 AS FLOAT)) AS ntpv
FROM	 pp_access_views.dw_payment_sent_adhoc adhoc
	LEFT JOIN cdim_payment_flow2 flow
	ON	adhoc.pmt_flow_key2=flow.pmt_flow_key2
LEFT JOIN fpa_region_definition c
	ON	adhoc.flow_to_country = c.country_code
LEFT JOIN
	pp_scratch_gba.VK_tagging_TPV_b b
	ON	
	adhoc.payment_transid=b.payment_transid
GROUP	BY 1,2,3,4,5,6,7,8,9,10
WHERE	
           adhoc.transaction_created_date BETWEEN '2009-07-01' AND '2009-12-31'--(Set the month you want to segment the merchant TPV by here)

	AND	TRANSACTION_REFERENCE_STATUS IN ('S','OS','FV','PV')

        /* Fix USPS bug on PP site */
        
	AND	NOT ( COALESCE(adhoc.CUSTOMER_COUNTERPARTY,'0') = '2025189576733869774'
                
	AND	COALESCE(adhoc.TRANSACTION_CREATED_DATE, DATE '1969-12-31') BETWEEN DATE '2005-02-16' 
	AND	DATE '2005-03-23' )

        /* Payment was NOT flagged as going to CAT-99 merchant*/
        
	AND	ZEROIFNULL(adhoc.TRANSACTION_FLAG3) / 256 MOD 2 NOT = 1

        /* Receiver was not in cat-97 nor cat-99 */
        
	AND	ZEROIFNULL(COUNTERPARTY_CUST_CATEG) NOT IN (97,99)

        /* Not a USPS or Gift Certificate Loading Transaction */
        
	AND	COALESCE(adhoc.TRANSACTION_SUBTYPE,'#') NOT IN ('U', 'P')
) 
WITH	DATA PRIMARY INDEX(funding_source1,funding_source2,intra_cross,
		Receiver_geography, Sender_geography, month_, flow_family,onoff_ebay);


CREATE	TABLE pp_scratch_gba.VK_tagging_TPV_e AS
(
SELECT	
CASE	
	   
	WHEN	((adhoc.transaction_flag4/16384) MOD 2) = 1 THEN 'iEFT '
	   
	WHEN	((adhoc.transaction_flag3/16777216) MOD 2) = 1 THEN 'mEFT'
	   
	WHEN	((adhoc.transaction_flag4/16) MOD 2) = 1 THEN 'ELV '
	   
	WHEN	((adhoc.transaction_flag3/32) MOD 2) = 1 THEN 'uACH'
	   
	WHEN	((adhoc.transaction_flag1/33554432) MOD 2) = 1 THEN 'iACH'
	   
	WHEN	((adhoc.transaction_flag1/67108864) MOD 2) = 1 THEN 'eCheck'
	   
	WHEN	((adhoc.transaction_flag3/134217728) MOD 2) = 1 THEN 'PPBC' 
	   
	WHEN	adhoc.cc_transid_y_n ='Y' AND	card_usage =1 THEN 'CC'
	   
	WHEN	adhoc.cc_transid_y_n ='Y' AND	card_usage=2 THEN 'DC'
	
	WHEN adhoc.cc_transid_y_n ='Y' THEN 'CC other'
	   
ELSE	'Balance' 
	   
END	funding_source1
,
CASE	WHEN Funding_Source1 IN ('iEFT', 'mEFT', 'ELV', 'uACH', 'iACH',
		'eCheck') THEN 'BA' 
					WHEN Funding_Source1 IN  ('CC','DC','CC other') THEN 'CC'
					ELSE Funding_Source1 
END	AS Funding_Source2	   
,
CASE		WHEN	adhoc.flow_to_country = '99'  
	OR	 adhoc.flow_from_country = adhoc.flow_to_country THEN 'IB'		ELSE	'XB' 	END AS intra_cross
,
CASE	WHEN flow_from_country = 'US' THEN 'US' 
ELSE	'Intl' 
END	AS sender_geography
,		PPREV_ROLLUP1 AS receiver_geography

,	CASE
		WHEN	adhoc.transaction_usd_equiv_amt > -5000 THEN '$0-$50'
		WHEN	adhoc.transaction_usd_equiv_amt > -10000 THEN '$50-$100'
		WHEN	adhoc.transaction_usd_equiv_amt > -25000 THEN '$100-$250'
		WHEN	adhoc.transaction_usd_equiv_amt > -50000 THEN '$250-$500'
		WHEN	adhoc.transaction_usd_equiv_amt > -100000 THEN '$500-$1K'
		WHEN	adhoc.transaction_usd_equiv_amt > -200000 THEN '$1K-$2K'
		WHEN	adhoc.transaction_usd_equiv_amt > -300000 THEN '$2K-$3K'
		ELSE '>$3K'
	END ASP_bucket
	
,	CASE		WHEN	adhoc.transaction_subtype='I' THEN 'On-Ebay' 
ELSE	'Off-Ebay' 
END	AS OnOff_EBay
,	flow_family
,	CASE 
	WHEN	primary_flow = 'MS PF Virtual Terminal' THEN 'VT'
			WHEN primary_flow = 'MS PF Direct Payments' THEN 'DCC'
			ELSE 'Other' 
END	AS DCC_VT_ACTUAL
	,adhoc.transaction_created_date (FORMAT 'YYYY/MM') (CHAR(7)) AS month_
	,COUNT(adhoc.payment_transid) AS Txn_cnt
	,SUM( (TRANSACTION_REFERENCE_AMT)/CAST(-100 AS FLOAT)) AS ntpv
FROM	 pp_access_views.dw_payment_sent_adhoc adhoc
	LEFT JOIN cdim_payment_flow2 flow
	ON	adhoc.pmt_flow_key2=flow.pmt_flow_key2
LEFT JOIN fpa_region_definition c
	ON	adhoc.flow_to_country = c.country_code
LEFT JOIN
	pp_scratch_gba.VK_tagging_TPV_b b
	ON	
	adhoc.payment_transid=b.payment_transid
GROUP	BY 1,2,3,4,5,6,7,8,9,10
WHERE	
           adhoc.transaction_created_date BETWEEN  '2010-01-01' AND '2010-06-30'--(Set the month you want to segment the merchant TPV by here)

	AND	TRANSACTION_REFERENCE_STATUS IN ('S','OS','FV','PV')

        /* Fix USPS bug on PP site */
        
	AND	NOT ( COALESCE(adhoc.CUSTOMER_COUNTERPARTY,'0') = '2025189576733869774'
                
	AND	COALESCE(adhoc.TRANSACTION_CREATED_DATE, DATE '1969-12-31') BETWEEN DATE '2005-02-16' 
	AND	DATE '2005-03-23' )

        /* Payment was NOT flagged as going to CAT-99 merchant*/
        
	AND	ZEROIFNULL(adhoc.TRANSACTION_FLAG3) / 256 MOD 2 NOT = 1

        /* Receiver was not in cat-97 nor cat-99 */
        
	AND	ZEROIFNULL(COUNTERPARTY_CUST_CATEG) NOT IN (97,99)

        /* Not a USPS or Gift Certificate Loading Transaction */
        
	AND	COALESCE(adhoc.TRANSACTION_SUBTYPE,'#') NOT IN ('U', 'P')
) 
WITH	DATA PRIMARY INDEX(funding_source1,funding_source2,intra_cross,
		Receiver_geography, Sender_geography, month_, flow_family,onoff_ebay);
		
		
CREATE	TABLE pp_scratch_gba.VK_tagging_TPV_f AS
(
SELECT	
CASE	
	   
	WHEN	((adhoc.transaction_flag4/16384) MOD 2) = 1 THEN 'iEFT '
	   
	WHEN	((adhoc.transaction_flag3/16777216) MOD 2) = 1 THEN 'mEFT'
	   
	WHEN	((adhoc.transaction_flag4/16) MOD 2) = 1 THEN 'ELV '
	   
	WHEN	((adhoc.transaction_flag3/32) MOD 2) = 1 THEN 'uACH'
	   
	WHEN	((adhoc.transaction_flag1/33554432) MOD 2) = 1 THEN 'iACH'
	   
	WHEN	((adhoc.transaction_flag1/67108864) MOD 2) = 1 THEN 'eCheck'
	   
	WHEN	((adhoc.transaction_flag3/134217728) MOD 2) = 1 THEN 'PPBC' 
	   
	WHEN	adhoc.cc_transid_y_n ='Y' AND	card_usage =1 THEN 'CC'
	   
	WHEN	adhoc.cc_transid_y_n ='Y' AND	card_usage=2 THEN 'DC'
	
	WHEN adhoc.cc_transid_y_n ='Y' THEN 'CC other'
	   
ELSE	'Balance' 
	   
END	funding_source1
,
CASE	WHEN Funding_Source1 IN ('iEFT', 'mEFT', 'ELV', 'uACH', 'iACH',
		'eCheck') THEN 'BA' 
					WHEN Funding_Source1 IN  ('CC','DC','CC other') THEN 'CC'
					ELSE Funding_Source1 
END	AS Funding_Source2	   
,
CASE		WHEN	adhoc.flow_to_country = '99'  
	OR	 adhoc.flow_from_country = adhoc.flow_to_country THEN 'IB'		ELSE	'XB' 	END AS intra_cross
,
CASE	WHEN flow_from_country = 'US' THEN 'US' 
ELSE	'Intl' 
END	AS sender_geography
,		PPREV_ROLLUP1 AS receiver_geography

,	CASE
		WHEN	adhoc.transaction_usd_equiv_amt > -5000 THEN '$0-$50'
		WHEN	adhoc.transaction_usd_equiv_amt > -10000 THEN '$50-$100'
		WHEN	adhoc.transaction_usd_equiv_amt > -25000 THEN '$100-$250'
		WHEN	adhoc.transaction_usd_equiv_amt > -50000 THEN '$250-$500'
		WHEN	adhoc.transaction_usd_equiv_amt > -100000 THEN '$500-$1K'
		WHEN	adhoc.transaction_usd_equiv_amt > -200000 THEN '$1K-$2K'
		WHEN	adhoc.transaction_usd_equiv_amt > -300000 THEN '$2K-$3K'
		ELSE '>$3K'
	END ASP_bucket
	
,	CASE		WHEN	adhoc.transaction_subtype='I' THEN 'On-Ebay' 
ELSE	'Off-Ebay' 
END	AS OnOff_EBay
,	flow_family
,	CASE 
	WHEN	primary_flow = 'MS PF Virtual Terminal' THEN 'VT'
			WHEN primary_flow = 'MS PF Direct Payments' THEN 'DCC'
			ELSE 'Other' 
END	AS DCC_VT_ACTUAL
	,adhoc.transaction_created_date (FORMAT 'YYYY/MM') (CHAR(7)) AS month_
	,COUNT(adhoc.payment_transid) AS Txn_cnt
	,SUM( (TRANSACTION_REFERENCE_AMT)/CAST(-100 AS FLOAT)) AS ntpv
FROM	 pp_access_views.dw_payment_sent_adhoc adhoc
	LEFT JOIN cdim_payment_flow2 flow
	ON	adhoc.pmt_flow_key2=flow.pmt_flow_key2
LEFT JOIN fpa_region_definition c
	ON	adhoc.flow_to_country = c.country_code
LEFT JOIN
	pp_scratch_gba.VK_tagging_TPV_b b
	ON	
	adhoc.payment_transid=b.payment_transid
GROUP	BY 1,2,3,4,5,6,7,8,9,10
WHERE	
           adhoc.transaction_created_date > '2010-06-30'--(Set the month you want to segment the merchant TPV by here)

	AND	TRANSACTION_REFERENCE_STATUS IN ('S','OS','FV','PV')

        /* Fix USPS bug on PP site */
        
	AND	NOT ( COALESCE(adhoc.CUSTOMER_COUNTERPARTY,'0') = '2025189576733869774'
                
	AND	COALESCE(adhoc.TRANSACTION_CREATED_DATE, DATE '1969-12-31') BETWEEN DATE '2005-02-16' 
	AND	DATE '2005-03-23' )

        /* Payment was NOT flagged as going to CAT-99 merchant*/
        
	AND	ZEROIFNULL(adhoc.TRANSACTION_FLAG3) / 256 MOD 2 NOT = 1

        /* Receiver was not in cat-97 nor cat-99 */
        
	AND	ZEROIFNULL(COUNTERPARTY_CUST_CATEG) NOT IN (97,99)

        /* Not a USPS or Gift Certificate Loading Transaction */
        
	AND	COALESCE(adhoc.TRANSACTION_SUBTYPE,'#') NOT IN ('U', 'P')
) 
WITH	DATA PRIMARY INDEX(funding_source1,funding_source2,intra_cross,
		Receiver_geography, Sender_geography, month_, flow_family,onoff_ebay);
		
CREATE	TABLE pp_scratch_gba.VK_tagging_TPV AS
(
SELECT	
*
FROM	
 pp_scratch_gba.VK_tagging_TPV_c
UNION	
SELECT	
*
FROM	
 pp_scratch_gba.VK_tagging_TPV_d
 UNION
SELECT	
*
FROM	
 pp_scratch_gba.VK_tagging_TPV_e
 UNION
SELECT	
*
FROM	
 pp_scratch_gba.VK_tagging_TPV_f
 ) 
WITH	DATA PRIMARY INDEX(funding_source1,funding_source2,intra_cross,
		Receiver_geography, Sender_geography, month_, flow_family,onoff_ebay);
 
 
 
SEL	
 month_,
 COUNT(*)
 
FROM	
 pp_scratch_gba.VK_tagging_TPV
 
GROUP	BY 1
 ;

month_	COUNT(*)
2009/01	8725
2009/02	8765
2009/03	8665
2009/04	8655
2009/05	8769
2009/06	8916
2009/07	8916
2009/08	9073
2009/09	9184
2009/10	9299
2009/11	8915
2009/12	9041
2010/01	9113
2010/02	9337
2010/03	9771
2010/04	9988
2010/05	10007
2010/06	10050
2010/07	10154
2010/08	10333
2010/09	10572
2010/10	8485


 
SEL	
 month_,
 COUNT(*)
 
FROM	
vk_tagging_dataset 
 
GROUP	BY 1
 ;
month_	COUNT(*)
200901	138234
200902	136925
200903	142558
200904	139271
200905	140821
200906	139307
200907	139652
200908	141532
200909	142464
200910	146078
200911	150036
200912	153479
201001	153061
201002	150669
201003	165719
201004	164375
201005	166166
201006	166868
201007	164182
201008	162815
201009	139676
201010	35594




CREATE	TABLE zst_taggingloss0809 AS(
SELECT	x.*, y.transaction_created_ts 
FROM	 tdavis1_ltm_tags_test_new x LEFT JOIN pp_access_views.tld_negative_payment y 
	ON	x.payment_transid = y.payment_transid
) 
WITH	DATA UNIQUE PRIMARY INDEX(payment_transid);

--  drop table zst_Qdata_Subset;
CREATE	TABLE zst_Qdata_Subset AS(
SELECT	fq.FRAUD_DATA_ID
                , fq.FQ_ID
                , fq.CUST_ID
                , fq.FRAUD_DATA_COMMENTS
                , fq.FRAUD_DATA_CRTD_TS
                , fq.FRAUD_DATA_CRTD_DATE
                , fq.FRAUD_DATA_STATUS
                , fq.FRAUD_DATA_MODEL_NAME
                , fq.FRAUD_TRANS_LIKE_ID
                , fsr.FQ_SUSPCT_RSN_DESC
                , fq.FRAUD_ACTN_TS
                , fsr.FRAUD_SUSPCT_CREATOR_MODEL
                , fql.fql_actn_type
                , 
CASE	WHEN fql.fql_actn_type IN ('R','L') THEN 1 
ELSE	0 
END	restricted
                , 
CASE	WHEN fql.fql_actn_type='D' THEN 1 
ELSE	0 
END	dismissed
                , 
CASE	WHEN fql.fql_actn_type='Q' THEN 1 
ELSE	0 
END	queued
                , 
CASE	WHEN fql.fql_actn_type='E' THEN 1 
ELSE	0 
END	denied
FROM	pp_access_views.dw_fraud_data fq
                LEFT JOIN pp_access_views.dw_fraud_suspect_reason_new fsr
                                
	ON	fsr.fq_suspct_rsn_id = fq.fq_suspct_rsn_id
                LEFT JOIN (
SELECT	* 
FROM	pp_access_views.dw_fraud_queue_log 
WHERE	fql_actn_type IN ('R','L','D','Q','E')) fql
                                
	ON	fq.fq_id=fql.fql_fq_id
WHERE	fq.FRAUD_DATA_CRTD_DATE >= '2009-01-01'
                
	AND	FQ_SUSPCT_RSN_DESC NOT LIKE 'SVC%'
) 
WITH	DATA UNIQUE PRIMARY INDEX(FRAUD_DATA_ID, fql_actn_type);

--SELECT FRAUD_DATA_MODEL_NAME, FQ_SUSPCT_RSN_DESC, FRAUD_SUSPCT_CREATOR_MODEL, COUNT(*) FROM zst_Qdata_Subset GROUP BY 1,2,3;

COLLECT	STATISTICS PP_SCRATCH_GBA.zst_Qdata_Subset COLUMN CUST_ID;
COLLECT	STATISTICS PP_SCRATCH_GBA.zst_Qdata_Subset COLUMN FRAUD_TRANS_LIKE_ID;
COLLECT	STATISTICS PP_SCRATCH_GBA.zst_taggingloss0809 COLUMN SENDER_ID;

CREATE	TABLE zst_Qdata_T_temp_b AS(
SEL	
				  FRAUD_TRANS_LIKE_ID
                , MAX(fqt.restricted) AS T_rest
                , MAX(fqt.dismissed) AS T_dism
                , MAX(fqt.queued) AS T_qd
                , MAX(fqt.denied) AS T_den
FROM	
zst_Qdata_Subset fqt
GROUP	BY 1
) 
WITH	DATA UNIQUE PRIMARY INDEX(FRAUD_TRANS_LIKE_ID);

--diagnostic helpstats on for session;
CREATE	TABLE zst_Qdata_T AS(
SELECT	a.payment_transid
                , a.sender_id
                , a.receiver_id
                , a.transaction_created_ts
                , T_rest
                , T_dism
                , T_qd
                , T_den
FROM	zst_taggingloss0809 a
                LEFT JOIN zst_Qdata_T_temp_b fqt
                                
	ON	a.payment_transid = fqt.FRAUD_TRANS_LIKE_ID
) 
WITH	DATA UNIQUE PRIMARY INDEX (payment_transid);

--SEL TOP 100 * FROM zst_Qdata_T;
CREATE TABLE Data_set_1
AS
(
SEL
         COALESCE(fq.cust_id, g.sender_id) AS sender_id
        , g.transaction_created_date
        , fq.FRAUD_DATA_CRTD_TS
        , MAX(fq.restricted) AS S_rest
        , MAX(fq.dismissed) AS S_dism
        , MAX(fq.queued) AS S_qd
        , MAX(fq.denied) AS S_den
FROM                
(
SEL 
sender_id,
transaction_created_date
FROM
zst_taggingloss0809 g
GROUP BY 1,2
) g
LEFT JOIN
zst_Qdata_Subset fq
ON 
g.sender_id=fq.cust_id
AND fq.FRAUD_DATA_CRTD_DATE >= g.transaction_created_date
AND (fq.FRAUD_DATA_CRTD_DATE - g.transaction_created_date) <= 1
GROUP BY 1,2,3
) WITH DATA PRIMARY INDEX (sender_id, transaction_created_date);

--drop table  zst_Qdata_S;
CREATE TABLE zst_Qdata_S 
AS
(
SEL
payment_transid,
sender_id,
receiver_id,
transaction_created_ts,
MAX(s_rest) s_rest,
MAX(s_dism) s_dism,
MAX(s_qd) s_qd,
MAX(s_den) s_den
FROM
(
SEL 
a.payment_transid
                , a.sender_id
                , a.receiver_id
                , a.transaction_created_ts
                ,(EXTRACT (DAY FROM (FRAUD_DATA_CRTD_TS- transaction_created_ts DAY(4) TO HOUR)) * 24 + EXTRACT (HOUR FROM (FRAUD_DATA_CRTD_TS- transaction_created_ts DAY(4) TO HOUR))) AS hr_diff
                ,CASE WHEN hr_diff >= 0 AND hr_diff <=24 THEN b.S_rest ELSE NULL END AS S_rest
                ,CASE WHEN hr_diff >= 0 AND hr_diff <=24 THEN b.S_dism ELSE NULL END AS S_dism
                 ,CASE WHEN hr_diff >= 0 AND hr_diff <=24 THEN b.S_qd ELSE NULL END AS S_qd
                  ,CASE WHEN hr_diff >= 0 AND hr_diff <=24 THEN b.S_den ELSE NULL END AS S_den
FROM
zst_taggingloss0809 a
INNER JOIN
 Data_set_1 b
ON a.sender_id=b.sender_id
AND a.transaction_created_date=b.transaction_created_date
) a
GROUP BY 1,2,3,4
) WITH DATA  PRIMARY INDEX (payment_transid);

COLLECT	STATISTICS PP_SCRATCH_GBA.zst_taggingloss0809 COLUMN  RECEIVER_ID;

CREATE TABLE Data_set_2
AS
(
SEL
         COALESCE(fq.cust_id, g.RECEIVER_ID) AS RECEIVER_ID
        , g.transaction_created_date
        , fq.FRAUD_DATA_CRTD_TS
        , MAX(fq.restricted) AS R_rest
        , MAX(fq.dismissed) AS R_dism
        , MAX(fq.queued) AS R_qd
        , MAX(fq.denied) AS R_den
FROM                
(
SEL 
RECEIVER_ID,
transaction_created_date
FROM
zst_taggingloss0809 g
GROUP BY 1,2
) g
LEFT JOIN
zst_Qdata_Subset fq
ON 
g.RECEIVER_ID=fq.cust_id
AND fq.FRAUD_DATA_CRTD_DATE >= g.transaction_created_date
AND (fq.FRAUD_DATA_CRTD_DATE - g.transaction_created_date) <= 1
GROUP BY 1,2,3
) WITH DATA PRIMARY INDEX (RECEIVER_ID, transaction_created_date);


CREATE TABLE zst_Qdata_R
AS
(
SEL
payment_transid,
sender_id,
receiver_id,
transaction_created_ts,
MAX(R_rest) R_rest,
MAX(R_dism) R_dism,
MAX(R_qd) R_qd,
MAX(R_den) R_den
FROM
(
SEL 
a.payment_transid
                , a.sender_id
                , a.receiver_id
                , a.transaction_created_ts
                ,(EXTRACT (DAY FROM (FRAUD_DATA_CRTD_TS- transaction_created_ts DAY(4) TO HOUR)) * 24 + EXTRACT (HOUR FROM (FRAUD_DATA_CRTD_TS- transaction_created_ts DAY(4) TO HOUR))) AS hr_diff
                ,CASE WHEN hr_diff >= 0 AND hr_diff <=24 THEN b.R_rest ELSE NULL END AS R_rest
                ,CASE WHEN hr_diff >= 0 AND hr_diff <=24 THEN b.R_dism ELSE NULL END AS R_dism
                 ,CASE WHEN hr_diff >= 0 AND hr_diff <=24 THEN b.R_qd ELSE NULL END AS R_qd
                  ,CASE WHEN hr_diff >= 0 AND hr_diff <=24 THEN b.R_den ELSE NULL END AS R_den
FROM
zst_taggingloss0809 a
INNER JOIN
 Data_set_2 b
ON a.RECEIVER_ID=b.RECEIVER_ID
AND a.transaction_created_date=b.transaction_created_date
) a
GROUP BY 1,2,3,4
) WITH DATA  PRIMARY INDEX (payment_transid);



--  drop table zst_Qdata_TSR;
CREATE	TABLE zst_Qdata_TSR AS (
SELECT	a.payment_transid
                , MAX(
CASE	WHEN ( ZEROIFNULL(T_qd) + ZEROIFNULL(S_qd) + ZEROIFNULL(R_qd)) > 0 THEN 1 
ELSE	0 
END	) AS queued
                , MAX(
CASE	WHEN ( ZEROIFNULL(T_dism) + ZEROIFNULL(S_dism) + ZEROIFNULL(R_dism)) > 0 THEN 1 
ELSE	0 
END	) AS dismissed
                , MAX(
CASE	WHEN ( ZEROIFNULL(T_den) + ZEROIFNULL(S_den) + ZEROIFNULL(R_den)) > 0 THEN 1 
ELSE	0 
END	) AS denied
                , MAX(
CASE	WHEN ( ZEROIFNULL(T_rest) + ZEROIFNULL(S_rest) + ZEROIFNULL(R_rest)) > 0 THEN 1 
ELSE	0 
END	) AS restricted
FROM	zst_Qdata_T a
                LEFT JOIN zst_Qdata_S s
                                
	ON	a.payment_transid=s.payment_transid
                LEFT JOIN zst_Qdata_R b
                                
	ON	a.payment_transid=b.payment_transid
GROUP	BY 1
) 
WITH	DATA UNIQUE PRIMARY INDEX(payment_transid);


CREATE	TABLE tdavis1_ltm_tags_test_new_a AS (
SELECT	
a.*,
queued,
dismissed,
denied,
restricted
FROM	
tdavis1_ltm_tags_test_new a
LEFT JOIN
zst_Qdata_TSR b
ON	
a.payment_transid=b.payment_transid
) 
WITH	DATA UNIQUE PRIMARY INDEX(payment_transid);

--drop table  tdavis1_ltm_tags_test_new;

RENAME TABLE tdavis1_ltm_tags_test_new_a TO tdavis1_ltm_tags_test_new;


SEL TOP 100 * FROM tdavis1_ltm_tags_test_new_a;

SEL month_,SUM(queued),
SUM(dismissed),
SUM(denied),
SUM(restricted) FROM tdavis1_ltm_tags_test_new GROUP BY 1;

