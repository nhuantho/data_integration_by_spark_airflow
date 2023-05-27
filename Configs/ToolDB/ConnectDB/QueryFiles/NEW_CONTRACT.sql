SELECT
	p.pawn_online_wid,
	p.caller_name,
	p.caller_code,
	p.CUSTOMER_Name,
	p.shop_name,
	p.shop_code,
	p.SHOP_CURRENT_WID,
	p.pawn_dt,
	p.disburse_dt,
	p.pawn_wid,
	p.contract_no,
	p.packge_codde,
	p.money,
	p.campaign,
	p.nhom_nguon,
	p.nguon_phu,
	p.phan_loai_nguon_phu,
	p.asset,
	p.url,
	a.created_dt,
	c.GN_Datetime,
	opc.note,
	opc.action_id,
	opc.lastcall,
	opc.pawn_status,
	p.FORM_CREATED_DT,
	p.FIRST_CALL,
	t.CREATED_USER "Nguoi tao HĐ",
	t.CHANNEL_CODE,
	t.CTR_TYPE,
	ROW_NUMBER() OVER(PARTITION BY p.contract_no
ORDER BY
	p.pawn_online_wid) AS row_n
FROM
	f88dwh.w_pawn_online_tls_f p
LEFT JOIN (
	SELECT
		LOAN_CODE ,
		CREATED_USER,
		CTR_TYPE,
		CHANNEL_CODE
	FROM
		F88DWH.W_LOAN_DTL_F
		) t ON
	p.CONTRACT_NO = t.LOAN_CODE
LEFT JOIN (
	SELECT
		l.created_dt,
		l.loan_code
	FROM
		f88dwh.w_loan_state_workflow_f l
	JOIN (
		SELECT
			loan_code,
			min(integration_id) min_integration_id
		FROM
			f88dwh.w_loan_state_workflow_f
		GROUP BY
			loan_code) l1 ON
		l.loan_code = l1.loan_code
		AND l.integration_id = l1.min_integration_id
	WHERE
		trunc(l.created_dt) >= sysdate - 35
) a ON
	p.contract_no = a.loan_code
LEFT JOIN (
	SELECT
		lt.loan_code,
		lt.created_dt GN_Datetime
	FROM
		f88dwh.w_loan_trans_dtl_f lt
	JOIN (
		SELECT
			loan_code,
			max(integration_id) maxid
		FROM
			f88dwh.w_loan_trans_dtl_f
		WHERE
			(trunc(created_dt) >= sysdate - 35)
				AND ACTION_CODE IN ('GIAI_NGAN', 'GIAI_NGAN_CIMB', 'GIAI_NGAN_TOPUP', 'VAY_THEM_GOC', 'GIAI_NGAN_HD_CU', 'GIAI_NGAN_CIMB_TM' )
			GROUP BY
				loan_code) b ON
		lt.integration_id = b.maxid
	WHERE
		trunc(lt.created_dt) >= sysdate - 35) c ON
	p.contract_no = c.loan_code
LEFT JOIN (
	SELECT
		op.INT_POL_PAWN_ID ,
		op.NOTE ,
		op.ACTION_ID,
		op.PAWN_STATUS,
		to_date(op.CREATED_DT,
		'YYYY MM DD HH24:MI:SS') lastcall
	FROM
		F88DWH.W_ONLINE_PROCESS_F op
	WHERE
		op.INTEGRATION_ID IN (
		SELECT
			max(WOPF.INTEGRATION_ID)
		FROM
			F88DWH.W_ONLINE_PROCESS_F wopf
		LEFT JOIN F88DWH.W_EMPLOYEE_D wed ON
			WOPF.EMPLOYEE_WID = WED.EMPLOYEE_WID
		WHERE
			lower(WED.DEPARTMENT_NM) LIKE '%bán hàng qua điện thoại%'
				AND WOPF.ACTION_ID <> 4
				AND WOPF.PAWN_STATUS <> 0
				AND WOPF.INT_POL_PAWN_ID = op.INT_POL_PAWN_ID)
		AND date_wid >= '20220101' ) opc ON
	p.pawn_online_wid = opc.INT_POL_PAWN_ID
WHERE
	trunc(p.disburse_dt) >= sysdate - 35
	AND LOWER(p.STATUS_STR) = 'nhận cầm cố'