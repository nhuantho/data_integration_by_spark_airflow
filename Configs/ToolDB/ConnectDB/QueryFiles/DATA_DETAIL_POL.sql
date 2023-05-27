SELECT
	t.*, t1.CHANNEL_CODE
FROM
	F88DWH.W_PAWN_ONLINE_TLS_F t
	LEFT JOIN F88DWH.W_LOAN_DTL_F t1 ON t.CONTRACT_NO = t1.LOAN_CODE
WHERE
	1 = 1
	AND ((TRUNC(FORM_CREATED_DT) >= to_date(:day1, 'yyyy-mm-dd')
		AND TRUNC(FORM_CREATED_DT) <= to_date(:day2, 'yyyy-mm-dd'))
		OR (CALL_LST_DT >= to_date(:day1, 'yyyy-mm-dd')
			AND CALL_LST_DT <= to_date(:day2, 'yyyy-mm-dd'))
			OR (PAWN_DT >= to_date(:day1, 'yyyy-mm-dd')
				AND PAWN_DT <= to_date(:day2, 'yyyy-mm-dd'))
				OR (SHOP_LST_DT >= to_date(:day1, 'yyyy-mm-dd')
					AND SHOP_LST_DT <= to_date(:day2, 'yyyy-mm-dd'))
        )
