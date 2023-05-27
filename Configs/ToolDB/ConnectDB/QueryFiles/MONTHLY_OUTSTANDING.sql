select/*+ index(a W_DOCUMENT_ODS_HISTORY_DW) */
        a.CODE_NO ,
    a.DATE_WID,
    Case when a.OVER_DUE_DAYS < 90 then '<90days' else '>=90days' end DPD,
    a.PRINCIPAL_AMOUNT,
    a.DISBURST_AMOUNT,
    case when l_1.ASSET_TYPE_WID  = 7 then 'OTO'
        when l_1.ASSET_TYPE_WID  = 15 then 'DKOTO'
        when l_1.ASSET_TYPE_WID  = 17 then 'DKXM'
        else 'Khác' end Asset,
    a.CIF_CODE ,
    fl.GENDER_NM,
    fl.BIRTH_DAY,
    c.Vung,
    c.DV_HC,
    c.DIA_HINH,
    fl.industry_nm,
    case when p.loan_purpose_name is null then l_1.LOAN_PURPOSE else p.loan_purpose_name end Loan_purpose
from W_DOCUMENT_ODS_HISTORY a
JOIN W_LOAN_DTL_F l_1 ON l_1.LOAN_code = a.CODE_NO
left join (select s.shop_wid, m.OPEN_DT Vung, m.DV_HC, m.DIA_HINH, s.SHOP_CODE,s.VALID_FM_DT ,s.VALID_TO_DT   from f88dwh.w_shop_d s
           left join f88dwh.w_area_manager_d m on trim(s.shop_code) = trim(m.shop_id)) c on a.OFFICE_CODE  = c.SHOP_CODE AND c.VALID_FM_DT <= to_date(:last_day_of_month,'yyyymmdd') AND to_date(:last_day_of_month,'yyyymmdd') < c.VALID_TO_DT
LEFT join (select distinct CUSTOMER_CODE, GENDER_NM, BIRTH_DAY, industry_nm from  minhnh.w_first_loan_contract_v ) fl on a.CIF_CODE  = fl.CUSTOMER_CODE
LEFT JOIN (SELECT l.loan_wid, l.loan_purpose_name, count(l.loan_wid) FROM f88dwh.w_loan_customer_f l
    join (select loan_wid, max(INTEGRATION_ID) max_id from f88dwh.w_loan_customer_f group by loan_wid) t on l.loan_wid = t.loan_wid and l.INTEGRATION_ID = t.max_id
    group by l.loan_wid, l.loan_purpose_name) p on l_1.loan_wid = p.loan_wid
where 1 = 1
and a.year_num = :year1
and a.month_num = :month1
and a.date_wid = :last_day_of_month
and a.LOAN_STATUS in (300,603)
and a.is_bad_debt <> 1
and (nvl(l_1.FUND_NAME,'F88 Fund') <> 'CIMB Fund' or a.IS_BUY_DEBT = 1)