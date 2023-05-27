SELECT
        to_char(l.UPLOAD_DATE,'yyyymmdd')    DATE_WID
        ,to_date(t.TRANS_DATE_WID,'YYYYMMDD') trans_date
        ,l.LOAN_CODE                          CONTRACT_ID
        ,l.LOAN_ID
        ,c.customer_code                      CUSTOMER_ID
        ,l.STATUS_DISP                        STATUS
        ,t.TRANS_AMT
        ,t.PRINCIPAL_AMT
        ,t.PAYMENT_TYPE
        ,t.action_code
FROM f88dwh.w_lendable_loan_save l
JOIN f88dwh.W_LOAN_TRANS_DTL_F t on l.loan_id = t.loan_wid
left join f88dwh.vw_w_customer_d c on l.customer_id = c.customer_wid
where 1=1
and t.TRANS_DATE_WID = :DATE_WID
and (t.DEACTIVE_FLAG = 0 or t.DEACTIVE_FLAG is null)
and t.ACTION_CODE not in ('ADD_ADVANCE_TRANSFER','CHUYEN_NO_XAU','CHUYEN_THANH_LY','CHUYEN_THANH_LY_HDNX','CHUYEN_TIEN_DU_CIF_HD','CHUYEN_TIEN_DU_HD_CIF','DIEU_CHINH_GD','DIEU_CHINH_GOC','DONG_HD_MIGRATE'
                        ,'GIAI_NGAN','GIAI_NGAN_CIMB','GIAI_NGAN_CIMB_TM','GIAI_NGAN_HD_CU','GIAI_NGAN_TOPUP','GIAI_NGAN_VMH','HUY_BAN_THANHLY','HUY_DONG_HD','NAP_TIEN_DU','NAP_TIEN_DU_HD','NAP_TIEN_DU_HD_HDNX'
                        ,'SUB_ADVANCE_TRANSFER','TRA_CPV_CIMB','TRA_HANG_THANH_LY','TRA_HANG_THANH_LY_HDNX','TRA_TIEN_DU','VAY_THEM_GOC')
and l.status not in ('close','withdraw')
--split
SELECT
        to_char(l.UPLOAD_DATE,'yyyymmdd')    DATE_WID
        ,to_date(t.TRANS_DATE_WID,'YYYYMMDD') trans_date
        ,l.LOAN_CODE                          CONTRACT_ID
        ,l.LOAN_ID
        ,c.customer_code                      CUSTOMER_ID
        ,l.STATUS_DISP                        STATUS
        ,t.TRANS_AMT
        ,t.PRINCIPAL_AMT
        ,t.PAYMENT_TYPE
        ,t.action_code
FROM f88dwh.w_lendable_loan_save_v2 l
JOIN f88dwh.W_LOAN_TRANS_DTL_F t on l.loan_id = t.loan_wid
left join f88dwh.vw_w_customer_d c on l.customer_id = c.customer_wid
where 1=1
and t.TRANS_DATE_WID = :DATE_WID 
and (t.DEACTIVE_FLAG = 0 or t.DEACTIVE_FLAG is null)
and t.ACTION_CODE not in ('ADD_ADVANCE_TRANSFER','CHUYEN_NO_XAU','CHUYEN_THANH_LY','CHUYEN_THANH_LY_HDNX','CHUYEN_TIEN_DU_CIF_HD','CHUYEN_TIEN_DU_HD_CIF','DIEU_CHINH_GD','DIEU_CHINH_GOC','DONG_HD_MIGRATE'
                        ,'GIAI_NGAN','GIAI_NGAN_CIMB','GIAI_NGAN_CIMB_TM','GIAI_NGAN_HD_CU','GIAI_NGAN_TOPUP','GIAI_NGAN_VMH','HUY_BAN_THANHLY','HUY_DONG_HD','NAP_TIEN_DU','NAP_TIEN_DU_HD','NAP_TIEN_DU_HD_HDNX'
                        ,'SUB_ADVANCE_TRANSFER','TRA_CPV_CIMB','TRA_HANG_THANH_LY','TRA_HANG_THANH_LY_HDNX','TRA_TIEN_DU','VAY_THEM_GOC')
and l.status not in ('close','withdraw')
--split
SELECT
        to_char(l.UPLOAD_DATE,'yyyymmdd')    DATE_WID
        ,to_date(t.TRANS_DATE_WID,'YYYYMMDD') trans_date
        ,l.LOAN_CODE                          CONTRACT_ID
        ,l.LOAN_ID
        ,c.customer_code                      CUSTOMER_ID
        ,l.STATUS_DISP                        STATUS
        ,t.TRANS_AMT
        ,t.PRINCIPAL_AMT
        ,t.PAYMENT_TYPE
        ,t.action_code
FROM f88dwh.w_lendable_loan_save_v3 l
JOIN f88dwh.W_LOAN_TRANS_DTL_F t on l.loan_id = t.loan_wid
left join f88dwh.vw_w_customer_d c on l.customer_id = c.customer_wid
where 1=1 
and t.TRANS_DATE_WID = :DATE_WID 
and (t.DEACTIVE_FLAG = 0 or t.DEACTIVE_FLAG is null)
and t.ACTION_CODE not in ('ADD_ADVANCE_TRANSFER','CHUYEN_NO_XAU','CHUYEN_THANH_LY','CHUYEN_THANH_LY_HDNX','CHUYEN_TIEN_DU_CIF_HD','CHUYEN_TIEN_DU_HD_CIF','DIEU_CHINH_GD','DIEU_CHINH_GOC','DONG_HD_MIGRATE'
                        ,'GIAI_NGAN','GIAI_NGAN_CIMB','GIAI_NGAN_CIMB_TM','GIAI_NGAN_HD_CU','GIAI_NGAN_TOPUP','GIAI_NGAN_VMH','HUY_BAN_THANHLY','HUY_DONG_HD','NAP_TIEN_DU','NAP_TIEN_DU_HD','NAP_TIEN_DU_HD_HDNX'
                        ,'SUB_ADVANCE_TRANSFER','TRA_CPV_CIMB','TRA_HANG_THANH_LY','TRA_HANG_THANH_LY_HDNX','TRA_TIEN_DU','VAY_THEM_GOC')
and l.status not in ('close','withdraw')
