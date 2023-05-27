select wpotf.pawn_online_wid ,  wpotf.form_created_dt , wpotf.hour_from_created , wpotf.form_asset , wpotf.province ,wpotf.district , wpotf.caller_name ,
wpotf.caller_code , wpotf.first_call , wpotf.hour_first_call , wpotf.last_call , wpotf.hour_last_call , wpotf.trans_fst_dt , wpotf.trans_lst_dt ,
wamd.tpk , wamd.qlkv ,wpotf.shop_name , wpotf.saler_name , wpotf.saler_code , wpotf.shop_fst_dt , wpotf.hour_shop_fst , wpotf.shop_lst_dt , wpotf.hour_shop_lst,
wpotf.campaign , wpotf.nhom_nguon , wpotf.nguon_phu , wpotf.phan_loai_nguon_phu , wpotf.disburse_dt , wpotf.contract_no , wpotf.packge_codde , wpotf.asset , wpotf.money,
wpotf.status_str , wpotf.lst_str_status_content , wpotf.comment1
from f88dwh.w_pawn_online_tls_f wpotf
join (select ma_ns, count(1) from f88dwh.w_tls_employee_d where year = 2023 group by ma_ns) wted on (case when wpotf.caller_code like '%"_"1'then substr(caller_code,1,LENGTH (caller_code) - 2)
											 when wpotf.caller_code like '%"_"%'then substr(caller_code,1,LENGTH (caller_code) - 3)
											 else wpotf.caller_code end) = wted.ma_ns
left join f88dwh.w_area_manager_d wamd on wpotf.shop_current_wid= wamd.shop_id
where trunc(wpotf.trans_fst_dt) >= trunc(to_Date(:DATE_WID,'yyyy-mm-dd'),'MM') and trunc(wpotf.trans_fst_dt) <= to_date(:DATE_WID,'yyyy-mm-dd') 