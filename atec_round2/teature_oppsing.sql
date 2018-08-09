select t1.device_sign,
       t1.gmt_occur_unix,
       count(*) as device_cnt_24h,
       size(collect_set(t2.user_id)) as device_ucnt_user_id_24h
from
    (select device_sign,
            gmt_occur_unix
     from t_sj_test_data_code_unix
     group by device_sign,
              gmt_occur_unix) t1
left outer join
    (select event_id,
            device_sign,
            gmt_occur_unix,
            user_id
     from t_sj_test_data_code_unix
     where device_sign is not null) t2 on t1.device_sign = t2.device_sign
where (t1.gmt_occur_unix - t2.gmt_occur_unix) < 86400
    and t2.gmt_occur_unix < t1.gmt_occur_unix
    group by t1.device_sign, t1.gmt_occur_unix