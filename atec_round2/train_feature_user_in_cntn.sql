drop table if exists t_sj_test_user_client_ip_20n;


create table t_sj_test_user_client_ip_20n as
select t3.event_id,
       -- t3.user_id,
       -- t3.client_ip as t3_client_ip,
       -- t4.client_ip as t4_client_ip,
       t4.client_ip,
       t4.user_client_ip_cnt_20n
from t_sj_test_data_code_unix t3
inner join
    (select user_id,
            gmt_occur_unix,
            client_ip,
            count(*) as user_client_ip_cnt_20n
     from
         (select row_number() over(partition by t1.user_id
                                   order by t2.gmt_occur_unix desc) as cnt_order,
                 t1.user_id,
                 t1.gmt_occur_unix,
                 t2.event_id,
                 t2.client_ip
          from
              (select user_id,
                      gmt_occur_unix
               from t_sj_test_data_code_unix
               group by user_id,
                        gmt_occur_unix) t1
          left outer join
              (select event_id,
                      user_id,
                      gmt_occur_unix,
                      client_ip
               from t_sj_test_data_code_unix) t2 on t1.user_id = t2.user_id
          where t2.gmt_occur_unix < t1.gmt_occur_unix)t
     where cnt_order <=20
     group by user_id,
              gmt_occur_unix,
              client_ip)t4 on t3.user_id = t4.user_id
and t3.gmt_occur_unix = t4.gmt_occur_unix;