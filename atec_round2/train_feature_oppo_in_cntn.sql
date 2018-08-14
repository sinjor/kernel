drop table if exists t_sj_train_oppo_20n_not_now;


create table t_sj_train_oppo_20n_not_now as
select t3.event_id,
       t4.oppo_cnt_20n,
       t4.oppo_ucnt_client_ip_20n,
       t4.oppo_ucnt_network_20n,
       t4.oppo_ucnt_device_sign_20n,
       t4.oppo_ucnt_info_1_20n,
       t4.oppo_ucnt_info_2_20n,
       t4.oppo_ucnt_ip_prov_20n,
       t4.oppo_ucnt_ip_city_20n,
       t4.oppo_ucnt_mobile_oper_platform_20n,
       t4.oppo_ucnt_operation_channel_20n,
       t4.oppo_ucnt_pay_scene_20n,
       t4.oppo_sum_amt_20n,
       t4.oppo_ucnt_user_id_20n
from t_sj_train_data_code_unix t3
inner join
    (select opposing_id,
            gmt_occur_unix,
            count(*) as oppo_cnt_20n,
            size(collect_set(client_ip)) as oppo_ucnt_client_ip_20n,
            size(collect_set(network)) as oppo_ucnt_network_20n,
            size(collect_set(device_sign)) as oppo_ucnt_device_sign_20n,
            size(collect_set(info_1)) as oppo_ucnt_info_1_20n,
            size(collect_set(info_2)) as oppo_ucnt_info_2_20n,
            size(collect_set(ip_prov)) as oppo_ucnt_ip_prov_20n,
            size(collect_set(ip_city)) as oppo_ucnt_ip_city_20n,
            size(collect_set(mobile_oper_platform)) as oppo_ucnt_mobile_oper_platform_20n,
            size(collect_set(operation_channel)) as oppo_ucnt_operation_channel_20n,
            size(collect_set(pay_scene)) as oppo_ucnt_pay_scene_20n,
            sum(amt) as oppo_sum_amt_20n,
            size(collect_set(user_id)) as oppo_ucnt_user_id_20n
     from
         (select row_number() over(partition by t1.opposing_id,t1.gmt_occur_unix
                                   order by t2.gmt_occur_unix desc) as cnt_order,
                 t1.opposing_id,
                 t1.gmt_occur_unix,
                 t2.client_ip,
                 t2.network,
                 t2.device_sign,
                 t2.info_1,
                 t2.info_2,
                 t2.ip_prov,
                 t2.ip_city,
                 t2.mobile_oper_platform,
                 t2.operation_channel,
                 t2.pay_scene,
                 t2.amt,
                 t2.user_id
          from
              (select t02.opposing_id,
                      t02.gmt_occur_unix
               from
                   (select event_id
                    from t_sj_train_feature_oppo_24h_not_now
                    where oppo_cnt_24h <20)t01
               inner join t_sj_train_data_code_unix t02 on t01.event_id = t02.event_id
               group by t02.opposing_id,
                        t02.gmt_occur_unix) t1
          left outer join
              (select opposing_id,
                      gmt_occur_unix,
                      client_ip,
                      network,
                      device_sign,
                      info_1,
                      info_2,
                      ip_prov,
                      ip_city,
                      mobile_oper_platform,
                      operation_channel,
                      pay_scene,
                      amt,
                      user_id
               from t_sj_train_data_code_unix) t2 on t1.opposing_id = t2.opposing_id
          where t2.gmt_occur_unix < t1.gmt_occur_unix)t
     where cnt_order <=20
     group by opposing_id,
              gmt_occur_unix) t4 on t3.opposing_id = t4.opposing_id
and t3.gmt_occur_unix = t4.gmt_occur_unix;