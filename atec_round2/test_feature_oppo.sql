drop table if exists t_sj_test_feature_oppo_24h_not_now;


create table t_sj_test_feature_oppo_24h_not_now as
select t3.event_id,
       t3.opposing_id,
       t3.gmt_occur_unix,
       t4.oppo_cnt_24h,
       t4.oppo_ucnt_user_id_24h,
       t4.oppo_ucnt_client_ip_24h,
       t4.oppo_ucnt_network_24h,
       t4.oppo_ucnt_device_sign_24h,
       t4.oppo_ucnt_info_1_24h,
       t4.oppo_ucnt_info_2_24h,
       t4.oppo_ucnt_ip_prov_24h,
       t4.oppo_ucnt_ip_city_24h,
       t4.oppo_ucnt_mobile_oper_platform_24h,
       t4.oppo_ucnt_operation_channel_24h,
       t4.oppo_ucnt_pay_scene_24h,
       t4.oppo_sum_amt_24h
from t_sj_test_data_code_unix t3
left outer join
    (select t1.opposing_id,
            t1.gmt_occur_unix,
            count(*) as oppo_cnt_24h,
            size(collect_set(t2.user_id)) oppo_ucnt_user_id_24h,
            size(collect_set(t2.client_ip)) as oppo_ucnt_client_ip_24h,
            size(collect_set(t2.network)) as oppo_ucnt_network_24h,
            size(collect_set(t2.device_sign)) as oppo_ucnt_device_sign_24h,
            size(collect_set(t2.info_1)) as oppo_ucnt_info_1_24h,
            size(collect_set(t2.info_2)) as oppo_ucnt_info_2_24h,
            size(collect_set(t2.ip_prov)) as oppo_ucnt_ip_prov_24h,
            size(collect_set(t2.ip_city)) as oppo_ucnt_ip_city_24h,
            size(collect_set(t2.mobile_oper_platform)) as oppo_ucnt_mobile_oper_platform_24h,
            size(collect_set(t2.operation_channel)) as oppo_ucnt_operation_channel_24h,
            size(collect_set(t2.pay_scene)) as oppo_ucnt_pay_scene_24h,
            sum(t2.amt) as oppo_sum_amt_24h
     from
         (select opposing_id,
                 gmt_occur_unix
          from t_sj_test_data_code_unix
          group by opposing_id,
                   gmt_occur_unix) t1
     left outer join
         (select event_id,
                 opposing_id,
                 gmt_occur_unix,
                 user_id,
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
                 amt
          from t_sj_test_data_code_unix) t2 on t1.opposing_id = t2.opposing_id
     where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
         and t2.gmt_occur_unix < t1.gmt_occur_unix
     group by t1.opposing_id,
              t1.gmt_occur_unix) t4 on t3.opposing_id = t4.opposing_id
and t3.gmt_occur_unix = t4.gmt_occur_unix;



-- 一小时内的交易次数 和部分离散变量的去重个数
-- 为了优化代码，以下特征包括样本自身
drop table if exists t_sj_test_feature_oppo_1h_not_now;


create table t_sj_test_feature_oppo_1h_not_now as
select t3.event_id,
       t3.opposing_id,
       t3.gmt_occur_unix,
       t4.oppo_cnt_1h,
       t4.oppo_ucnt_user_id_1h,
       t4.oppo_ucnt_client_ip_1h,
       t4.oppo_ucnt_network_1h,
       t4.oppo_ucnt_device_sign_1h,
       t4.oppo_ucnt_info_1_1h,
       t4.oppo_ucnt_info_2_1h,
       t4.oppo_ucnt_ip_prov_1h,
       t4.oppo_ucnt_ip_city_1h,
       t4.oppo_ucnt_mobile_oper_platform_1h,
       t4.oppo_ucnt_operation_channel_1h,
       t4.oppo_ucnt_pay_scene_1h,
       t4.oppo_sum_amt_1h
from t_sj_test_data_code_unix t3
left outer join
    (select t1.opposing_id,
            t1.gmt_occur_unix,
            count(*) as oppo_cnt_1h,
            size(collect_set(t2.user_id)) oppo_ucnt_user_id_1h,
            size(collect_set(t2.client_ip)) as oppo_ucnt_client_ip_1h,
            size(collect_set(t2.network)) as oppo_ucnt_network_1h,
            size(collect_set(t2.device_sign)) as oppo_ucnt_device_sign_1h,
            size(collect_set(t2.info_1)) as oppo_ucnt_info_1_1h,
            size(collect_set(t2.info_2)) as oppo_ucnt_info_2_1h,
            size(collect_set(t2.ip_prov)) as oppo_ucnt_ip_prov_1h,
            size(collect_set(t2.ip_city)) as oppo_ucnt_ip_city_1h,
            size(collect_set(t2.mobile_oper_platform)) as oppo_ucnt_mobile_oper_platform_1h,
            size(collect_set(t2.operation_channel)) as oppo_ucnt_operation_channel_1h,
            size(collect_set(t2.pay_scene)) as oppo_ucnt_pay_scene_1h,
            sum(t2.amt) as oppo_sum_amt_1h
     from
         (select opposing_id,
                 gmt_occur_unix
          from t_sj_test_data_code_unix
          group by opposing_id,
                   gmt_occur_unix) t1
     left outer join
         (select event_id,
                 opposing_id,
                 gmt_occur_unix,
                 user_id,
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
                 amt
          from t_sj_test_data_code_unix) t2 on t1.opposing_id = t2.opposing_id
     and t1.gmt_occur_unix = t2.gmt_occur_unix
     group by t1.opposing_id,
              t1.gmt_occur_unix) t4 on t3.opposing_id = t4.opposing_id
and t3.gmt_occur_unix = t4.gmt_occur_unix;


-- -- 获取用户上一次的时间点 有记录的是9444000 去掉了用户第一次交易的记录
-- drop table if exists t_sj_test_oppo_time_diff;


-- create table t_sj_test_oppo_time_diff as
-- select t3.event_id,
--        (t3.gmt_occur_unix - t4.gmt_occur_unix_last)/3600 as gmt_occur_unix_oppo_diff
-- from t_sj_test_data_code_unix t3
-- left outer join
--     (select t1.opposing_id,
--             t1.gmt_occur_unix,
--             case when t1.cnt == 1 then max(t2.gmt_occur_unix) else t1.gmt_occur_unix end as gmt_occur_unix_last
--      from
--          (select opposing_id,
--                  gmt_occur_unix,
--                  count(*) as cnt
--           from t_sj_test_data_code_unix
--           group by opposing_id,
--                    gmt_occur_unix)t1
--      left outer join
--          (select opposing_id,
--                  gmt_occur_unix
--           from t_sj_test_data_code_unix
--           group by opposing_id,
--                    gmt_occur_unix)t2 on t1.opposing_id = t2.opposing_id
--      and t1.gmt_occur_unix = t2.gmt_occur_unix
--      where t1.gmt_occur_unix>=t2.gmt_occur_unix
--      group by t1.opposing_id,
--               t1.gmt_occur_unix) t4 on t3.opposing_id = t4.opposing_id
-- and t3.gmt_occur_unix = t4.gmt_occur_unix;


-------------------- 获取用户上一次的时间点(去除同一个小时的记录)
-- 若当前同一个小时内有多次记录，则认为当前样本在没有去除同一个小时记录时的时间间隔为0，需要获取当前小时内的样本计数
drop table if exists t_sj_test_oppo_time_diff_not_now;


create table t_sj_test_oppo_time_diff_not_now as
select t3.event_id,
       case
           when t4.gmt_occur_unix_last is null then (t3.gmt_occur_unix - 1514995200)/3600
           else (t3.gmt_occur_unix - t4.gmt_occur_unix_last)/3600
       end as gmt_occur_unix_oppo_diff_not_now
from t_sj_test_data_code_unix t3
left outer join
    (select t1.opposing_id,
            t1.gmt_occur_unix,
            max(t2.gmt_occur_unix) as gmt_occur_unix_last
     from
         (select opposing_id,
                 gmt_occur_unix
          from t_sj_test_data_code_unix
          group by opposing_id,
                   gmt_occur_unix)t1
     left outer join
         (select opposing_id,
                 gmt_occur_unix
          from t_sj_test_data_code_unix
          group by opposing_id,
                   gmt_occur_unix)t2 on t1.opposing_id = t2.opposing_id
     where t1.gmt_occur_unix>t2.gmt_occur_unix
     group by t1.opposing_id,
              t1.gmt_occur_unix) t4 on t3.opposing_id = t4.opposing_id
and t3.gmt_occur_unix = t4.gmt_occur_unix;




drop table if exists t_sj_test_feature_oppo;


create table t_sj_test_feature_oppo as
select 
t0.event_id,
t1.oppo_cnt_24h,
t1.oppo_ucnt_user_id_24h,
t1.oppo_ucnt_client_ip_24h,
t1.oppo_ucnt_network_24h,
t1.oppo_ucnt_device_sign_24h,
t1.oppo_ucnt_info_1_24h,
t1.oppo_ucnt_info_2_24h,
t1.oppo_ucnt_ip_prov_24h,
t1.oppo_ucnt_ip_city_24h,
t1.oppo_ucnt_mobile_oper_platform_24h,
t1.oppo_ucnt_operation_channel_24h,
t1.oppo_ucnt_pay_scene_24h,
t1.oppo_sum_amt_24h,

t2.oppo_cnt_1h,
t2.oppo_ucnt_user_id_1h,
t2.oppo_ucnt_client_ip_1h,
t2.oppo_ucnt_network_1h,
t2.oppo_ucnt_device_sign_1h,
t2.oppo_ucnt_info_1_1h,
t2.oppo_ucnt_info_2_1h,
t2.oppo_ucnt_ip_prov_1h,
t2.oppo_ucnt_ip_city_1h,
t2.oppo_ucnt_mobile_oper_platform_1h,
t2.oppo_ucnt_operation_channel_1h,
t2.oppo_ucnt_pay_scene_1h,
t2.oppo_sum_amt_1h,
t3.gmt_occur_unix_oppo_diff_not_now
 from 
(select event_id from t_sj_test_data_code_unix) t0 
left outer join t_sj_test_feature_oppo_24h_not_now t1 on t0.event_id = t1.event_id
left outer join t_sj_test_feature_oppo_1h_not_now t2 on t0.event_id = t2.event_id
left outer join t_sj_test_oppo_time_diff_not_now t3 on t0.event_id = t3.event_id;


-- 去除特征中的缺失值

drop table if exists t_sj_test_feature_oppo_notnull;


create table t_sj_test_feature_oppo_notnull as
select event_id,
       nvl(oppo_cnt_24h, 0) as oppo_cnt_24h,
       nvl(oppo_ucnt_user_id_24h, 0) as oppo_ucnt_user_id_24h,
       nvl(oppo_ucnt_client_ip_24h, 0) as oppo_ucnt_client_ip_24h,
       nvl(oppo_ucnt_network_24h, 0) as oppo_ucnt_network_24h,
       nvl(oppo_ucnt_device_sign_24h, 0) as oppo_ucnt_device_sign_24h,
       nvl(oppo_ucnt_info_1_24h, 0) as oppo_ucnt_info_1_24h,
       nvl(oppo_ucnt_info_2_24h, 0) as oppo_ucnt_info_2_24h,
       nvl(oppo_ucnt_ip_prov_24h, 0) as oppo_ucnt_ip_prov_24h,
       nvl(oppo_ucnt_ip_city_24h, 0) as oppo_ucnt_ip_city_24h,
       nvl(oppo_ucnt_mobile_oper_platform_24h, 0) as oppo_ucnt_mobile_oper_platform_24h,
       nvl(oppo_ucnt_operation_channel_24h, 0) as oppo_ucnt_operation_channel_24h,
       nvl(oppo_ucnt_pay_scene_24h, 0) as oppo_ucnt_pay_scene_24h,
       nvl(oppo_sum_amt_24h, 0) as oppo_sum_amt_24h,
       nvl(oppo_cnt_1h, 0) as oppo_cnt_1h,
       nvl(oppo_ucnt_user_id_1h, 0) as oppo_ucnt_user_id_1h,
       nvl(oppo_ucnt_client_ip_1h, 0) as oppo_ucnt_client_ip_1h,
       nvl(oppo_ucnt_network_1h, 0) as oppo_ucnt_network_1h,
       nvl(oppo_ucnt_device_sign_1h, 0) as oppo_ucnt_device_sign_1h,
       nvl(oppo_ucnt_info_1_1h, 0) as oppo_ucnt_info_1_1h,
       nvl(oppo_ucnt_info_2_1h, 0) as oppo_ucnt_info_2_1h,
       nvl(oppo_ucnt_ip_prov_1h, 0) as oppo_ucnt_ip_prov_1h,
       nvl(oppo_ucnt_ip_city_1h, 0) as oppo_ucnt_ip_city_1h,
       nvl(oppo_ucnt_mobile_oper_platform_1h, 0) as oppo_ucnt_mobile_oper_platform_1h,
       nvl(oppo_ucnt_operation_channel_1h, 0) as oppo_ucnt_operation_channel_1h,
       nvl(oppo_ucnt_pay_scene_1h, 0) as oppo_ucnt_pay_scene_1h,
       nvl(oppo_sum_amt_1h, 0) as oppo_sum_amt_1h,
       gmt_occur_unix_oppo_diff_not_now
from t_sj_test_feature_oppo;
