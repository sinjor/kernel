-- 获取9月份好人在黑产交易小时外的样本
-- 去除了黑样本（1，-1)所在小时内的样本信息，去除同一个小时内好交易的异常干扰
drop table if exists t_sj_train_data_code_y0_m9;


create table t_sj_train_data_code_y0_m9 as
select t4.*
from
    (select t1.user_id,
            t1.gmt_occur
     from
         (select distinct user_id,
                          gmt_occur
          from t_sj_train_data_code
          where is_fraud = 0
              and gmt_occur < '2017-10-05') t1
     left outer join
         (select distinct user_id,
                          gmt_occur
          from t_sj_train_data_code
          where is_fraud != 0
              and gmt_occur < '2017-10-05') t2 on t1.user_id = t2.user_id
     and t1.gmt_occur = t2.gmt_occur
     and t2.user_id is null) t3
left outer join t_sj_train_data_code t4 on t3.user_id = t4.user_id
and t3.gmt_occur = t4.gmt_occur;


-- feature data
drop table if exists t_sj_feature_data_m10;

create table t_sj_feature_data_m10 as
select t1.user_id,
       t1.gmt_occur,
       case
           when t1.is_fraud = -1 or t1.is_fraud = 1 then 1
           else 0
       end as is_fraud,
       case
           when t1.client_ip = t2.uid_most_client_ip_m9 then t2.uid_most_rate_client_ip_cnt_m9
           else 0
       end as uid_habit_client_ip_m9,
       case
           when t1.device_sign = t2.uid_most_device_sign_m9 then t2.uid_most_rate_device_sign_cnt_m9
           else 0
       end as uid_habit_device_sign_m9,
       case
           when t1.ip_prov = t2.uid_most_ip_prov_m9 then t2.uid_most_rate_ip_prov_cnt_m9
           else 0
       end as uid_habit_ip_prov_m9,
       case
           when t1.ip_city = t2.uid_most_ip_city_m9 then t2.uid_most_rate_ip_city_cnt_m9
           else 0
       end as uid_habit_ip_city_m9,
       case
           when t1.cert_prov = t2.uid_most_cert_prov_m9 then t2.uid_most_rate_cert_prov_cnt_m9
           else 0
       end as uid_habit_cert_prov_m9,
       case
           when t1.cert_city = t2.uid_most_cert_city_m9 then t2.uid_most_rate_cert_city_cnt_m9
           else 0
       end as uid_habit_cert_city_m9,
       -- case
       --     when t1.card_bin_prov = t2.uid_most_card_bin_prov_m9 then t2.uid_most_rate_card_bin_prov_cnt_m9
       --     else 0
       -- end as uid_habit_card_bin_prov_m9,
       -- case
       --     when t1.card_bin_city = t2.uid_most_card_bin_city_m9 then t2.uid_most_rate_card_bin_city_cnt_m9
       --     else 0
       -- end as uid_habit_card_bin_city_m9,
       -- case
       --     when t1.card_mobile_prov = t2.uid_most_card_mobile_prov_m9 then t2.uid_most_rate_card_mobile_prov_cnt_m9
       --     else 0
       -- end as uid_habit_card_mobile_prov_m9,
       -- case
       --     when t1.card_mobile_city = t2.uid_most_card_mobile_city_m9 then t2.uid_most_rate_card_mobile_city_cnt_m9
       --     else 0
       -- end as uid_habit_card_mobile_city_m9,
       -- case
       --     when t1.card_cert_prov = t2.uid_most_card_cert_prov_m9 then t2.uid_most_rate_card_cert_prov_cnt_m9
       --     else 0
       -- end as uid_habit_card_cert_prov_m9,
       -- case
       --     when t1.card_cert_city = t2.uid_most_card_cert_city_m9 then t2.uid_most_rate_card_cert_city_cnt_m9
       --     else 0
       -- end as uid_habit_card_cert_city_m9,
       case
           when t1.is_one_people = t2.uid_most_is_one_people_m9 then t2.uid_most_rate_is_one_people_cnt_m9
           else 0
       end as uid_habit_is_one_people_m9,
       case
           when t1.mobile_oper_platform = t2.uid_most_mobile_oper_platform_m9 then t2.uid_most_rate_mobile_oper_platform_cnt_m9
           else 0
       end as uid_habit_mobile_oper_platform_m9,
       case
           when t1.operation_channel = t2.uid_most_operation_channel_m9 then t2.uid_most_rate_operation_channel_cnt_m9
           else 0
       end as uid_habit_operation_channel_m9,
       case
           when t1.pay_scene = t2.uid_most_pay_scene_m9 then t2.uid_most_rate_pay_scene_cnt_m9
           else 0
       end as uid_habit_pay_scene_m9,
       case
           when t1.card_cert_no = t2.uid_most_card_cert_no_m9 then t2.uid_most_rate_card_cert_no_cnt_m9
           else 0
       end as uid_habit_card_cert_no_m9,
       case
           when t1.opposing_id = t2.uid_most_opposing_id_m9 then t2.uid_most_rate_opposing_id_cnt_m9
           else 0
       end as uid_habit_opposing_id_m9,
       case
           when t1.income_card_no = t2.uid_most_income_card_no_m9 then t2.uid_most_rate_income_card_no_cnt_m9
           else 0
       end as uid_habit_income_card_no_m9,
       -- case
       --     when t1.card_mobile = t2.uid_most_card_mobile_m9 then t2.uid_most_rate_card_mobile_cnt_m9
       --     else 0
       -- end as uid_habit_card_mobile_m9,
       -- -- case
       --     when t1.income_card_bank_code = t2.uid_most_income_card_bank_code_m9 then t2.uid_most_rate_income_card_bank_code_cnt_m9
       --     else 0
       -- end as uid_habit_income_card_bank_code_m9,
       -- case
       --     when t1.city = t2.uid_most_city_m9 then t2.uid_most_rate_city_cnt_m9
       --     else 0
       -- end as uid_habit_city_m9,

       case
           when t1.client_ip is null then t2.uid_miss_rate_client_ip_m9
           else 0
       end as uid_miss_cost_client_ip_m9,
       case
           when t1.device_sign is null then t2.uid_miss_rate_device_sign_m9
           else 0
       end as uid_miss_cost_device_sign_m9,
       case
           when t1.network is null then t2.uid_miss_rate_network_m9
           else 0
       end as uid_miss_cost_network_m9,
       case
           when t1.ip_prov is null then t2.uid_miss_rate_ip_prov_m9
           else 0
       end as uid_miss_cost_ip_prov_m9,
       case
           when t1.ip_city is null then t2.uid_miss_rate_ip_city_m9
           else 0
       end as uid_miss_cost_ip_city_m9,
       case
           when t1.cert_prov is null then t2.uid_miss_rate_cert_prov_m9
           else 0
       end as uid_miss_cost_cert_prov_m9,
       case
           when t1.cert_city is null then t2.uid_miss_rate_cert_city_m9
           else 0
       end as uid_miss_cost_cert_city_m9,
       case
           when t1.card_bin_prov is null then t2.uid_miss_rate_card_bin_prov_m9
           else 0
       end as uid_miss_cost_card_bin_prov_m9,
       case
           when t1.card_bin_city is null then t2.uid_miss_rate_card_bin_city_m9
           else 0
       end as uid_miss_cost_card_bin_city_m9,
       case
           when t1.card_mobile_prov is null then t2.uid_miss_rate_card_mobile_prov_m9
           else 0
       end as uid_miss_cost_card_mobile_prov_m9,
       case
           when t1.card_mobile_city is null then t2.uid_miss_rate_card_mobile_city_m9
           else 0
       end as uid_miss_cost_card_mobile_city_m9,
       case
           when t1.card_cert_prov is null then t2.uid_miss_rate_card_cert_prov_m9
           else 0
       end as uid_miss_cost_card_cert_prov_m9,
       case
           when t1.card_cert_city is null then t2.uid_miss_rate_card_cert_city_m9
           else 0
       end as uid_miss_cost_card_cert_city_m9,
       case
           when t1.card_cert_no is null then t2.uid_miss_rate_card_cert_no_m9
           else 0
       end as uid_miss_cost_card_cert_no_m9,
       case
           when t1.income_card_no is null then t2.uid_miss_rate_income_card_no_m9
           else 0
       end as uid_miss_cost_income_card_no_m9,
       case
           when t1.income_card_mobile is null then t2.uid_miss_rate_income_card_mobile_m9
           else 0
       end as uid_miss_cost_income_card_mobile_m9,
       case
           when t1.income_card_bank_code is null then t2.uid_miss_rate_income_card_bank_code_m9
           else 0
       end as uid_miss_cost_income_card_bank_code_m9,
       case
           when t1.city is null then t2.uid_miss_rate_city_m9
           else 0
       end as uid_miss_cost_city_m9
from
    (select *
     from t_sj_train_data_code
     where gmt_occur >='2017-10-05') t1
left outer join t_sj_feature_uid_y0_m9 t2 on t1.user_id = t2.user_id;


drop table if exists t_sj_feature_uid_y0_m9;


create table t_sj_feature_uid_y0_m9 as
select 
t1.user_id,
t1.uid_cnt_instanse,
t1.uid_ucnt_client_ip_m9,
t1.uid_ucnt_device_sign_m9,
t1.uid_ucnt_network_m9,
t1.uid_ucnt_ip_prov_m9,
t1.uid_ucnt_ip_city_m9,
t1.uid_ucnt_cert_prov_m9,
t1.uid_ucnt_cert_city_m9,
t1.uid_ucnt_card_bin_prov_m9,
t1.uid_ucnt_card_bin_city_m9,
t1.uid_ucnt_card_mobile_prov_m9,
t1.uid_ucnt_card_mobile_city_m9,
t1.uid_ucnt_card_cert_prov_m9,
t1.uid_ucnt_card_cert_city_m9,
t1.uid_ucnt_is_one_people_m9,
t1.uid_ucnt_mobile_oper_platform_m9,
t1.uid_ucnt_operation_channel_m9,
t1.uid_ucnt_pay_scene_m9,
t1.uid_ucnt_amt_m9,
t1.uid_ucnt_card_cert_no_m9,
t1.uid_ucnt_opposing_id_m9,
t1.uid_ucnt_income_card_no_m9,
t1.uid_ucnt_income_card_cert_no_m9,
t1.uid_ucnt_income_card_mobile_m9,
t1.uid_ucnt_income_card_bank_code_m9,
t1.uid_ucnt_city_m9,
t1.uid_cnt_client_ip_m9/t1.uid_cnt_instanse as uid_miss_rate_client_ip_m9,
t1.uid_cnt_device_sign_m9/t1.uid_cnt_instanse as uid_miss_rate_device_sign_m9,
t1.uid_cnt_network_m9/t1.uid_cnt_instanse as uid_miss_rate_network_m9,
t1.uid_cnt_ip_prov_m9/t1.uid_cnt_instanse as uid_miss_rate_ip_prov_m9,
t1.uid_cnt_ip_city_m9/t1.uid_cnt_instanse as uid_miss_rate_ip_city_m9,
t1.uid_cnt_cert_prov_m9/t1.uid_cnt_instanse as uid_miss_rate_cert_prov_m9,
t1.uid_cnt_cert_city_m9/t1.uid_cnt_instanse as uid_miss_rate_cert_city_m9,
t1.uid_cnt_card_bin_prov_m9/t1.uid_cnt_instanse as uid_miss_rate_card_bin_prov_m9,
t1.uid_cnt_card_bin_city_m9/t1.uid_cnt_instanse as uid_miss_rate_card_bin_city_m9,
t1.uid_cnt_card_mobile_prov_m9/t1.uid_cnt_instanse as uid_miss_rate_card_mobile_prov_m9,
t1.uid_cnt_card_mobile_city_m9/t1.uid_cnt_instanse as uid_miss_rate_card_mobile_city_m9,
t1.uid_cnt_card_cert_prov_m9/t1.uid_cnt_instanse as uid_miss_rate_card_cert_prov_m9,
t1.uid_cnt_card_cert_city_m9/t1.uid_cnt_instanse as uid_miss_rate_card_cert_city_m9,
-- t1.uid_cnt_is_one_people_m9/t1.uid_cnt_instanse as uid_miss_rate_is_one_people_m9,
-- t1.uid_cnt_mobile_oper_platform_m9/t1.uid_cnt_instanse as uid_miss_rate_mobile_oper_platform_m9,
-- t1.uid_cnt_operation_channel_m9/t1.uid_cnt_instanse as uid_miss_rate_operation_channel_m9,
-- t1.uid_cnt_pay_scene_m9/t1.uid_cnt_instanse as uid_miss_rate_pay_scene_m9,
-- t1.uid_cnt_amt_m9/t1.uid_cnt_instanse as uid_miss_rate_amt_m9,
t1.uid_cnt_card_cert_no_m9/t1.uid_cnt_instanse as uid_miss_rate_card_cert_no_m9,
-- t1.uid_cnt_opposing_id_m9/t1.uid_cnt_instanse as uid_miss_rate_opposing_id_m9,
t1.uid_cnt_income_card_no_m9/t1.uid_cnt_instanse as uid_miss_rate_income_card_no_m9,
t1.uid_cnt_income_card_cert_no_m9/t1.uid_cnt_instanse as uid_miss_rate_income_card_cert_no_m9,
t1.uid_cnt_income_card_mobile_m9/t1.uid_cnt_instanse as uid_miss_rate_income_card_mobile_m9,
t1.uid_cnt_income_card_bank_code_m9/t1.uid_cnt_instanse as uid_miss_rate_income_card_bank_code_m9,
t1.uid_cnt_city_m9/t1.uid_cnt_instanse as uid_miss_rate_city_m9,
t2.uid_most_client_ip_m9,
t2.uid_most_client_ip_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_client_ip_cnt_m9,
t2.uid_most_network_m9,
t2.uid_most_network_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_network_cnt_m9,
t2.uid_most_device_sign_m9,
t2.uid_most_device_sign_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_device_sign_cnt_m9,
t2.uid_most_ip_prov_m9,
t2.uid_most_ip_prov_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_ip_prov_cnt_m9,
t2.uid_most_ip_city_m9,
t2.uid_most_ip_city_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_ip_city_cnt_m9,
t2.uid_most_cert_prov_m9,
t2.uid_most_cert_prov_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_cert_prov_cnt_m9,
t2.uid_most_cert_city_m9,
t2.uid_most_cert_city_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_cert_city_cnt_m9,
t2.uid_most_is_one_people_m9,
t2.uid_most_is_one_people_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_is_one_people_cnt_m9,
t2.uid_most_mobile_oper_platform_m9,
t2.uid_most_mobile_oper_platform_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_mobile_oper_platform_cnt_m9,
t2.uid_most_operation_channel_m9,
t2.uid_most_operation_channel_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_operation_channel_cnt_m9,
t2.uid_most_pay_scene_m9,
t2.uid_most_pay_scene_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_pay_scene_cnt_m9,
t2.uid_most_amt_m9,
t2.uid_most_amt_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_amt_cnt_m9,
t2.uid_most_card_cert_no_m9,
t2.uid_most_card_cert_no_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_card_cert_no_cnt_m9,
t2.uid_most_opposing_id_m9,
t2.uid_most_opposing_id_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_opposing_id_cnt_m9,
t2.uid_most_income_card_no_m9,
t2.uid_most_income_card_no_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_income_card_no_cnt_m9,
t2.uid_most_version_m9,
t2.uid_most_version_cnt_m9/t1.uid_cnt_instanse as uid_most_rate_version_cnt_m9
from 
t_sj_feature_uid_cnt_m9 t1 left outer join
t_sj_feature_uid_habit_m9 t2 on t1.user_id = t2.user_id

-- 同一个sql中不允许出现两个distinct
-- select distinct client_ip, distinct device_sign from t_sj_train_data_code_y0_m9 group by user_id;
-- ucnt:unique count

drop table if exists t_sj_feature_uid_cnt_m9;


create table t_sj_feature_uid_cnt_m9 as
select 
       user_id,
       size(collect_set(client_ip)) as uid_ucnt_client_ip_m9,
       size(collect_set(network)) as uid_ucnt_network_m9,
       size(collect_set(device_sign)) as uid_ucnt_device_sign_m9,
       size(collect_set(ip_prov)) as uid_ucnt_ip_prov_m9,
       size(collect_set(ip_city)) as uid_ucnt_ip_city_m9,
       size(collect_set(cert_prov)) as uid_ucnt_cert_prov_m9,
       size(collect_set(cert_city)) as uid_ucnt_cert_city_m9,
       size(collect_set(card_bin_prov)) as uid_ucnt_card_bin_prov_m9,
       size(collect_set(card_bin_city)) as uid_ucnt_card_bin_city_m9,
       size(collect_set(card_mobile_prov)) as uid_ucnt_card_mobile_prov_m9,
       size(collect_set(card_mobile_city)) as uid_ucnt_card_mobile_city_m9,
       size(collect_set(card_cert_prov)) as uid_ucnt_card_cert_prov_m9,
       size(collect_set(card_cert_city)) as uid_ucnt_card_cert_city_m9,
       size(collect_set(is_one_people)) as uid_ucnt_is_one_people_m9,
       size(collect_set(mobile_oper_platform)) as uid_ucnt_mobile_oper_platform_m9,
       size(collect_set(operation_channel)) as uid_ucnt_operation_channel_m9,
       size(collect_set(pay_scene)) as uid_ucnt_pay_scene_m9,
       size(collect_set(amt)) as uid_ucnt_amt_m9,
       size(collect_set(card_cert_no)) as uid_ucnt_card_cert_no_m9,
       size(collect_set(opposing_id)) as uid_ucnt_opposing_id_m9,
       size(collect_set(income_card_no)) as uid_ucnt_income_card_no_m9,
       size(collect_set(income_card_cert_no)) as uid_ucnt_income_card_cert_no_m9,
       size(collect_set(income_card_mobile)) as uid_ucnt_income_card_mobile_m9,
       size(collect_set(income_card_bank_code)) as uid_ucnt_income_card_bank_code_m9,
       size(collect_set(city)) as uid_ucnt_city_m9,
       count(client_ip) as uid_cnt_client_ip_m9,
       count(network) as uid_cnt_network_m9,
       count(device_sign) as uid_cnt_device_sign_m9,
       count(ip_prov) as uid_cnt_ip_prov_m9,
       count(ip_city) as uid_cnt_ip_city_m9,
       count(cert_prov) as uid_cnt_cert_prov_m9,
       count(cert_city) as uid_cnt_cert_city_m9,
       count(card_bin_prov) as uid_cnt_card_bin_prov_m9,
       count(card_bin_city) as uid_cnt_card_bin_city_m9,
       count(card_mobile_prov) as uid_cnt_card_mobile_prov_m9,
       count(card_mobile_city) as uid_cnt_card_mobile_city_m9,
       count(card_cert_prov) as uid_cnt_card_cert_prov_m9,
       count(card_cert_city) as uid_cnt_card_cert_city_m9,
       count(is_one_people) as uid_cnt_is_one_people_m9,
       count(mobile_oper_platform) as uid_cnt_mobile_oper_platform_m9,
       count(operation_channel) as uid_cnt_operation_channel_m9,
       count(pay_scene) as uid_cnt_pay_scene_m9,
       count(amt) as uid_cnt_amt_m9,
       count(card_cert_no) as uid_cnt_card_cert_no_m9,
       count(opposing_id) as uid_cnt_opposing_id_m9,
       count(income_card_no) as uid_cnt_income_card_no_m9,
       count(income_card_cert_no) as uid_cnt_income_card_cert_no_m9,
       count(income_card_mobile) as uid_cnt_income_card_mobile_m9,
       count(income_card_bank_code) as uid_cnt_income_card_bank_code_m9,
       count(city) as uid_cnt_city_m9,
       count(*) as uid_cnt_instanse
from t_sj_train_data_code_y0_m9
group by user_id



select t2.user_id,
       t2.client_ip as uid_most_client_ip_m9,
       t2.cnt as uid_most_client_ip_cnt_m9
from
    (select t1.user_id,
            t1.client_ip,
            t1.cnt,
            row_number() over(partition by user_id
                              order by t1.cnt desc) as cnt_order
     from
         (select user_id,
                 client_ip,
                 count(*) as cnt
          from t_sj_train_data_code_y0_m9 where client_ip is not null
          group by user_id,
                   client_ip) t1) t2
where t2.cnt_order = 1;


drop table if exists t_sj_feature_uid_habit_m9;


create table t_sj_feature_uid_habit_m9 as
select t0.user_id,
       t1.uid_most_client_ip_m9,
       t1.uid_most_client_ip_cnt_m9,
       t2.uid_most_network_m9,
       t2.uid_most_network_cnt_m9,
       t3.uid_most_device_sign_m9,
       t3.uid_most_device_sign_cnt_m9,
       t4.uid_most_ip_prov_m9,
       t4.uid_most_ip_prov_cnt_m9,
       t5.uid_most_ip_city_m9,
       t5.uid_most_ip_city_cnt_m9,
       t6.uid_most_cert_prov_m9,
       t6.uid_most_cert_prov_cnt_m9,
       t7.uid_most_cert_city_m9,
       t7.uid_most_cert_city_cnt_m9,
       t8.uid_most_is_one_people_m9,
       t8.uid_most_is_one_people_cnt_m9,
       t9.uid_most_mobile_oper_platform_m9,
       t9.uid_most_mobile_oper_platform_cnt_m9,
       t10.uid_most_operation_channel_m9,
       t10.uid_most_operation_channel_cnt_m9,
       t11.uid_most_pay_scene_m9,
       t11.uid_most_pay_scene_cnt_m9,
       t12.uid_most_amt_m9,
       t12.uid_most_amt_cnt_m9,
       t13.uid_most_card_cert_no_m9,
       t13.uid_most_card_cert_no_cnt_m9,
       t14.uid_most_opposing_id_m9,
       t14.uid_most_opposing_id_cnt_m9,
       t15.uid_most_income_card_no_m9,
       t15.uid_most_income_card_no_cnt_m9,
       t16.uid_most_version_m9,
       t16.uid_most_version_cnt_m9
from
    (select distinct user_id
     from t_sj_train_data_code_y0_m9) t0
left outer join
    (select t02.user_id,
            t02.client_ip as uid_most_client_ip_m9,
            t02.cnt as uid_most_client_ip_cnt_m9
     from
         (select t01.user_id,
                 t01.client_ip,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      client_ip,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where client_ip is not null
               group by user_id,
                        client_ip) t01) t02
     where t02.cnt_order = 1) t1 on t0.user_id = t1.user_id
left outer join
    (select t02.user_id,
            t02.network as uid_most_network_m9,
            t02.cnt as uid_most_network_cnt_m9
     from
         (select t01.user_id,
                 t01.network,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      network,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where network is not null
               group by user_id,
                        network) t01) t02
     where t02.cnt_order = 1) t2 on t0.user_id = t2.user_id
left outer join
    (select t02.user_id,
            t02.device_sign as uid_most_device_sign_m9,
            t02.cnt as uid_most_device_sign_cnt_m9
     from
         (select t01.user_id,
                 t01.device_sign,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      device_sign,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where device_sign is not null
               group by user_id,
                        device_sign) t01) t02
     where t02.cnt_order = 1) t3 on t0.user_id = t3.user_id
left outer join
    (select t02.user_id,
            t02.ip_prov as uid_most_ip_prov_m9,
            t02.cnt as uid_most_ip_prov_cnt_m9
     from
         (select t01.user_id,
                 t01.ip_prov,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      ip_prov,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where ip_prov is not null
               group by user_id,
                        ip_prov) t01) t02
     where t02.cnt_order = 1) t4 on t0.user_id = t4.user_id
left outer join
    (select t02.user_id,
            t02.ip_city as uid_most_ip_city_m9,
            t02.cnt as uid_most_ip_city_cnt_m9
     from
         (select t01.user_id,
                 t01.ip_city,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      ip_city,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where ip_city is not null
               group by user_id,
                        ip_city) t01) t02
     where t02.cnt_order = 1) t5 on t0.user_id = t5.user_id
left outer join
    (select t02.user_id,
            t02.cert_prov as uid_most_cert_prov_m9,
            t02.cnt as uid_most_cert_prov_cnt_m9
     from
         (select t01.user_id,
                 t01.cert_prov,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      cert_prov,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where cert_prov is not null
               group by user_id,
                        cert_prov) t01) t02
     where t02.cnt_order = 1) t6 on t0.user_id = t6.user_id
left outer join
    (select t02.user_id,
            t02.cert_city as uid_most_cert_city_m9,
            t02.cnt as uid_most_cert_city_cnt_m9
     from
         (select t01.user_id,
                 t01.cert_city,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      cert_city,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where cert_city is not null
               group by user_id,
                        cert_city) t01) t02
     where t02.cnt_order = 1) t7 on t0.user_id = t7.user_id
left outer join
    (select t02.user_id,
            t02.is_one_people as uid_most_is_one_people_m9,
            t02.cnt as uid_most_is_one_people_cnt_m9
     from
         (select t01.user_id,
                 t01.is_one_people,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      is_one_people,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where is_one_people is not null
               group by user_id,
                        is_one_people) t01) t02
     where t02.cnt_order = 1) t8 on t0.user_id = t8.user_id
left outer join
    (select t02.user_id,
            t02.mobile_oper_platform as uid_most_mobile_oper_platform_m9,
            t02.cnt as uid_most_mobile_oper_platform_cnt_m9
     from
         (select t01.user_id,
                 t01.mobile_oper_platform ,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      mobile_oper_platform ,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where mobile_oper_platform is not null
               group by user_id,
                        mobile_oper_platform) t01) t02
     where t02.cnt_order = 1) t9 on t0.user_id = t9.user_id
left outer join
    (select t02.user_id,
            t02.operation_channel as uid_most_operation_channel_m9,
            t02.cnt as uid_most_operation_channel_cnt_m9
     from
         (select t01.user_id,
                 t01.operation_channel ,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      operation_channel ,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where operation_channel is not null
               group by user_id,
                        operation_channel) t01) t02
     where t02.cnt_order = 1) t10 on t0.user_id = t10.user_id
left outer join
    (select t02.user_id,
            t02.pay_scene as uid_most_pay_scene_m9,
            t02.cnt as uid_most_pay_scene_cnt_m9
     from
         (select t01.user_id,
                 t01.pay_scene ,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      pay_scene ,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where pay_scene is not null
               group by user_id,
                        pay_scene) t01) t02
     where t02.cnt_order = 1) t11 on t0.user_id = t11.user_id
left outer join
    (select t02.user_id,
            t02.amt as uid_most_amt_m9,
            t02.cnt as uid_most_amt_cnt_m9
     from
         (select t01.user_id,
                 t01.amt ,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      amt ,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where amt is not null
               group by user_id,
                        amt) t01) t02
     where t02.cnt_order = 1) t12 on t0.user_id = t12.user_id
left outer join
    (select t02.user_id,
            t02.card_cert_no as uid_most_card_cert_no_m9,
            t02.cnt as uid_most_card_cert_no_cnt_m9
     from
         (select t01.user_id,
                 t01.card_cert_no ,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      card_cert_no ,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where card_cert_no is not null
               group by user_id,
                        card_cert_no) t01) t02
     where t02.cnt_order = 1) t13 on t0.user_id = t13.user_id
left outer join
    (select t02.user_id,
            t02.opposing_id as uid_most_opposing_id_m9,
            t02.cnt as uid_most_opposing_id_cnt_m9
     from
         (select t01.user_id,
                 t01.opposing_id ,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      opposing_id ,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where opposing_id is not null
               group by user_id,
                        opposing_id) t01) t02
     where t02.cnt_order = 1) t14 on t0.user_id = t14.user_id
left outer join
    (select t02.user_id,
            t02.income_card_no as uid_most_income_card_no_m9,
            t02.cnt as uid_most_income_card_no_cnt_m9
     from
         (select t01.user_id,
                 t01.income_card_no ,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      income_card_no ,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where income_card_no is not null
               group by user_id,
                        income_card_no) t01) t02
     where t02.cnt_order = 1) t15 on t0.user_id = t15.user_id
left outer join
    (select t02.user_id,
            t02.version as uid_most_version_m9,
            t02.cnt as uid_most_version_cnt_m9
     from
         (select t01.user_id,
                 t01.version ,
                 t01.cnt,
                 row_number() over(partition by user_id
                                   order by t01.cnt desc) as cnt_order
          from
              (select user_id,
                      version ,
                      count(*) as cnt
               from t_sj_train_data_code_y0_m9
               where version is not null
               group by user_id,
                        version) t01) t02
     where t02.cnt_order = 1) t16 on t0.user_id = t16.user_id;















