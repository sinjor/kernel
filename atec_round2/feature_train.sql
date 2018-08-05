drop table if exists t_sj_train_feature_data;

create table t_sj_train_feature_data as
select t1.event_id,
       t1.gmt_occur,
       t1.hour,
       t1.hour_bin,
       t1.network,
       t1.info_1,
       t1.info_2,
       t1.is_one_people,
       t1.mobile_oper_platform,
       t1.operation_channel,
       t1.pay_scene,
       t1.amt,

       t2.id_cnt_24h,
       t2.id_ucnt_device_sign_24h,
       t2.id_ucnt_network_24h,
       t2.id_ucnt_client_ip_24h,
       t2.id_ucnt_ip_prov_24h,
       t2.id_ucnt_ip_city_24h,
       t2.id_ucnt_operation_channel_24h,
       t2.id_ucnt_pay_scene_24h,
       t2.id_cnt_1h,
       t2.id_ucnt_device_sign_1h,
       t2.id_ucnt_network_1h,
       t2.id_ucnt_client_ip_1h,
       t2.id_ucnt_ip_prov_1h,
       t2.id_ucnt_ip_city_1h,
       t2.id_ucnt_operation_channel_1h,
       t2.id_ucnt_pay_scene_1h,
       t2.gmt_occur_unix_diff,
       t2.gmt_occur_unix_diff_not_now,
       t2.uid_client_ip_rate_24h,
       t2.uid_network_rate_24h,
       t2.uid_device_sign_rate_24h,
       t2.uid_ip_prov_rate_24h,
       t2.uid_ip_city_rate_24h,
       t2.uid_mobile_oper_platform_rate_24h,
       t2.uid_operation_channel_rate_24h,
       t2.uid_pay_scene_rate_24h,

       case
           when t3.is_fraud = 0 then 0
           else 1
       end as label
from t_sj_feature_base t1
left outer join t_sj_feature_uid_notnull t2 on t1.event_id = t2.event_id
left outer join
    (select event_id,
            is_fraud
     from t_sj_train_data_code_unix) t3 on t1.event_id = t3.event_id;

drop table if exists t_sj_train_dataset;

create table t_sj_train_dataset as
select *
from t_sj_train_feature_data
where gmt_occur <'2017-10-20 00';

drop table if exists t_sj_tset_dataset;

create table t_sj_tset_dataset as
select *
from t_sj_train_feature_data
where gmt_occur >='2017-10-20 00';

select count(*),
       count(*)/ 9600000
from t_sj_train_feature_data
where gmt_occur >'2017-10-20 00'

select count(event_id) as event_id_cnt,
       count(hour) as hour_cnt,
       count(hour_bin) as hour_bin_cnt,
       count(network) as network_cnt,
       count(info_1) as info_1_cnt,
       count(info_2) as info_2_cnt,
       count(is_one_people) as is_one_people_cnt,
       count(mobile_oper_platform) as mobile_oper_platform_cnt,
       count(operation_channel) as operation_channel_cnt,
       count(pay_scene) as pay_scene_cnt,
       count(amt) as amt_cnt
from t_sj_train_feature_data;


