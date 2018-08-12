

drop table if exists t_sj_train_feature_user;


create table t_sj_train_feature_user as
select t0.event_id,
       t1.user_cnt_24h,
       t1.user_ucnt_client_ip_24h,
       t1.user_ucnt_network_24h,
       t1.user_ucnt_device_sign_24h,
       t1.user_ucnt_info_1_24h,
       t1.user_ucnt_info_2_24h,
       t1.user_ucnt_ip_prov_24h,
       t1.user_ucnt_ip_city_24h,
       t1.user_ucnt_mobile_oper_platform_24h,
       t1.user_ucnt_operation_channel_24h,
       t1.user_ucnt_pay_scene_24h,
       t1.user_sum_amt_24h,
       t1.user_ucnt_opposing_id_24h,
       t2.user_cnt_1h,
       t2.user_ucnt_client_ip_1h,
       t2.user_ucnt_network_1h,
       t2.user_ucnt_device_sign_1h,
       t2.user_ucnt_info_1_1h,
       t2.user_ucnt_info_2_1h,
       t2.user_ucnt_ip_prov_1h,
       t2.user_ucnt_ip_city_1h,
       t2.user_ucnt_mobile_oper_platform_1h,
       t2.user_ucnt_operation_channel_1h,
       t2.user_ucnt_pay_scene_1h,
       t2.user_sum_amt_1h,
       t2.user_ucnt_opposing_id_1h,
       t3.gmt_occur_unix_user_diff_not_now,
       t4.user_client_ip_rate_24h,
       t4.user_network_rate_24h,
       t4.user_device_sign_rate_24h,
       t4.user_info_1_rate_24h,
       t4.user_info_2_rate_24h,
       t4.user_ip_prov_rate_24h,
       t4.user_ip_city_rate_24h,
       t4.user_mobile_oper_platform_rate_24h,
       t4.user_operation_channel_rate_24h,
       t4.user_pay_scene_rate_24h,
       t4.user_amt_rate_24h,
       t4.user_opposing_id_rate_24h
from
    (select event_id
     from t_sj_train_data_code_unix) t0
left outer join t_sj_train_feature_user_24h_not_now t1 on t0.event_id = t1.event_id
left outer join t_sj_train_feature_user_1h_not_now t2 on t0.event_id = t2.event_id
left outer join t_sj_train_user_time_diff_not_now t3 on t0.event_id = t3.event_id
left outer join t_sj_train_user_scatter_freq_24h t4 on t0.event_id = t4.event_id;


-- 去除特征中的缺失值
-- rate 特征不会为空，24小时内没有使用过该IP，则设为0
-- time_diff特征不会为空，如果数据集中第一次交易，则时间间隔为数据集起始时间（起始时间 - 24小时）至当前样本的时间
-- 24小时内没有交易样本，则24h变量为0
-- 1小时内没有交易样本，则1h变量为0
-- 
drop table if exists t_sj_train_feature_user_notnull;


create table t_sj_train_feature_user_notnull as
select event_id,
       nvl(user_cnt_24h, 0) as user_cnt_24h,
       nvl(user_ucnt_client_ip_24h, 0) as user_ucnt_client_ip_24h,
       nvl(user_ucnt_network_24h, 0) as user_ucnt_network_24h,
       nvl(user_ucnt_device_sign_24h, 0) as user_ucnt_device_sign_24h,
       nvl(user_ucnt_info_1_24h, 0) as user_ucnt_info_1_24h,
       nvl(user_ucnt_info_2_24h, 0) as user_ucnt_info_2_24h,
       nvl(user_ucnt_ip_prov_24h, 0) as user_ucnt_ip_prov_24h,
       nvl(user_ucnt_ip_city_24h, 0) as user_ucnt_ip_city_24h,
       nvl(user_ucnt_mobile_oper_platform_24h, 0) as user_ucnt_mobile_oper_platform_24h,
       nvl(user_ucnt_operation_channel_24h, 0) as user_ucnt_operation_channel_24h,
       nvl(user_ucnt_pay_scene_24h, 0) as user_ucnt_pay_scene_24h,
       nvl(user_sum_amt_24h, 0) as user_sum_amt_24h,
       nvl(user_ucnt_opposing_id_24h, 0) as user_ucnt_opposing_id_24h,
       nvl(user_cnt_1h, 0) as user_cnt_1h,
       nvl(user_ucnt_client_ip_1h, 0) as user_ucnt_client_ip_1h,
       nvl(user_ucnt_network_1h, 0) as user_ucnt_network_1h,
       nvl(user_ucnt_device_sign_1h, 0) as user_ucnt_device_sign_1h,
       nvl(user_ucnt_info_1_1h, 0) as user_ucnt_info_1_1h,
       nvl(user_ucnt_info_2_1h, 0) as user_ucnt_info_2_1h,
       nvl(user_ucnt_ip_prov_1h, 0) as user_ucnt_ip_prov_1h,
       nvl(user_ucnt_ip_city_1h, 0) as user_ucnt_ip_city_1h,
       nvl(user_ucnt_mobile_oper_platform_1h, 0) as user_ucnt_mobile_oper_platform_1h,
       nvl(user_ucnt_operation_channel_1h, 0) as user_ucnt_operation_channel_1h,
       nvl(user_ucnt_pay_scene_1h, 0) as user_ucnt_pay_scene_1h,
       nvl(user_sum_amt_1h, 0) as user_sum_amt_1h,
       nvl(user_ucnt_opposing_id_1h, 0) as user_ucnt_opposing_id_1h,
       case
           when gmt_occur_unix_user_diff_not_now>744 then 744
           else gmt_occur_unix_user_diff_not_now
       end as gmt_occur_unix_user_diff_not_now,
       user_client_ip_rate_24h,
       user_network_rate_24h,
       user_device_sign_rate_24h,
       user_info_1_rate_24h,
       user_info_2_rate_24h,
       user_ip_prov_rate_24h,
       user_ip_city_rate_24h,
       user_mobile_oper_platform_rate_24h,
       user_operation_channel_rate_24h,
       user_pay_scene_rate_24h,
       user_amt_rate_24h,
       user_opposing_id_rate_24h
from t_sj_train_feature_user;

 -----------------------------------device---------------

drop table if exists t_sj_train_feature_device;


create table t_sj_train_feature_device as
select t0.event_id,
       t1.device_cnt_24h,
       t1.device_ucnt_client_ip_24h,
       t1.device_ucnt_network_24h,
       t1.device_ucnt_user_id_24h,
       t1.device_ucnt_info_1_24h,
       t1.device_ucnt_info_2_24h,
       t1.device_ucnt_ip_prov_24h,
       t1.device_ucnt_ip_city_24h,
       t1.device_ucnt_mobile_oper_platform_24h,
       t1.device_ucnt_operation_channel_24h,
       t1.device_ucnt_pay_scene_24h,
       t1.device_sum_amt_24h,
       t1.device_ucnt_opposing_id_24h,
       t2.device_cnt_1h,
       t2.device_ucnt_client_ip_1h,
       t2.device_ucnt_network_1h,
       t2.device_ucnt_user_id_1h,
       t2.device_ucnt_info_1_1h,
       t2.device_ucnt_info_2_1h,
       t2.device_ucnt_ip_prov_1h,
       t2.device_ucnt_ip_city_1h,
       t2.device_ucnt_mobile_oper_platform_1h,
       t2.device_ucnt_operation_channel_1h,
       t2.device_ucnt_pay_scene_1h,
       t2.device_sum_amt_1h,
       t2.device_ucnt_opposing_id_1h,
       t3.gmt_occur_unix_device_diff_not_now,
       t4.device_client_ip_rate_24h,
       t4.device_network_rate_24h,
       t4.device_user_id_rate_24h,
       t4.device_info_1_rate_24h,
       t4.device_info_2_rate_24h,
       t4.device_ip_prov_rate_24h,
       t4.device_ip_city_rate_24h,
       t4.device_mobile_oper_platform_rate_24h,
       t4.device_operation_channel_rate_24h,
       t4.device_pay_scene_rate_24h,
       t4.device_amt_rate_24h,
       t4.device_opposing_id_rate_24h
from
    (select event_id
     from t_sj_train_data_code_unix) t0
left outer join t_sj_train_feature_device_24h_not_now t1 on t0.event_id = t1.event_id
left outer join t_sj_train_feature_device_1h_not_now t2 on t0.event_id = t2.event_id
left outer join t_sj_train_device_time_diff_not_now t3 on t0.event_id = t3.event_id
left outer join t_sj_train_device_scatter_freq_24h t4 on t0.event_id = t4.event_id;

 -- 去除特征中的缺失值
-- rate 特征不会为空，24小时内没有使用过该IP，则设为0
-- time_diff特征不会为空，如果数据集中第一次交易，则时间间隔为数据集起始时间（起始时间 - 24小时）至当前样本的时间
-- 24小时内没有交易样本，则24h变量为0
-- 1小时内没有交易样本，则1h变量为0
--

drop table if exists t_sj_train_feature_device_notnull;


create table t_sj_train_feature_device_notnull as
select event_id,
       nvl(device_cnt_24h, 0) as device_cnt_24h,
       nvl(device_ucnt_client_ip_24h, 0) as device_ucnt_client_ip_24h,
       nvl(device_ucnt_network_24h, 0) as device_ucnt_network_24h,
       nvl(device_ucnt_user_id_24h, 0) as device_ucnt_user_id_24h,
       nvl(device_ucnt_info_1_24h, 0) as device_ucnt_info_1_24h,
       nvl(device_ucnt_info_2_24h, 0) as device_ucnt_info_2_24h,
       nvl(device_ucnt_ip_prov_24h, 0) as device_ucnt_ip_prov_24h,
       nvl(device_ucnt_ip_city_24h, 0) as device_ucnt_ip_city_24h,
       nvl(device_ucnt_mobile_oper_platform_24h, 0) as device_ucnt_mobile_oper_platform_24h,
       nvl(device_ucnt_operation_channel_24h, 0) as device_ucnt_operation_channel_24h,
       nvl(device_ucnt_pay_scene_24h, 0) as device_ucnt_pay_scene_24h,
       nvl(device_sum_amt_24h, 0) as device_sum_amt_24h,
       nvl(device_ucnt_opposing_id_24h, 0) as device_ucnt_opposing_id_24h,
       nvl(device_cnt_1h, 0) as device_cnt_1h,
       nvl(device_ucnt_client_ip_1h, 0) as device_ucnt_client_ip_1h,
       nvl(device_ucnt_network_1h, 0) as device_ucnt_network_1h,
       nvl(device_ucnt_user_id_1h, 0) as device_ucnt_user_id_1h,
       nvl(device_ucnt_info_1_1h, 0) as device_ucnt_info_1_1h,
       nvl(device_ucnt_info_2_1h, 0) as device_ucnt_info_2_1h,
       nvl(device_ucnt_ip_prov_1h, 0) as device_ucnt_ip_prov_1h,
       nvl(device_ucnt_ip_city_1h, 0) as device_ucnt_ip_city_1h,
       nvl(device_ucnt_mobile_oper_platform_1h, 0) as device_ucnt_mobile_oper_platform_1h,
       nvl(device_ucnt_operation_channel_1h, 0) as device_ucnt_operation_channel_1h,
       nvl(device_ucnt_pay_scene_1h, 0) as device_ucnt_pay_scene_1h,
       nvl(device_sum_amt_1h, 0) as device_sum_amt_1h,
       nvl(device_ucnt_opposing_id_1h, 0) as device_ucnt_opposing_id_1h,
       case
           when gmt_occur_unix_device_diff_not_now>744 then 744
           else gmt_occur_unix_device_diff_not_now
       end as gmt_occur_unix_device_diff_not_now,
       device_client_ip_rate_24h,
       device_network_rate_24h,
       device_user_id_rate_24h,
       device_info_1_rate_24h,
       device_info_2_rate_24h,
       device_ip_prov_rate_24h,
       device_ip_city_rate_24h,
       device_mobile_oper_platform_rate_24h,
       device_operation_channel_rate_24h,
       device_pay_scene_rate_24h,
       device_amt_rate_24h,
       device_opposing_id_rate_24h
from t_sj_train_feature_device;

 ----------------------------------------oppo-----------------------------

drop table if exists t_sj_train_feature_oppo;


create table t_sj_train_feature_oppo as
select t0.event_id,
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
    (select event_id
     from t_sj_train_data_code_unix) t0
left outer join t_sj_train_feature_oppo_24h_not_now t1 on t0.event_id = t1.event_id
left outer join t_sj_train_feature_oppo_1h_not_now t2 on t0.event_id = t2.event_id
left outer join t_sj_train_oppo_time_diff_not_now t3 on t0.event_id = t3.event_id;

 -- 去除特征中的缺失值

drop table if exists t_sj_train_feature_oppo_notnull;


create table t_sj_train_feature_oppo_notnull as
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
       case
           when gmt_occur_unix_oppo_diff_not_now>744 then 744
           else gmt_occur_unix_oppo_diff_not_now
       end as gmt_occur_unix_oppo_diff_not_now
from t_sj_train_feature_oppo;

----------------------------------------------all-------------------------------------
drop table if exists t_sj_train_feature_data;


create table t_sj_train_feature_data as
select case
           when t0.is_fraud = 0 then 0
           else 1
       end as label,
       t1.event_id,
       t1.gmt_occur,
       t1.hour,
       t1.hour_bin,
       t1.network,
       -- t1.info_1,
       -- t1.info_2,
       t1.is_one_people,
       t1.mobile_oper_platform,
       t1.operation_channel,
       t1.pay_scene,
       t1.amt,
       case
           when t2.user_cnt_24h = 0 then 0
           else t2.user_cnt_1h / (t2.user_cnt_24h + t2.user_cnt_1h)
       end as user_cnt_1h_div_cnt,
       case
           when t2.user_cnt_1h = 0 then 0
           else t2.user_ucnt_client_ip_1h / t2.user_cnt_1h
       end as user_ucnt_client_ip_1h_div_cnt,
       case
           when t2.user_cnt_1h = 0 then 0
           else t2.user_ucnt_network_1h / t2.user_cnt_1h
       end as user_ucnt_network_1h_div_cnt,
       case
           when t2.user_cnt_1h = 0 then 0
           else t2.user_ucnt_device_sign_1h / t2.user_cnt_1h
       end as user_ucnt_device_sign_1h_div_cnt,
       case
           when t2.user_cnt_1h = 0 then 0
           else t2.user_ucnt_info_1_1h / t2.user_cnt_1h
       end as user_ucnt_info_1_1h_div_cnt,
       case
           when t2.user_cnt_1h = 0 then 0
           else t2.user_ucnt_info_2_1h / t2.user_cnt_1h
       end as user_ucnt_info_2_1h_div_cnt,
       case
           when t2.user_cnt_1h = 0 then 0
           else t2.user_ucnt_ip_prov_1h / t2.user_cnt_1h
       end as user_ucnt_ip_prov_1h_div_cnt,
       case
           when t2.user_cnt_1h = 0 then 0
           else t2.user_ucnt_ip_city_1h / t2.user_cnt_1h
       end as user_ucnt_ip_city_1h_div_cnt,
       case
           when t2.user_cnt_1h = 0 then 0
           else t2.user_ucnt_mobile_oper_platform_1h / t2.user_cnt_1h
       end as user_ucnt_mobile_oper_platform_1h_div_cnt,
       case
           when t2.user_cnt_1h = 0 then 0
           else t2.user_ucnt_operation_channel_1h / t2.user_cnt_1h
       end as user_ucnt_operation_channel_1h_div_cnt,
       case
           when t2.user_cnt_1h = 0 then 0
           else t2.user_ucnt_pay_scene_1h / t2.user_cnt_1h
       end as user_ucnt_pay_scene_1h_div_cnt,
       case
           when t2.user_cnt_1h = 0 then 0
           else t2.user_sum_amt_1h / t2.user_cnt_1h
       end as user_sum_amt_1h_div_cnt,
       case
           when t2.user_cnt_1h = 0 then 0
           else t2.user_ucnt_opposing_id_1h / t2.user_cnt_1h
       end as user_ucnt_opposing_id_1h_div_cnt,

       case
           when t2.user_cnt_24h = 0 then 0
           else t2.user_ucnt_client_ip_24h / t2.user_cnt_24h
       end as user_ucnt_client_ip_24h_div_cnt,
       case
           when t2.user_cnt_24h = 0 then 0
           else t2.user_ucnt_network_24h / t2.user_cnt_24h
       end as user_ucnt_network_24h_div_cnt,
       case
           when t2.user_cnt_24h = 0 then 0
           else t2.user_ucnt_device_sign_24h / t2.user_cnt_24h
       end as user_ucnt_device_sign_24h_div_cnt,
       case
           when t2.user_cnt_24h = 0 then 0
           else t2.user_ucnt_info_1_24h / t2.user_cnt_24h
       end as user_ucnt_info_1_24h_div_cnt,
       case
           when t2.user_cnt_24h = 0 then 0
           else t2.user_ucnt_info_2_24h / t2.user_cnt_24h
       end as user_ucnt_info_2_24h_div_cnt,
       case
           when t2.user_cnt_24h = 0 then 0
           else t2.user_ucnt_ip_prov_24h / t2.user_cnt_24h
       end as user_ucnt_ip_prov_24h_div_cnt,
       case
           when t2.user_cnt_24h = 0 then 0
           else t2.user_ucnt_ip_city_24h / t2.user_cnt_24h
       end as user_ucnt_ip_city_24h_div_cnt,
       case
           when t2.user_cnt_24h = 0 then 0
           else t2.user_ucnt_mobile_oper_platform_24h / t2.user_cnt_24h
       end as user_ucnt_mobile_oper_platform_24h_div_cnt,
       case
           when t2.user_cnt_24h = 0 then 0
           else t2.user_ucnt_operation_channel_24h / t2.user_cnt_24h
       end as user_ucnt_operation_channel_24h_div_cnt,
       case
           when t2.user_cnt_24h = 0 then 0
           else t2.user_ucnt_pay_scene_24h / t2.user_cnt_24h
       end as user_ucnt_pay_scene_24h_div_cnt,
       case
           when t2.user_cnt_24h = 0 then 0
           else t2.user_sum_amt_24h / t2.user_cnt_24h
       end as user_sum_amt_24h_div_cnt,
       case
           when t2.user_cnt_24h = 0 then 0
           else t2.user_ucnt_opposing_id_24h / t2.user_cnt_24h
       end as user_ucnt_opposing_id_24h_div_cnt,
       -- t2.user_cnt_24h,
       -- t2.user_ucnt_client_ip_24h,
       -- t2.user_ucnt_network_24h,
       -- t2.user_ucnt_device_sign_24h,
       -- t2.user_ucnt_info_1_24h,
       -- t2.user_ucnt_info_2_24h,
       -- t2.user_ucnt_ip_prov_24h,
       -- t2.user_ucnt_ip_city_24h,
       -- t2.user_ucnt_mobile_oper_platform_24h,
       -- t2.user_ucnt_operation_channel_24h,
       -- t2.user_ucnt_pay_scene_24h,
       -- t2.user_sum_amt_24h,
       -- t2.user_ucnt_opposing_id_24h,
       -- t2.user_cnt_1h,
       -- t2.user_ucnt_client_ip_1h,
       -- t2.user_ucnt_network_1h,
       -- t2.user_ucnt_device_sign_1h,
       -- t2.user_ucnt_info_1_1h,
       -- t2.user_ucnt_info_2_1h,
       -- t2.user_ucnt_ip_prov_1h,
       -- t2.user_ucnt_ip_city_1h,
       -- t2.user_ucnt_mobile_oper_platform_1h,
       -- t2.user_ucnt_operation_channel_1h,
       -- t2.user_ucnt_pay_scene_1h,
       -- t2.user_sum_amt_1h,
       -- t2.user_ucnt_opposing_id_1h,
       t2.gmt_occur_unix_user_diff_not_now,
       t2.user_client_ip_rate_24h,
       t2.user_network_rate_24h,
       t2.user_device_sign_rate_24h,
       t2.user_info_1_rate_24h,
       t2.user_info_2_rate_24h,
       t2.user_ip_prov_rate_24h,
       t2.user_ip_city_rate_24h,
       t2.user_mobile_oper_platform_rate_24h,
       t2.user_operation_channel_rate_24h,
       t2.user_pay_scene_rate_24h,
       t2.user_amt_rate_24h,
       t2.user_opposing_id_rate_24h,

       t3.device_cnt_24h,
       t3.device_ucnt_client_ip_24h,
       t3.device_ucnt_network_24h,
       t3.device_ucnt_user_id_24h,
       t3.device_ucnt_info_1_24h,
       t3.device_ucnt_info_2_24h,
       t3.device_ucnt_ip_prov_24h,
       t3.device_ucnt_ip_city_24h,
       t3.device_ucnt_mobile_oper_platform_24h,
       t3.device_ucnt_operation_channel_24h,
       t3.device_ucnt_pay_scene_24h,
       t3.device_sum_amt_24h,
       t3.device_ucnt_opposing_id_24h,
       t3.device_cnt_1h,
       t3.device_ucnt_client_ip_1h,
       t3.device_ucnt_network_1h,
       t3.device_ucnt_user_id_1h,
       t3.device_ucnt_info_1_1h,
       t3.device_ucnt_info_2_1h,
       t3.device_ucnt_ip_prov_1h,
       t3.device_ucnt_ip_city_1h,
       t3.device_ucnt_mobile_oper_platform_1h,
       t3.device_ucnt_operation_channel_1h,
       t3.device_ucnt_pay_scene_1h,
       t3.device_sum_amt_1h,
       t3.device_ucnt_opposing_id_1h,
       t3.gmt_occur_unix_device_diff_not_now,
       t3.device_client_ip_rate_24h,
       t3.device_network_rate_24h,
       t3.device_user_id_rate_24h,
       t3.device_info_1_rate_24h,
       t3.device_info_2_rate_24h,
       t3.device_ip_prov_rate_24h,
       t3.device_ip_city_rate_24h,
       t3.device_mobile_oper_platform_rate_24h,
       t3.device_operation_channel_rate_24h,
       t3.device_pay_scene_rate_24h,
       t3.device_amt_rate_24h,
       t3.device_opposing_id_rate_24h,
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
       t4.oppo_sum_amt_24h,
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
       t4.oppo_sum_amt_1h,
       t4.gmt_occur_unix_oppo_diff_not_now
from
    (select event_id,
            is_fraud
     from t_sj_train_data_code_unix) t0
left outer join t_sj_train_feature_base t1 on t1.event_id = t0.event_id
left outer join t_sj_train_feature_user_notnull t2 on t1.event_id = t2.event_id
left outer join t_sj_train_feature_device_notnull t3 on t1.event_id = t3.event_id
left outer join t_sj_train_feature_oppo_notnull t4 on t1.event_id = t4.event_id;


drop table if exists t_sj_train_dataset;

create table t_sj_train_dataset as
select *
from t_sj_train_feature_data
where gmt_occur <'2017-10-20 00'and gmt_occur>'2017-09-06 00';

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


