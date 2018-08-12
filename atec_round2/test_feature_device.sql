-- 字段名称 解释
-- event_id 事件id
factor-- user_id    虚拟用户ID
-- gmt_occur    事件发生时间
factor-- client_ip  用户IP
factor-- network    网络类型 19
factor-- device_sign    设备ID
-- info_1   信息1
-- info_2   信息2
factor-- ip_prov    IP省
factor-- ip_city    IP市
-- cert_prov    证件省
-- cert_city    证件市
-- card_bin_prov    支付卡bin省
-- card_bin_city    支付卡bin市
-- card_mobile_prov 支付账号省
-- card_mobile_city 支付账号市
-- card_cert_prov   支付卡省
-- card_cert_city   支付卡市
factor -- is_one_people 主次双方证件是否一致 2
factor -- mobile_oper_platform  手机操作平台 4
factor-- operation_channel  支付方式 4
factor-- pay_scene  支付场景 21
numeric-- amt   金额
factor-- card_cert_no   虚拟用户证件号 0.48

factor-- opposing_id    对方虚拟用户ID 257个样本一致
factor-- income_card_no 虚拟用户的收款银行卡号 0.91
facror-- income_card_cert_no    虚拟收款用户的证件号 0.99
factor-- income_card_mobile 虚拟收款用户的手机号 0.99
-- income_card_bank_code    收入账号银行代码 0.95
-- province 收入账号归属省份
-- city 虚拟收款用户归属城市
-- is_peer_pay  是否代付 0.98
-- version  版本号
-- is_fraud 预测标签

-- 获取用户相关的特征
-- create table t_sj_device_client_ip_24h as select * from t_device_client_ip_24h;
-- create table t_sj_device_network_24h as select * from t_device_network_24h;
-- create table t_sj_device_user_id_24h as select * from t_device_user_id_24h;
-- create table t_sj_device_ip_prov_24h as select * from t_device_ip_prov_24h;
-- create table t_sj_device_ip_city_24h as select * from t_device_ip_city_24h;
-- create table t_sj_device_mobile_oper_platform_24h as select * from t_device_mobile_oper_platform_24h;
-- create table t_sj_device_operation_channel_24h as select * from t_device_operation_channel_24h;
-- create table t_sj_device_pay_scene_24h as select * from t_device_pay_scene_24h;

drop table if exists t_sj_test_feature_device_24h_not_now;


create table t_sj_test_feature_device_24h_not_now as
select t1.event_id,
       count(*) as device_cnt_24h,
       size(collect_set(t2.user_id)) as device_ucnt_user_id_24h,
       size(collect_set(t2.client_ip)) as device_ucnt_client_ip_24h,
       size(collect_set(t2.network)) as device_ucnt_network_24h,
       size(collect_set(t2.info_1)) as device_ucnt_info_1_24h,
       size(collect_set(t2.info_2)) as device_ucnt_info_2_24h,
       size(collect_set(t2.ip_prov)) as device_ucnt_ip_prov_24h,
       size(collect_set(t2.ip_city)) as device_ucnt_ip_city_24h,
       size(collect_set(t2.mobile_oper_platform)) as device_ucnt_mobile_oper_platform_24h,
       size(collect_set(t2.operation_channel)) as device_ucnt_operation_channel_24h,
       size(collect_set(t2.pay_scene)) as device_ucnt_pay_scene_24h,
       sum(t2.amt) as device_sum_amt_24h,
       size(collect_set(t2.opposing_id)) as device_ucnt_opposing_id_24h
from
    (select event_id,
            device_sign,
            gmt_occur_unix
     from t_sj_test_data_code_unix
     where device_sign is not null) t1
left outer join
    (select event_id,
            device_sign,
            gmt_occur_unix,
            user_id,
            client_ip,
            network,
            info_1,
            info_2,
            ip_prov,
            ip_city,
            mobile_oper_platform,
            operation_channel,
            pay_scene,
            amt,
            opposing_id
     from t_sj_test_data_code_unix
     where device_sign is not null) t2 on t1.device_sign = t2.device_sign
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix < t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id;


-- 一小时内的交易次数 和部分离散变量的去重个数
drop table if exists t_sj_test_feature_device_1h_not_now;


create table t_sj_test_feature_device_1h_not_now as
select t3.event_id,
       t3.device_sign,
       t3.gmt_occur_unix,
       t4.device_cnt_1h,
       t4.device_ucnt_client_ip_1h,
       t4.device_ucnt_network_1h,
       t4.device_ucnt_user_id_1h,
       t4.device_ucnt_info_1_1h,
       t4.device_ucnt_info_2_1h,
       t4.device_ucnt_ip_prov_1h,
       t4.device_ucnt_ip_city_1h,
       t4.device_ucnt_mobile_oper_platform_1h,
       t4.device_ucnt_operation_channel_1h,
       t4.device_ucnt_pay_scene_1h,
       t4.device_sum_amt_1h,
       t4.device_ucnt_opposing_id_1h
from t_sj_test_data_code_unix t3
left outer join
    (select t1.device_sign,
            t1.gmt_occur_unix,
            count(*) as device_cnt_1h,
            size(collect_set(t2.client_ip)) as device_ucnt_client_ip_1h,
            size(collect_set(t2.network)) as device_ucnt_network_1h,
            size(collect_set(t2.user_id)) as device_ucnt_user_id_1h,
            size(collect_set(t2.info_1)) as device_ucnt_info_1_1h,
            size(collect_set(t2.info_2)) as device_ucnt_info_2_1h,
            size(collect_set(t2.ip_prov)) as device_ucnt_ip_prov_1h,
            size(collect_set(t2.ip_city)) as device_ucnt_ip_city_1h,
            size(collect_set(t2.mobile_oper_platform)) as device_ucnt_mobile_oper_platform_1h,
            size(collect_set(t2.operation_channel)) as device_ucnt_operation_channel_1h,
            size(collect_set(t2.pay_scene)) as device_ucnt_pay_scene_1h,
            sum(t2.amt) as device_sum_amt_1h,
            size(collect_set(t2.opposing_id)) as device_ucnt_opposing_id_1h
     from
         (select device_sign,
                 gmt_occur_unix
          from t_sj_test_data_code_unix where device_sign is not null
          group by device_sign,
                   gmt_occur_unix) t1
     left outer join
         (select event_id,
                 device_sign,
                 gmt_occur_unix,
                 client_ip,
                 network,
                 user_id,
                 info_1,
                 info_2,
                 ip_prov,
                 ip_city,
                 mobile_oper_platform,
                 operation_channel,
                 pay_scene,
                 amt,
                 opposing_id
          from t_sj_test_data_code_unix where device_sign is not null) t2 on t1.device_sign = t2.device_sign
     and t1.gmt_occur_unix = t2.gmt_occur_unix
     group by t1.device_sign,
              t1.gmt_occur_unix) t4 on t3.device_sign = t4.device_sign
and t3.gmt_occur_unix = t4.gmt_occur_unix;

drop table if exists t_sj_test_device_time_diff_not_now;


create table t_sj_test_device_time_diff_not_now as
select t3.event_id,
       case
           when t4.gmt_occur_unix_last is null then (t3.gmt_occur_unix - 1514995200)/3600
           else (t3.gmt_occur_unix - t4.gmt_occur_unix_last)/3600
       end as gmt_occur_unix_device_diff_not_now
from t_sj_test_data_code_unix t3
left outer join
    (select t1.device_sign,
            t1.gmt_occur_unix,
            max(t2.gmt_occur_unix) as gmt_occur_unix_last
     from
         (select device_sign,
                 gmt_occur_unix
          from t_sj_test_data_code_unix
          where device_sign is not null
          group by device_sign,
                   gmt_occur_unix)t1
     left outer join
         (select device_sign,
                 gmt_occur_unix
          from t_sj_test_data_code_unix
          where device_sign is not null
          group by device_sign,
                   gmt_occur_unix)t2 on t1.device_sign = t2.device_sign
     where t1.gmt_occur_unix>t2.gmt_occur_unix
     group by t1.device_sign,
              t1.gmt_occur_unix) t4 on t3.device_sign = t4.device_sign
and t3.gmt_occur_unix = t4.gmt_occur_unix;


-- -- 获取用户上一次的时间点 有记录的是9444000 去掉了用户第一次交易的记录
-- drop table if exists t_sj_device_last_time;


-- create table t_sj_device_last_time as
-- select t1.event_id,
--         t2.device_sign,
--        max(t2.gmt_occur_unix) as gmt_occur_unix_last
-- from
--     (select event_id,
--             device_sign,
--             gmt_occur_unix
--      from t_sj_test_data_code_unix where device_sign is not null)t1
-- left outer join
--     (select event_id,
--             device_sign,
--             gmt_occur_unix
--      from t_sj_test_data_code_unix where device_sign is not null)t2 on t1.device_sign = t2.device_sign
-- where t1.gmt_occur_unix>=t2.gmt_occur_unix
--     and t1.event_id !=t2.event_id
-- group by t1.event_id,t2.device_sign;


-- -- 时间差表格
-- drop table if exists t_sj_device_time_diff;


-- create table t_sj_device_time_diff as
-- select t1.event_id,
--        (t1.gmt_occur_unix - t2.gmt_occur_unix_last)/3600 as gmt_occur_unix_device_diff
-- from t_sj_test_data_code_unix t1
-- left outer join t_sj_device_last_time t2 on t1.event_id = t2.event_id;

-- -------------------- 获取用户上一次的时间点(去除同一个小时的记录)
-- -- 6min15s
-- drop table if exists t_sj_device_last_time_not_now;


-- create table t_sj_device_last_time_not_now as
-- select t1.event_id,
--         t2.device_sign,
--        max(t2.gmt_occur_unix) as gmt_occur_unix_last
-- from
--     (select event_id,
--             device_sign,
--             gmt_occur_unix
--      from t_sj_test_data_code_unix where device_sign is not null)t1
-- left outer join
--     (select event_id,
--             device_sign,
--             gmt_occur_unix
--      from t_sj_test_data_code_unix where device_sign is not null)t2 on t1.device_sign = t2.device_sign
-- where t1.gmt_occur_unix>t2.gmt_occur_unix
--     and t1.event_id !=t2.event_id
-- group by t1.event_id,t2.device_sign;

-- -- 时间差表格
-- drop table if exists t_sj_device_time_diff_not_now;


-- create table t_sj_device_time_diff_not_now as
-- select t1.event_id,
--        (t1.gmt_occur_unix - t2.gmt_occur_unix_last)/3600 as gmt_occur_unix_device_diff_not_now
-- from t_sj_test_data_code_unix t1
-- left outer join t_sj_device_last_time_not_now t2 on t1.event_id = t2.event_id;





---------------------------------------------------离散变量的历史出现频率 每张表的执行时间约3min
-- 过去24小时内ip对应的使用个数

drop table if exists t_sj_test_device_client_ip_24h;


create table t_sj_test_device_client_ip_24h as
select t1.event_id,
       t2.client_ip,
       count(*) as device_client_ip_cnt_24h
from
    (select event_id,
            device_sign,
            gmt_occur_unix
     from t_sj_test_data_code_unix
     where device_sign is not null) t1
left outer join
    (select event_id,
            device_sign,
            gmt_occur_unix,
            client_ip
     from t_sj_test_data_code_unix
     where device_sign is not null) t2 on t1.device_sign = t2.device_sign
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix <t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.client_ip;


drop table if exists t_sj_test_device_network_24h;


create table t_sj_test_device_network_24h as
select t1.event_id,
       t2.network,
       count(*) as device_network_cnt_24h
from
    (select event_id,
            device_sign,
            gmt_occur_unix
     from t_sj_test_data_code_unix
     where device_sign is not null) t1
left outer join
    (select event_id,
            device_sign,
            gmt_occur_unix,
            network
     from t_sj_test_data_code_unix
     where device_sign is not null) t2 on t1.device_sign = t2.device_sign
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix <t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.network;


drop table if exists t_sj_test_device_user_id_24h;


create table t_sj_test_device_user_id_24h as
select t1.event_id,
       t2.user_id,
       count(*) as device_user_id_cnt_24h
from
    (select event_id,
            device_sign,
            gmt_occur_unix
     from t_sj_test_data_code_unix
     where device_sign is not null) t1
left outer join
    (select event_id,
            device_sign,
            gmt_occur_unix,
            user_id
     from t_sj_test_data_code_unix
     where device_sign is not null) t2 on t1.device_sign = t2.device_sign
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix <t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.user_id;


drop table if exists t_sj_test_device_info_1_24h;


create table t_sj_test_device_info_1_24h as
select t1.event_id,
       t2.info_1,
       count(*) as device_info_1_cnt_24h
from
    (select event_id,
            device_sign,
            gmt_occur_unix
     from t_sj_test_data_code_unix
     where device_sign is not null) t1
left outer join
    (select event_id,
            device_sign,
            gmt_occur_unix,
            info_1
     from t_sj_test_data_code_unix
     where device_sign is not null) t2 on t1.device_sign = t2.device_sign
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix <t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.info_1;


drop table if exists t_sj_test_device_info_2_24h;


create table t_sj_test_device_info_2_24h as
select t1.event_id,
       t2.info_2,
       count(*) as device_info_2_cnt_24h
from
    (select event_id,
            device_sign,
            gmt_occur_unix
     from t_sj_test_data_code_unix
     where device_sign is not null) t1
left outer join
    (select event_id,
            device_sign,
            gmt_occur_unix,
            info_2
     from t_sj_test_data_code_unix
     where device_sign is not null) t2 on t1.device_sign = t2.device_sign
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix <t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.info_2;


drop table if exists t_sj_test_device_ip_prov_24h;


create table t_sj_test_device_ip_prov_24h as
select t1.event_id,
       t2.ip_prov,
       count(*) as device_ip_prov_cnt_24h
from
    (select event_id,
            device_sign,
            gmt_occur_unix
     from t_sj_test_data_code_unix
     where device_sign is not null) t1
left outer join
    (select event_id,
            device_sign,
            gmt_occur_unix,
            ip_prov
     from t_sj_test_data_code_unix
     where device_sign is not null) t2 on t1.device_sign = t2.device_sign
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix <t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.ip_prov;


drop table if exists t_sj_test_device_ip_city_24h;


create table t_sj_test_device_ip_city_24h as
select t1.event_id,
       t2.ip_city,
       count(*) as device_ip_city_cnt_24h
from
    (select event_id,
            device_sign,
            gmt_occur_unix
     from t_sj_test_data_code_unix
     where device_sign is not null) t1
left outer join
    (select event_id,
            device_sign,
            gmt_occur_unix,
            ip_city
     from t_sj_test_data_code_unix
     where device_sign is not null) t2 on t1.device_sign = t2.device_sign
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix <t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.ip_city;


drop table if exists t_sj_test_device_mobile_oper_platform_24h;


create table t_sj_test_device_mobile_oper_platform_24h as
select t1.event_id,
       t2.mobile_oper_platform,
       count(*) as device_mobile_oper_platform_cnt_24h
from
    (select event_id,
            device_sign,
            gmt_occur_unix
     from t_sj_test_data_code_unix
     where device_sign is not null) t1
left outer join
    (select event_id,
            device_sign,
            gmt_occur_unix,
            mobile_oper_platform
     from t_sj_test_data_code_unix
     where device_sign is not null) t2 on t1.device_sign = t2.device_sign
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix <t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.mobile_oper_platform;


drop table if exists t_sj_test_device_operation_channel_24h;


create table t_sj_test_device_operation_channel_24h as
select t1.event_id,
       t2.operation_channel,
       count(*) as device_operation_channel_cnt_24h
from
    (select event_id,
            device_sign,
            gmt_occur_unix
     from t_sj_test_data_code_unix
     where device_sign is not null) t1
left outer join
    (select event_id,
            device_sign,
            gmt_occur_unix,
            operation_channel
     from t_sj_test_data_code_unix
     where device_sign is not null) t2 on t1.device_sign = t2.device_sign
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix <t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.operation_channel;


drop table if exists t_sj_test_device_pay_scene_24h;


create table t_sj_test_device_pay_scene_24h as
select t1.event_id,
       t2.pay_scene,
       count(*) as device_pay_scene_cnt_24h
from
    (select event_id,
            device_sign,
            gmt_occur_unix
     from t_sj_test_data_code_unix
     where device_sign is not null) t1
left outer join
    (select event_id,
            device_sign,
            gmt_occur_unix,
            pay_scene
     from t_sj_test_data_code_unix
     where device_sign is not null) t2 on t1.device_sign = t2.device_sign
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix <t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.pay_scene;


drop table if exists t_sj_test_device_amt_24h;


create table t_sj_test_device_amt_24h as
select t1.event_id,
       t2.amt,
       count(*) as device_amt_cnt_24h
from
    (select event_id,
            device_sign,
            gmt_occur_unix
     from t_sj_test_data_code_unix
     where device_sign is not null) t1
left outer join
    (select event_id,
            device_sign,
            gmt_occur_unix,
            amt
     from t_sj_test_data_code_unix
     where device_sign is not null) t2 on t1.device_sign = t2.device_sign
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix <t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.amt;


drop table if exists t_sj_test_device_opposing_id_24h;


create table t_sj_test_device_opposing_id_24h as
select t1.event_id,
       t2.opposing_id,
       count(*) as device_opposing_id_cnt_24h
from
    (select event_id,
            device_sign,
            gmt_occur_unix
     from t_sj_test_data_code_unix
     where device_sign is not null) t1
left outer join
    (select event_id,
            device_sign,
            gmt_occur_unix,
            opposing_id
     from t_sj_test_data_code_unix
     where device_sign is not null) t2 on t1.device_sign = t2.device_sign
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix <t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.opposing_id;

---------------------------------------------------离散变量的历史出现频率.
-- 计算每条样本中用户对应的离散变量在当前样本最近24h内的出现频率[0,1],没有出现为0
-- 3min内
drop table if exists t_sj_test_device_scatter_freq_24h;


create table t_sj_test_device_scatter_freq_24h as
select t0.event_id,
       case
           when t2.device_client_ip_cnt_24h is null then 0
           else t2.device_client_ip_cnt_24h/t1.device_cnt_24h
       end as device_client_ip_rate_24h,
       case
           when t3.device_network_cnt_24h is null then 0
           else t3.device_network_cnt_24h/t1.device_cnt_24h
       end as device_network_rate_24h,
       case
           when t4.device_user_id_cnt_24h is null then 0
           else t4.device_user_id_cnt_24h/t1.device_cnt_24h
       end as device_user_id_rate_24h,
       case
           when t5.device_info_1_cnt_24h is null then 0
           else t5.device_info_1_cnt_24h/t1.device_cnt_24h
       end as device_info_1_rate_24h,
       case
           when t6.device_info_2_cnt_24h is null then 0
           else t6.device_info_2_cnt_24h/t1.device_cnt_24h
       end as device_info_2_rate_24h,
       case
           when t7.device_ip_prov_cnt_24h is null then 0
           else t7.device_ip_prov_cnt_24h/t1.device_cnt_24h
       end as device_ip_prov_rate_24h,
       case
           when t8.device_ip_city_cnt_24h is null then 0
           else t8.device_ip_city_cnt_24h/t1.device_cnt_24h
       end as device_ip_city_rate_24h,
       case
           when t9.device_mobile_oper_platform_cnt_24h is null then 0
           else t9.device_mobile_oper_platform_cnt_24h/t1.device_cnt_24h
       end as device_mobile_oper_platform_rate_24h,
       case
           when t10.device_operation_channel_cnt_24h is null then 0
           else t10.device_operation_channel_cnt_24h/t1.device_cnt_24h
       end as device_operation_channel_rate_24h,
       case
           when t11.device_pay_scene_cnt_24h is null then 0
           else t11.device_pay_scene_cnt_24h/t1.device_cnt_24h
       end as device_pay_scene_rate_24h,
       case
           when t12.device_amt_cnt_24h is null then 0
           else t12.device_amt_cnt_24h/t1.device_cnt_24h
       end as device_amt_rate_24h,
       case
           when t13.device_opposing_id_cnt_24h is null then 0
           else t13.device_opposing_id_cnt_24h/t1.device_cnt_24h
       end as device_opposing_id_rate_24h
from t_sj_test_data_code_unix t0
left outer join t_sj_test_feature_device_24h_not_now t1 on t0.event_id = t1.event_id
left outer join t_sj_test_device_client_ip_24h t2 on t0.event_id = t2.event_id
and t0.client_ip = t2.client_ip
left outer join t_sj_test_device_network_24h t3 on t0.event_id = t3.event_id
and t0.network = t3.network
left outer join t_sj_test_device_user_id_24h t4 on t0.event_id = t4.event_id
and t0.user_id = t4.user_id
left outer join t_sj_test_device_info_1_24h t5 on t0.event_id = t5.event_id
and t0.info_1 = t5.info_1
left outer join t_sj_test_device_info_2_24h t6 on t0.event_id = t6.event_id
and t0.info_2 = t6.info_2
left outer join t_sj_test_device_ip_prov_24h t7 on t0.event_id = t7.event_id
and t0.ip_prov = t7.ip_prov
left outer join t_sj_test_device_ip_city_24h t8 on t0.event_id = t8.event_id
and t0.ip_city = t8.ip_city
left outer join t_sj_test_device_mobile_oper_platform_24h t9 on t0.event_id = t9.event_id
and t0.mobile_oper_platform = t9.mobile_oper_platform
left outer join t_sj_test_device_operation_channel_24h t10 on t0.event_id = t10.event_id
and t0.operation_channel = t10.operation_channel
left outer join t_sj_test_device_pay_scene_24h t11 on t0.event_id = t11.event_id
and t0.pay_scene = t11.pay_scene
left outer join t_sj_test_device_amt_24h t12 on t0.event_id = t12.event_id
and t0.amt = t12.amt
left outer join t_sj_test_device_opposing_id_24h t13 on t0.event_id = t13.event_id
and t0.opposing_id = t13.opposing_id;



drop table if exists t_sj_test_feature_device;


create table t_sj_test_feature_device as
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
     from t_sj_test_data_code_unix) t0
left outer join t_sj_test_feature_device_24h_not_now t1 on t0.event_id = t1.event_id
left outer join t_sj_test_feature_device_1h_not_now t2 on t0.event_id = t2.event_id
left outer join t_sj_test_device_time_diff_not_now t3 on t0.event_id = t3.event_id
left outer join t_sj_test_device_scatter_freq_24h t4 on t0.event_id = t4.event_id;


-- 去除特征中的缺失值
-- rate 特征不会为空，24小时内没有使用过该IP，则设为0
-- time_diff特征不会为空，如果数据集中第一次交易，则时间间隔为数据集起始时间（起始时间 - 24小时）至当前样本的时间
-- 24小时内没有交易样本，则24h变量为0
-- 1小时内没有交易样本，则1h变量为0
-- 
drop table if exists t_sj_test_feature_device_notnull;


create table t_sj_test_feature_device_notnull as
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
       gmt_occur_unix_device_diff_not_now,
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
from t_sj_test_feature_device;

--------------------------------------用户id特征统计 end------------------------------------------
-- 查看缺失值分布情况
select count(*) as cnt,
       count(device_cnt_24h) as device_cnt_24h,
       count(device_ucnt_client_ip_24h) as device_ucnt_client_ip_24h,
       count(device_ucnt_network_24h) as device_ucnt_network_24h,
       count(device_ucnt_user_id_24h) as device_ucnt_user_id_24h,
       count(device_ucnt_info_1_24h) as device_ucnt_info_1_24h,
       count(device_ucnt_info_2_24h) as device_ucnt_info_2_24h,
       count(device_ucnt_ip_prov_24h) as device_ucnt_ip_prov_24h,
       count(device_ucnt_ip_city_24h) as device_ucnt_ip_city_24h,
       count(device_ucnt_mobile_oper_platform_24h) as device_ucnt_mobile_oper_platform_24h,
       count(device_ucnt_operation_channel_24h) as device_ucnt_operation_channel_24h,
       count(device_ucnt_pay_scene_24h) as device_ucnt_pay_scene_24h,
       count(device_sum_amt_24h) as device_sum_amt_24h,
       count(device_ucnt_opposing_id_24h) as device_ucnt_opposing_id_24h,
       count(device_cnt_1h) as device_cnt_1h,
       count(device_ucnt_client_ip_1h) as device_ucnt_client_ip_1h,
       count(device_ucnt_network_1h) as device_ucnt_network_1h,
       count(device_ucnt_user_id_1h) as device_ucnt_user_id_1h,
       count(device_ucnt_info_1_1h) as device_ucnt_info_1_1h,
       count(device_ucnt_info_2_1h) as device_ucnt_info_2_1h,
       count(device_ucnt_ip_prov_1h) as device_ucnt_ip_prov_1h,
       count(device_ucnt_ip_city_1h) as device_ucnt_ip_city_1h,
       count(device_ucnt_mobile_oper_platform_1h) as device_ucnt_mobile_oper_platform_1h,
       count(device_ucnt_operation_channel_1h) as device_ucnt_operation_channel_1h,
       count(device_ucnt_pay_scene_1h) as device_ucnt_pay_scene_1h,
       count(device_sum_amt_1h) as device_sum_amt_1h,
       count(device_ucnt_opposing_id_1h) as device_ucnt_opposing_id_1h,
       count(gmt_occur_unix_device_diff_not_now) as gmt_occur_unix_device_diff_not_now,
       count(device_client_ip_rate_24h) as device_client_ip_rate_24h,
       count(device_network_rate_24h) as device_network_rate_24h,
       count(device_user_id_rate_24h) as device_user_id_rate_24h,
       count(device_info_1_rate_24h) as device_info_1_rate_24h,
       count(device_info_2_rate_24h) as device_info_2_rate_24h,
       count(device_ip_prov_rate_24h) as device_ip_prov_rate_24h,
       count(device_ip_city_rate_24h) as device_ip_city_rate_24h,
       count(device_mobile_oper_platform_rate_24h) as device_mobile_oper_platform_rate_24h,
       count(device_operation_channel_rate_24h) as device_operation_channel_rate_24h,
       count(device_pay_scene_rate_24h) as device_pay_scene_rate_24h,
       count(device_amt_rate_24h) as device_amt_rate_24h,
       count(device_opposing_id_rate_24h) as device_opposing_id_rate_24h
from t_sj_test_feature_device;

--------------------------------------用户id特征统计 end------------------------------------------

