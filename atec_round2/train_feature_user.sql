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
-- 24小时内的交易数，和部分离散变量的去重个数（不包括当前小时内数据）
drop table if exists t_sj_train_feature_user_24h_not_now;


create table t_sj_train_feature_user_24h_not_now as
select t1.event_id,
       count(*) as user_cnt_24h,
       size(collect_set(t2.client_ip)) as user_ucnt_client_ip_24h,
       size(collect_set(t2.network)) as user_ucnt_network_24h,
       size(collect_set(t2.device_sign)) as user_ucnt_device_sign_24h,
       size(collect_set(t2.info_1)) as user_ucnt_info_1_24h,
       size(collect_set(t2.info_2)) as user_ucnt_info_2_24h,
       size(collect_set(t2.ip_prov)) as user_ucnt_ip_prov_24h,
       size(collect_set(t2.ip_city)) as user_ucnt_ip_city_24h,
       size(collect_set(t2.mobile_oper_platform)) as user_ucnt_mobile_oper_platform_24h,
       size(collect_set(t2.operation_channel)) as user_ucnt_operation_channel_24h,
       size(collect_set(t2.pay_scene)) as user_ucnt_pay_scene_24h,
       sum(t2.amt) as user_sum_amt_24h,
       size(collect_set(t2.opposing_id)) as user_ucnt_opposing_id_24h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
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
            amt,
            opposing_id
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix < t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id;

 -- 样本当前一小时内的交易次数 和部分离散变量的去重个数
drop table if exists t_sj_train_feature_user_1h_not_now;


create table t_sj_train_feature_user_1h_not_now as
select t3.event_id,
       t3.user_id,
       t3.gmt_occur_unix,
       t4.user_cnt_1h,
       t4.user_ucnt_client_ip_1h,
       t4.user_ucnt_network_1h,
       t4.user_ucnt_device_sign_1h,
       t4.user_ucnt_info_1_1h,
       t4.user_ucnt_info_2_1h,
       t4.user_ucnt_ip_prov_1h,
       t4.user_ucnt_ip_city_1h,
       t4.user_ucnt_mobile_oper_platform_1h,
       t4.user_ucnt_operation_channel_1h,
       t4.user_ucnt_pay_scene_1h,
       t4.user_sum_amt_1h,
       t4.user_ucnt_opposing_id_1h
from t_sj_train_data_code_unix t3
left outer join
    (select t1.user_id,
            t1.gmt_occur_unix,
            count(*) as user_cnt_1h,
            size(collect_set(t2.client_ip)) as user_ucnt_client_ip_1h,
            size(collect_set(t2.network)) as user_ucnt_network_1h,
            size(collect_set(t2.device_sign)) as user_ucnt_device_sign_1h,
            size(collect_set(t2.info_1)) as user_ucnt_info_1_1h,
            size(collect_set(t2.info_2)) as user_ucnt_info_2_1h,
            size(collect_set(t2.ip_prov)) as user_ucnt_ip_prov_1h,
            size(collect_set(t2.ip_city)) as user_ucnt_ip_city_1h,
            size(collect_set(t2.mobile_oper_platform)) as user_ucnt_mobile_oper_platform_1h,
            size(collect_set(t2.operation_channel)) as user_ucnt_operation_channel_1h,
            size(collect_set(t2.pay_scene)) as user_ucnt_pay_scene_1h,
            sum(t2.amt) as user_sum_amt_1h,
            size(collect_set(t2.opposing_id)) as user_ucnt_opposing_id_1h
     from
         (select user_id,
                 gmt_occur_unix
          from t_sj_train_data_code_unix
          group by user_id,
                   gmt_occur_unix) t1
     left outer join
         (select event_id,
                 user_id,
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
                 opposing_id
          from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
     and t1.gmt_occur_unix = t2.gmt_occur_unix
     group by t1.user_id,
              t1.gmt_occur_unix) t4 on t3.user_id = t4.user_id
and t3.gmt_occur_unix = t4.gmt_occur_unix;

-- drop table if exists t_sj_train_feature_user_1h_not_now;


-- create table t_sj_train_feature_user_1h_not_now as
-- select t1.event_id,
--        count(*) as user_cnt_1h,
--        size(collect_set(t2.client_ip)) as user_ucnt_client_ip_1h,
--        size(collect_set(t2.network)) as user_ucnt_network_1h,
--        size(collect_set(t2.device_sign)) as user_ucnt_device_sign_1h,
--        size(collect_set(t2.info_1)) as user_ucnt_info_1_1h,
--        size(collect_set(t2.info_2)) as user_ucnt_info_2_1h,
--        size(collect_set(t2.ip_prov)) as user_ucnt_ip_prov_1h,
--        size(collect_set(t2.ip_city)) as user_ucnt_ip_city_1h,
--        size(collect_set(t2.mobile_oper_platform)) as user_ucnt_mobile_oper_platform_1h,
--        size(collect_set(t2.operation_channel)) as user_ucnt_operation_channel_1h,
--        size(collect_set(t2.pay_scene)) as user_ucnt_pay_scene_1h,
--        sum(t2.amt) as user_sum_amt_1h,
--        size(collect_set(t2.opposing_id)) as user_ucnt_opposing_id_1h
-- from
--     (select event_id,
--             user_id,
--             gmt_occur_unix
--      from t_sj_train_data_code_unix) t1
-- left outer join
--     (select event_id,
--             gmt_occur_unix,
--             user_id,
--             client_ip,
--             network,
--             device_sign,
--             info_1,
--             info_2,
--             ip_prov,
--             ip_city,
--             mobile_oper_platform,
--             operation_channel,
--             pay_scene,
--             amt,
--             opposing_id
--      from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
-- where t2.gmt_occur_unix = t1.gmt_occur_unix
--     and t1.event_id != t2.event_id
-- group by t1.event_id;

-- 用户上一次交易至今的时间间隔，如果在数据集中是第一次交易，那么时间间隔即为当前时间点减去起始时间点（数据集前一天），确保模拟的时间超过24小时
drop table if exists t_sj_train_user_time_diff_not_now;


create table t_sj_train_user_time_diff_not_now as
select t3.event_id,
       case
           when t4.gmt_occur_unix_last is null then (t3.gmt_occur_unix - 1504454400)/3600
           else (t3.gmt_occur_unix - t4.gmt_occur_unix_last)/3600
       end as gmt_occur_unix_user_diff_not_now
from t_sj_train_data_code_unix t3
left outer join
    (select t1.user_id,
            t1.gmt_occur_unix,
            max(t2.gmt_occur_unix) as gmt_occur_unix_last
     from
         (select user_id,
                 gmt_occur_unix
          from t_sj_train_data_code_unix
          group by user_id,
                   gmt_occur_unix)t1
     left outer join
         (select user_id,
                 gmt_occur_unix
          from t_sj_train_data_code_unix
          group by user_id,
                   gmt_occur_unix)t2 on t1.user_id = t2.user_id
     where t1.gmt_occur_unix>t2.gmt_occur_unix
     group by t1.user_id,
              t1.gmt_occur_unix) t4 on t3.user_id = t4.user_id
and t3.gmt_occur_unix = t4.gmt_occur_unix;

 -- -- 获取用户上一次的时间点 有记录的是9444000 去掉了用户第一次交易的记录
-- drop table if exists t_sj_user_last_time;
 -- create table t_sj_user_last_time as
-- select t1.event_id,
--         t2.user_id,
--        max(t2.gmt_occur_unix) as gmt_occur_unix_last
-- from
--     (select event_id,
--             user_id,
--             gmt_occur_unix
--      from t_sj_train_data_code_unix)t1
-- left outer join
--     (select event_id,
--             user_id,
--             gmt_occur_unix
--      from t_sj_train_data_code_unix)t2 on t1.user_id = t2.user_id
-- where t1.gmt_occur_unix>=t2.gmt_occur_unix
--     and t1.event_id !=t2.event_id
-- group by t1.event_id,t2.user_id;
 -- -- 时间差表格
-- drop table if exists t_sj_user_time_diff;
 -- create table t_sj_user_time_diff as
-- select t1.event_id,
--        t1.is_fraud,
--        (t1.gmt_occur_unix - t2.gmt_occur_unix_last)/3600 as gmt_occur_unix_diff
-- from t_sj_train_data_code_unix t1
-- left outer join t_sj_user_last_time t2 on t1.event_id = t2.event_id
 -- -------------------- 获取用户上一次的时间点(去除同一个小时的记录)
-- -- 6min15s
-- drop table if exists t_sj_user_last_time_not_now;
 -- create table t_sj_user_last_time_not_now as
-- select t1.event_id,
--         t2.user_id,
--        max(t2.gmt_occur_unix) as gmt_occur_unix_last
-- from
--     (select event_id,
--             user_id,
--             gmt_occur_unix
--      from t_sj_train_data_code_unix)t1
-- left outer join
--     (select event_id,
--             user_id,
--             gmt_occur_unix
--      from t_sj_train_data_code_unix)t2 on t1.user_id = t2.user_id
-- where t1.gmt_occur_unix>t2.gmt_occur_unix
--     and t1.event_id !=t2.event_id
-- group by t1.event_id,t2.user_id;
 -- -- 时间差表格
-- drop table if exists t_sj_user_time_diff_not_now;
 -- create table t_sj_user_time_diff_not_now as
-- select t1.event_id,
--        case
--            when t1.is_fraud = 0 then 0 then 1 as t1.is_fraud
--        end,
--        -- t1.is_fraud,
 --        (t1.gmt_occur_unix - t2.gmt_occur_unix_last)/3600 as gmt_occur_unix_diff_not_now
-- from t_sj_train_data_code_unix t1
-- left outer join t_sj_user_last_time_not_now t2 on t1.event_id = t2.event_id
 ---------------------------------------------------离散变量的历史出现频率 每张表的执行时间约3min
-- 过去24小时内ip对应的使用个数

drop table if exists t_sj_train_user_client_ip_24h;


create table t_sj_train_user_client_ip_24h as
select t1.event_id,
       t2.client_ip,
       count(*) as user_client_ip_cnt_24h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            client_ip
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix < t1.gmt_occur_unix
group by t1.event_id,
         t2.client_ip;


drop table if exists t_sj_train_user_network_24h;


create table t_sj_train_user_network_24h as
select t1.event_id,
       t2.network,
       count(*) as user_network_cnt_24h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            network
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix < t1.gmt_occur_unix
group by t1.event_id,
         t2.network;


drop table if exists t_sj_train_user_device_sign_24h;


create table t_sj_train_user_device_sign_24h as
select t1.event_id,
       t2.device_sign,
       count(*) as user_device_sign_cnt_24h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            device_sign
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix < t1.gmt_occur_unix
group by t1.event_id,
         t2.device_sign;


drop table if exists t_sj_train_user_info_1_24h;


create table t_sj_train_user_info_1_24h as
select t1.event_id,
       t2.info_1,
       count(*) as user_info_1_cnt_24h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            info_1
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix < t1.gmt_occur_unix
group by t1.event_id,
         t2.info_1;


drop table if exists t_sj_train_user_info_2_24h;


create table t_sj_train_user_info_2_24h as
select t1.event_id,
       t2.info_2,
       count(*) as user_info_2_cnt_24h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            info_2
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix < t1.gmt_occur_unix
group by t1.event_id,
         t2.info_2;


drop table if exists t_sj_train_user_ip_prov_24h;


create table t_sj_train_user_ip_prov_24h as
select t1.event_id,
       t2.ip_prov,
       count(*) as user_ip_prov_cnt_24h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            ip_prov
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix < t1.gmt_occur_unix
group by t1.event_id,
         t2.ip_prov;


drop table if exists t_sj_train_user_ip_city_24h;


create table t_sj_train_user_ip_city_24h as
select t1.event_id,
       t2.ip_city,
       count(*) as user_ip_city_cnt_24h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            ip_city
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix < t1.gmt_occur_unix
group by t1.event_id,
         t2.ip_city;


drop table if exists t_sj_train_user_mobile_oper_platform_24h;


create table t_sj_train_user_mobile_oper_platform_24h as
select t1.event_id,
       t2.mobile_oper_platform,
       count(*) as user_mobile_oper_platform_cnt_24h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            mobile_oper_platform
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix < t1.gmt_occur_unix
group by t1.event_id,
         t2.mobile_oper_platform;


drop table if exists t_sj_train_user_operation_channel_24h;


create table t_sj_train_user_operation_channel_24h as
select t1.event_id,
       t2.operation_channel,
       count(*) as user_operation_channel_cnt_24h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            operation_channel
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix < t1.gmt_occur_unix
group by t1.event_id,
         t2.operation_channel;


drop table if exists t_sj_train_user_pay_scene_24h;


create table t_sj_train_user_pay_scene_24h as
select t1.event_id,
       t2.pay_scene,
       count(*) as user_pay_scene_cnt_24h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            pay_scene
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix < t1.gmt_occur_unix
group by t1.event_id,
         t2.pay_scene;


drop table if exists t_sj_train_user_amt_24h;


create table t_sj_train_user_amt_24h as
select t1.event_id,
       t2.amt,
       count(*) as user_amt_cnt_24h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            amt
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix < t1.gmt_occur_unix
group by t1.event_id,
         t2.amt;

drop table if exists t_sj_train_user_opposing_id_24h;


create table t_sj_train_user_opposing_id_24h as
select t1.event_id,
       t2.opposing_id,
       count(*) as user_opposing_id_cnt_24h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            opposing_id
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where (t1.gmt_occur_unix - t2.gmt_occur_unix) <= 86400
    and t2.gmt_occur_unix < t1.gmt_occur_unix
group by t1.event_id,
         t2.opposing_id;
---------------------------------------------------离散变量的历史出现频率.


-- 计算每条样本中用户对应的离散变量在当前样本最近24h内的出现频率[0,1],没有出现为0
-- 3min内
-- 计算每条样本中用户对应的离散变量在当前样本最近24h内的出现频率[0,1],没有出现为0
-- t_sj_train_user_client_ip_24h中记录了用户在24小时内ip的使用次数，user_client_ip_cnt_24h必然大于0
-- 如果用户在24小时内没有交易，或者当前的ip在24小时内没有使用过，则left join时t2.user_client_ip_cnt_24h是空值
-- user_client_ip_cnt_24h 与 user_cnt_24h的时间周期需要一致，不然会出现user_client_ip_cnt_24h非空，但是user_cnt_24h为空的情况
-- 导致user_client_ip_rate_24h出现空值
drop table if exists t_sj_train_user_scatter_freq_24h;


create table t_sj_train_user_scatter_freq_24h as
select t0.event_id,
       case
           when t2.user_client_ip_cnt_24h is null then 0
           else t2.user_client_ip_cnt_24h/t1.user_cnt_24h
       end as user_client_ip_rate_24h,
       case
           when t3.user_network_cnt_24h is null then 0
           else t3.user_network_cnt_24h/t1.user_cnt_24h
       end as user_network_rate_24h,
       case
           when t4.user_device_sign_cnt_24h is null then 0
           else t4.user_device_sign_cnt_24h/t1.user_cnt_24h
       end as user_device_sign_rate_24h,
       case
           when t5.user_info_1_cnt_24h is null then 0
           else t5.user_info_1_cnt_24h/t1.user_cnt_24h
       end as user_info_1_rate_24h,
       case
           when t6.user_info_2_cnt_24h is null then 0
           else t6.user_info_2_cnt_24h/t1.user_cnt_24h
       end as user_info_2_rate_24h,
       case
           when t7.user_ip_prov_cnt_24h is null then 0
           else t7.user_ip_prov_cnt_24h/t1.user_cnt_24h
       end as user_ip_prov_rate_24h,
       case
           when t8.user_ip_city_cnt_24h is null then 0
           else t8.user_ip_city_cnt_24h/t1.user_cnt_24h
       end as user_ip_city_rate_24h,
       case
           when t9.user_mobile_oper_platform_cnt_24h is null then 0
           else t9.user_mobile_oper_platform_cnt_24h/t1.user_cnt_24h
       end as user_mobile_oper_platform_rate_24h,
       case
           when t10.user_operation_channel_cnt_24h is null then 0
           else t10.user_operation_channel_cnt_24h/t1.user_cnt_24h
       end as user_operation_channel_rate_24h,
       case
           when t11.user_pay_scene_cnt_24h is null then 0
           else t11.user_pay_scene_cnt_24h/t1.user_cnt_24h
       end as user_pay_scene_rate_24h,
       case
           when t12.user_amt_cnt_24h is null then 0
           else t12.user_amt_cnt_24h/t1.user_cnt_24h
       end as user_amt_rate_24h,
       case
           when t13.user_opposing_id_cnt_24h is null then 0
           else t13.user_opposing_id_cnt_24h/t1.user_cnt_24h
       end as user_opposing_id_rate_24h
from t_sj_train_data_code_unix t0
left outer join t_sj_train_feature_user_24h_not_now t1 on t0.event_id = t1.event_id
left outer join t_sj_train_user_client_ip_24h t2 on t0.event_id = t2.event_id
and t0.client_ip = t2.client_ip
left outer join t_sj_train_user_network_24h t3 on t0.event_id = t3.event_id
and t0.network = t3.network
left outer join t_sj_train_user_device_sign_24h t4 on t0.event_id = t4.event_id
and t0.device_sign = t4.device_sign
left outer join t_sj_train_user_info_1_24h t5 on t0.event_id = t5.event_id
and t0.info_1 = t5.info_1
left outer join t_sj_train_user_info_2_24h t6 on t0.event_id = t6.event_id
and t0.info_2 = t6.info_2
left outer join t_sj_train_user_ip_prov_24h t7 on t0.event_id = t7.event_id
and t0.ip_prov = t7.ip_prov
left outer join t_sj_train_user_ip_city_24h t8 on t0.event_id = t8.event_id
and t0.ip_city = t8.ip_city
left outer join t_sj_train_user_mobile_oper_platform_24h t9 on t0.event_id = t9.event_id
and t0.mobile_oper_platform = t9.mobile_oper_platform
left outer join t_sj_train_user_operation_channel_24h t10 on t0.event_id = t10.event_id
and t0.operation_channel = t10.operation_channel
left outer join t_sj_train_user_pay_scene_24h t11 on t0.event_id = t11.event_id
and t0.pay_scene = t11.pay_scene
left outer join t_sj_train_user_amt_24h t12 on t0.event_id = t12.event_id
and t0.amt = t12.amt
left outer join t_sj_train_user_opposing_id_24h t13 on t0.event_id = t13.event_id
and t0.opposing_id = t13.opposing_id;



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
       gmt_occur_unix_user_diff_not_now,
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

--------------------------------------用户id特征统计 end------------------------------------------
-- 查看缺失值分布情况
select count(*) as cnt,
       count(user_cnt_24h) as user_cnt_24h,
       count(user_ucnt_client_ip_24h) as user_ucnt_client_ip_24h,
       count(user_ucnt_network_24h) as user_ucnt_network_24h,
       count(user_ucnt_device_sign_24h) as user_ucnt_device_sign_24h,
       count(user_ucnt_info_1_24h) as user_ucnt_info_1_24h,
       count(user_ucnt_info_2_24h) as user_ucnt_info_2_24h,
       count(user_ucnt_ip_prov_24h) as user_ucnt_ip_prov_24h,
       count(user_ucnt_ip_city_24h) as user_ucnt_ip_city_24h,
       count(user_ucnt_mobile_oper_platform_24h) as user_ucnt_mobile_oper_platform_24h,
       count(user_ucnt_operation_channel_24h) as user_ucnt_operation_channel_24h,
       count(user_ucnt_pay_scene_24h) as user_ucnt_pay_scene_24h,
       count(user_sum_amt_24h) as user_sum_amt_24h,
       count(user_ucnt_opposing_id_24h) as user_ucnt_opposing_id_24h,
       count(user_cnt_1h) as user_cnt_1h,
       count(user_ucnt_client_ip_1h) as user_ucnt_client_ip_1h,
       count(user_ucnt_network_1h) as user_ucnt_network_1h,
       count(user_ucnt_device_sign_1h) as user_ucnt_device_sign_1h,
       count(user_ucnt_info_1_1h) as user_ucnt_info_1_1h,
       count(user_ucnt_info_2_1h) as user_ucnt_info_2_1h,
       count(user_ucnt_ip_prov_1h) as user_ucnt_ip_prov_1h,
       count(user_ucnt_ip_city_1h) as user_ucnt_ip_city_1h,
       count(user_ucnt_mobile_oper_platform_1h) as user_ucnt_mobile_oper_platform_1h,
       count(user_ucnt_operation_channel_1h) as user_ucnt_operation_channel_1h,
       count(user_ucnt_pay_scene_1h) as user_ucnt_pay_scene_1h,
       count(user_sum_amt_1h) as user_sum_amt_1h,
       count(user_ucnt_opposing_id_1h) as user_ucnt_opposing_id_1h,
       count(gmt_occur_unix_user_diff_not_now) as gmt_occur_unix_user_diff_not_now,
       count(user_client_ip_rate_24h) as user_client_ip_rate_24h,
       count(user_network_rate_24h) as user_network_rate_24h,
       count(user_device_sign_rate_24h) as user_device_sign_rate_24h,
       count(user_info_1_rate_24h) as user_info_1_rate_24h,
       count(user_info_2_rate_24h) as user_info_2_rate_24h,
       count(user_ip_prov_rate_24h) as user_ip_prov_rate_24h,
       count(user_ip_city_rate_24h) as user_ip_city_rate_24h,
       count(user_mobile_oper_platform_rate_24h) as user_mobile_oper_platform_rate_24h,
       count(user_operation_channel_rate_24h) as user_operation_channel_rate_24h,
       count(user_pay_scene_rate_24h) as user_pay_scene_rate_24h,
       count(user_amt_rate_24h) as user_amt_rate_24h,
       count(user_opposing_id_rate_24h) as user_opposing_id_rate_24h
from t_sj_train_feature_user;



-- select *
-- from
--     (select gmt_occur_unix_diff,
--             count(event_id) as cnt
--      from t_sj_user_last_time_not_now
--      group by gmt_occur_unix_diff)t
-- where t.gmt_occur_unix_diff <10

-- ---------------------------数据分析

-- -- 不同的用户不同的时间颗粒的交易次数 6573377 
-- select count(distinct user_id, gmt_occur_unix) as ucnt
-- from t_sj_train_data_code_unix;
-- -- 同一个小时交易数超过1次的交易笔数 1693267 
-- -- 同一个小时交易数为1次的交易笔数 4880110 2次1136806 3次301144
-- -- 4880110次交易中一小时内只有一次交易，其余都超过了一次

-- select count(*)
-- from
--     (select user_id,
--             gmt_occur_unix,
--             count(*) as id_cnt
--      from t_sj_train_data_code_unix
--      group by user_id,
--               gmt_occur_unix)t
-- where t.id_cnt >1;

-- -- 同一个小时交易数超过1次的笔数中负样本的个数 52998(包括1，-1)
-- -- 全量训练集中负样本个数 55608
-- -- 大部分的欺诈交易的发生时，同一小时内交易次数超过1次  
-- -- 因此重点关注一小时内的交易情况
-- select count(*) as y1_cnt
-- from
--     (select user_id,
--             gmt_occur_unix,
--             id_cnt
--      from
--          (select user_id,
--                  gmt_occur_unix,
--                  count(*) as id_cnt
--           from t_sj_train_data_code_unix
--           group by user_id,
--                    gmt_occur_unix)t
--      where t.id_cnt >1)t1
-- left outer join
--     (select event_id,
--             user_id,
--             gmt_occur_unix,
--             is_fraud
--      from t_sj_train_data_code_unix)t2 on t1.user_id = t2.user_id
-- and t1.gmt_occur_unix = t2.gmt_occur_unix
-- where t2.is_fraud !=0


-- select t1.event_id,
--        (max(t1.gmt_occur_unix) - max(t2.gmt_occur_unix)) as gmt_occur_unix_diff
-- from
--     (select event_id,
--             user_id,
--             gmt_occur_unix
--      from t_sj_train_data_code_unix)t1
-- left outer join
--     (select event_id,
--             user_id,
--             gmt_occur_unix
--      from t_sj_train_data_code_unix)t2 on t1.user_id = t2.user_id
-- where t1.gmt_occur_unix>=t2.gmt_occur_unix
--     and t1.event_id !=t2.event_id
-- group by t1.event_id;

-- -- 获取用户上一次的时间点 有记录的是9444000 去掉了用户第一次交易的记录
-- drop table if exists t_sj_user_last_time;


-- create table t_sj_user_last_time as
-- select t1.event_id,
--         t2.user_id,
--        max(t2.gmt_occur_unix) as gmt_occur_unix_last
-- from
--     (select event_id,
--             user_id,
--             gmt_occur_unix
--      from t_sj_train_data_code_unix)t1
-- left outer join
--     (select event_id,
--             user_id,
--             gmt_occur_unix
--      from t_sj_train_data_code_unix)t2 on t1.user_id = t2.user_id
-- where t1.gmt_occur_unix>=t2.gmt_occur_unix
--     and t1.event_id !=t2.event_id
-- group by t1.event_id,t2.user_id;


