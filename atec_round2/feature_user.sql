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
drop table if exists t_sj_feature_uid;


create table t_sj_feature_uid as
select 
t0.event_id,
t1.id_cnt_24h,
t1.id_ucnt_device_sign_24h,
t1.id_ucnt_network_24h,
t1.id_ucnt_client_ip_24h,
t1.id_ucnt_ip_prov_24h,
t1.id_ucnt_ip_city_24h,
t1.id_ucnt_operation_channel_24h,
t1.id_ucnt_pay_scene_24h
t2.id_cnt_1h,
t2.id_ucnt_device_sign_1h,
t2.id_ucnt_network_1h,
t2.id_ucnt_client_ip_1h,
t2.id_ucnt_ip_prov_1h,
t2.id_ucnt_ip_city_1h,
t2.id_ucnt_operation_channel_1h,
t2.id_ucnt_pay_scene_1h,
t3.gmt_occur_unix_diff,
t4.gmt_occur_unix_diff_not_now,
t5.uid_client_ip_rate_24h,
t5.uid_network_rate_24h,
t5.uid_device_sign_rate_24h,
t5.uid_ip_prov_rate_24h,
t5.uid_ip_city_rate_24h,
t5.uid_mobile_oper_platform_rate_24h,
t5.uid_operation_channel_rate_24h,
t5.uid_pay_scene_rate_24h
 from 
(select event_id from t_sj_train_data_code_unix) t0 
left outer join t_sj_feature_user_24h_not_now t1 on t0.event_id = t1.event_id
left outer join t_sj_feature_user_1h t2 on t0.event_id = t2.event_id
left outer join t_sj_user_time_diff t3 on t0.event_id = t3.event_id
left outer join t_sj_user_time_diff_not_now t4 on t0.event_id = t4.event_id
left outer join t_sj_user_scatter_freq_24h t5 on t0.event_id = t5.event_id;

-- 去除特征中的缺失值
drop table if exists t_sj_feature_uid_notnull;


create table t_sj_feature_uid_notnull as
select 
event_id,
nvl(id_cnt_24h, 0) as id_cnt_24h
nvl(id_ucnt_device_sign_24h, 0) as id_ucnt_device_sign_24h,
nvl(id_ucnt_network_24h, 0) as id_ucnt_network_24h,
nvl(id_ucnt_client_ip_24h, 0) as id_ucnt_client_ip_24h,
nvl(id_ucnt_ip_prov_24h, 0) as id_ucnt_ip_prov_24h,
nvl(id_ucnt_ip_city_24h, 0) as id_ucnt_ip_city_24h,
nvl(id_ucnt_operation_channel_24h, 0) as id_ucnt_operation_channel_24h,
nvl(id_ucnt_pay_scene_24h, 0) as id_ucnt_pay_scene_24h,
nvl(id_cnt_1h, 0) as id_cnt_1h,
nvl(id_ucnt_device_sign_1h, 0) as id_ucnt_device_sign_1h,
nvl(id_ucnt_network_1h, 0) as id_ucnt_network_1h,
nvl(id_ucnt_client_ip_1h, 0) as id_ucnt_client_ip_1h,
nvl(id_ucnt_ip_prov_1h, 0) as id_ucnt_ip_prov_1h,
nvl(id_ucnt_ip_city_1h, 0) as id_ucnt_ip_city_1h,
nvl(id_ucnt_operation_channel_1h, 0) as id_ucnt_operation_channel_1h,
nvl(id_ucnt_pay_scene_1h, 0) as id_ucnt_pay_scene_1h,
nvl(gmt_occur_unix_diff, 0) as gmt_occur_unix_diff,
nvl(gmt_occur_unix_diff_not_now, 0) as gmt_occur_unix_diff_not_now,
nvl(uid_client_ip_rate_24h, 0) as uid_client_ip_rate_24h,
nvl(uid_network_rate_24h, 0) as uid_network_rate_24h,
nvl(uid_device_sign_rate_24h, 0) as uid_device_sign_rate_24h,
nvl(uid_ip_prov_rate_24h, 0) as uid_ip_prov_rate_24h,
nvl(uid_ip_city_rate_24h, 0) as uid_ip_city_rate_24h,
nvl(uid_mobile_oper_platform_rate_24h, 0) as uid_mobile_oper_platform_rate_24h,
nvl(uid_operation_channel_rate_24h, 0) as uid_operation_channel_rate_24h,
nvl(uid_pay_scene_rate_24h 0) as uid_pay_scene_rate_24h
from t_sj_feature_uid;


-- 24小时内的交易数，和部分离散变量的去重个数
-- 4min32s
drop table if exists t_sj_feature_user_24h;


create table t_sj_feature_user_24h as
select t1.event_id,
       count(*) as id_cnt_24h,
       size(collect_set(t2.device_sign)) as id_ucnt_device_sign_24h,
       -- 3min7s

       size(collect_set(t2.network)) as id_ucnt_network_24h,
       size(collect_set(t2.client_ip)) as id_ucnt_client_ip_24h,
       size(collect_set(t2.ip_prov)) as id_ucnt_ip_prov_24h,
       size(collect_set(t2.ip_city)) as id_ucnt_ip_city_24h,
       size(collect_set(t2.operation_channel)) as id_ucnt_operation_channel_24h,
       size(collect_set(t2.pay_scene)) as id_ucnt_pay_scene_24h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            device_sign,
            network,
            client_ip,
            ip_prov,
            ip_city,
            operation_channel,
            pay_scene
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where (t1.gmt_occur_unix - t2.gmt_occur_unix) < 86400
    and t2.gmt_occur_unix <=t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id

-- 24小时内的交易数，和部分离散变量的去重个数（不包括当前小时内数据）
drop table if exists t_sj_feature_user_24h_not_now;


create table t_sj_feature_user_24h_not_now as
select t1.event_id,
       count(*) as id_cnt_24h,
       size(collect_set(t2.device_sign)) as id_ucnt_device_sign_24h,
       -- 3min7s

       size(collect_set(t2.network)) as id_ucnt_network_24h,
       size(collect_set(t2.client_ip)) as id_ucnt_client_ip_24h,
       size(collect_set(t2.ip_prov)) as id_ucnt_ip_prov_24h,
       size(collect_set(t2.ip_city)) as id_ucnt_ip_city_24h,
       size(collect_set(t2.operation_channel)) as id_ucnt_operation_channel_24h,
       size(collect_set(t2.pay_scene)) as id_ucnt_pay_scene_24h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            device_sign,
            network,
            client_ip,
            ip_prov,
            ip_city,
            operation_channel,
            pay_scene
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where (t1.gmt_occur_unix - t2.gmt_occur_unix) < 86400
    and t2.gmt_occur_unix <=t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id


-- 一小时内的交易次数 和部分离散变量的去重个数
drop table if exists t_sj_feature_user_1h;


create table t_sj_feature_user_1h as
select t1.event_id,
       count(*) as id_cnt_1h,
       size(collect_set(t2.device_sign)) as id_ucnt_device_sign_1h,
       -- 3min7s

       size(collect_set(t2.network)) as id_ucnt_network_1h,
       size(collect_set(t2.client_ip)) as id_ucnt_client_ip_1h,
       size(collect_set(t2.ip_prov)) as id_ucnt_ip_prov_1h,
       size(collect_set(t2.ip_city)) as id_ucnt_ip_city_1h,
       size(collect_set(t2.operation_channel)) as id_ucnt_operation_channel_1h,
       size(collect_set(t2.pay_scene)) as id_ucnt_pay_scene_1h
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix) t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            device_sign,
            network,
            client_ip,
            ip_prov,
            ip_city,
            operation_channel,
            pay_scene
     from t_sj_train_data_code_unix) t2 on t1.user_id = t2.user_id
where t2.gmt_occur_unix = t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id


-- 获取用户上一次的时间点 有记录的是9444000 去掉了用户第一次交易的记录
drop table if exists t_sj_user_last_time;


create table t_sj_user_last_time as
select t1.event_id,
        t2.user_id,
       max(t2.gmt_occur_unix) as gmt_occur_unix_last
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix)t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix)t2 on t1.user_id = t2.user_id
where t1.gmt_occur_unix>=t2.gmt_occur_unix
    and t1.event_id !=t2.event_id
group by t1.event_id,t2.user_id;

-- 熵0.052 
-- gini0.0115
-- info gain 0.0045
-- info gain ratio 0.0011


-- 时间差表格
drop table if exists t_sj_user_time_diff;


create table t_sj_user_time_diff as
select t1.event_id,
       t1.is_fraud,
       (t1.gmt_occur_unix - t2.gmt_occur_unix_last)/3600 as gmt_occur_unix_diff
from t_sj_train_data_code_unix t1
left outer join t_sj_user_last_time t2 on t1.event_id = t2.event_id

-------------------- 获取用户上一次的时间点(去除同一个小时的记录)
-- 6min15s
drop table if exists t_sj_user_last_time_not_now;


create table t_sj_user_last_time_not_now as
select t1.event_id,
        t2.user_id,
       max(t2.gmt_occur_unix) as gmt_occur_unix_last
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix)t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix)t2 on t1.user_id = t2.user_id
where t1.gmt_occur_unix>t2.gmt_occur_unix
    and t1.event_id !=t2.event_id
group by t1.event_id,t2.user_id;

-- 时间差表格
drop table if exists t_sj_user_time_diff_not_now;


create table t_sj_user_time_diff_not_now as
select t1.event_id,
       case
           when t1.is_fraud = 0 then 0 then 1 as t1.is_fraud
       end,
       -- t1.is_fraud,

       (t1.gmt_occur_unix - t2.gmt_occur_unix_last)/3600 as gmt_occur_unix_diff_not_now
from t_sj_train_data_code_unix t1
left outer join t_sj_user_last_time_not_now t2 on t1.event_id = t2.event_id


-- 计算每条样本中用户对应的离散变量在当前样本最近24h内的出现频率[0,1],没有出现为0
-- 3min内
drop table if exists t_sj_user_scatter_freq_24h;


create table t_sj_user_scatter_freq_24h as
select t0.event_id,
       case
           when t2.uid_client_ip_cnt_24h is null then 0
           else t2.uid_client_ip_cnt_24h/t1.id_cnt_24h
       end as uid_client_ip_rate_24h,
       case
           when t3.uid_network_cnt_24h is null then 0
           else t3.uid_network_cnt_24h/t1.id_cnt_24h
       end as uid_network_rate_24h,
       case
           when t4.uid_device_sign_cnt_24h is null then 0
           else t4.uid_device_sign_cnt_24h/t1.id_cnt_24h
       end as uid_device_sign_rate_24h,
       case
           when t5.uid_ip_prov_cnt_24h is null then 0
           else t5.uid_ip_prov_cnt_24h/t1.id_cnt_24h
       end as uid_ip_prov_rate_24h,
       case
           when t6.uid_ip_city_cnt_24h is null then 0
           else t6.uid_ip_city_cnt_24h/t1.id_cnt_24h
       end as uid_ip_city_rate_24h,
       case
           when t7.uid_mobile_oper_platform_cnt_24h is null then 0
           else t7.uid_mobile_oper_platform_cnt_24h/t1.id_cnt_24h
       end as uid_mobile_oper_platform_rate_24h,
       case
           when t8.uid_operation_channel_cnt_24h is null then 0
           else t8.uid_operation_channel_cnt_24h/t1.id_cnt_24h
       end as uid_operation_channel_rate_24h,
       case
           when t9.uid_pay_scene_cnt_24h is null then 0
           else t9.uid_pay_scene_cnt_24h/t1.id_cnt_24h
       end as uid_pay_scene_rate_24h
from t_sj_train_data_code_unix t0
left outer join t_sj_feature_user_24h_not_now t1 on t0.event_id = t1.event_id
left outer join t_sj_uid_client_ip_24h t2 on t0.event_id = t2.event_id
and t0.client_ip = t2.client_ip
left outer join t_sj_uid_network_24h t3 on t0.event_id = t3.event_id
and t0.network = t3.network
left outer join t_sj_uid_device_sign_24h t4 on t0.event_id = t4.event_id
and t0.device_sign = t4.device_sign
left outer join t_sj_uid_ip_prov_24h t5 on t0.event_id = t5.event_id
and t0.ip_prov = t5.ip_prov
left outer join t_sj_uid_ip_city_24h t6 on t0.event_id = t6.event_id
and t0.ip_city = t6.ip_city
left outer join t_sj_uid_mobile_oper_platform_24h t7 on t0.event_id = t7.event_id
and t0.mobile_oper_platform = t7.mobile_oper_platform
left outer join t_sj_uid_operation_channel_24h t8 on t0.event_id = t8.event_id
and t0.operation_channel = t8.operation_channel
left outer join t_sj_uid_pay_scene_24h t9 on t0.event_id = t9.event_id
and t0.pay_scene = t9.pay_scene;


---------------------------------------------------离散变量的历史出现频率 每张表的执行时间约3min
-- 过去24小时内ip对应的使用个数
drop table if exists t_sj_uid_client_ip_24h;


create table t_sj_uid_client_ip_24h as
select t1.event_id,
       t2.client_ip,
       count(*) as uid_client_ip_cnt_24h
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
where (t1.gmt_occur_unix - t2.gmt_occur_unix) < 86400
    and t2.gmt_occur_unix <=t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.client_ip;


drop table if exists t_sj_uid_network_24h;


create table t_sj_uid_network_24h as
select t1.event_id,
       t2.network,
       count(*) as uid_network_cnt_24h
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
where (t1.gmt_occur_unix - t2.gmt_occur_unix) < 86400
    and t2.gmt_occur_unix <=t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.network;


drop table if exists t_sj_uid_device_sign_24h;


create table t_sj_uid_device_sign_24h as
select t1.event_id,
       t2.device_sign,
       count(*) as uid_device_sign_cnt_24h
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
where (t1.gmt_occur_unix - t2.gmt_occur_unix) < 86400
    and t2.gmt_occur_unix <=t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.device_sign;


drop table if exists t_sj_uid_ip_prov_24h;


create table t_sj_uid_ip_prov_24h as
select t1.event_id,
       t2.ip_prov,
       count(*) as uid_ip_prov_cnt_24h
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
where (t1.gmt_occur_unix - t2.gmt_occur_unix) < 86400
    and t2.gmt_occur_unix <=t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.ip_prov;


drop table if exists t_sj_uid_ip_city_24h;


create table t_sj_uid_ip_city_24h as
select t1.event_id,
       t2.ip_city,
       count(*) as uid_ip_city_cnt_24h
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
where (t1.gmt_occur_unix - t2.gmt_occur_unix) < 86400
    and t2.gmt_occur_unix <=t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.ip_city;


drop table if exists t_sj_uid_mobile_oper_platform_24h;


create table t_sj_uid_mobile_oper_platform_24h as
select t1.event_id,
       t2.mobile_oper_platform,
       count(*) as uid_mobile_oper_platform_cnt_24h
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
where (t1.gmt_occur_unix - t2.gmt_occur_unix) < 86400
    and t2.gmt_occur_unix <=t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.mobile_oper_platform;


drop table if exists t_sj_uid_operation_channel_24h;


create table t_sj_uid_operation_channel_24h as
select t1.event_id,
       t2.operation_channel,
       count(*) as uid_operation_channel_cnt_24h
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
where (t1.gmt_occur_unix - t2.gmt_occur_unix) < 86400
    and t2.gmt_occur_unix <=t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.operation_channel;


drop table if exists t_sj_uid_pay_scene_24h;


create table t_sj_uid_pay_scene_24h as
select t1.event_id,
       t2.pay_scene,
       count(*) as uid_pay_scene_cnt_24h
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
where (t1.gmt_occur_unix - t2.gmt_occur_unix) < 86400
    and t2.gmt_occur_unix <=t1.gmt_occur_unix
    and t1.event_id != t2.event_id
group by t1.event_id,
         t2.pay_scene;
---------------------------------------------------离散变量的历史出现频率.






select *
from
    (select gmt_occur_unix_diff,
            count(event_id) as cnt
     from t_sj_user_last_time_not_now
     group by gmt_occur_unix_diff)t
where t.gmt_occur_unix_diff <10

---------------------------数据分析

-- 不同的用户不同的时间颗粒的交易次数 6573377 
select count(distinct user_id, gmt_occur_unix) as ucnt
from t_sj_train_data_code_unix;
-- 同一个小时交易数超过1次的交易笔数 1693267 
-- 同一个小时交易数为1次的交易笔数 4880110 2次1136806 3次301144
-- 4880110次交易中一小时内只有一次交易，其余都超过了一次

select count(*)
from
    (select user_id,
            gmt_occur_unix,
            count(*) as id_cnt
     from t_sj_train_data_code_unix
     group by user_id,
              gmt_occur_unix)t
where t.id_cnt >1;

-- 同一个小时交易数超过1次的笔数中负样本的个数 52998(包括1，-1)
-- 全量训练集中负样本个数 55608
-- 大部分的欺诈交易的发生时，同一小时内交易次数超过1次  
-- 因此重点关注一小时内的交易情况
select count(*) as y1_cnt
from
    (select user_id,
            gmt_occur_unix,
            id_cnt
     from
         (select user_id,
                 gmt_occur_unix,
                 count(*) as id_cnt
          from t_sj_train_data_code_unix
          group by user_id,
                   gmt_occur_unix)t
     where t.id_cnt >1)t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix,
            is_fraud
     from t_sj_train_data_code_unix)t2 on t1.user_id = t2.user_id
and t1.gmt_occur_unix = t2.gmt_occur_unix
where t2.is_fraud !=0


select t1.event_id,
       (max(t1.gmt_occur_unix) - max(t2.gmt_occur_unix)) as gmt_occur_unix_diff
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix)t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix)t2 on t1.user_id = t2.user_id
where t1.gmt_occur_unix>=t2.gmt_occur_unix
    and t1.event_id !=t2.event_id
group by t1.event_id;

-- 获取用户上一次的时间点 有记录的是9444000 去掉了用户第一次交易的记录
drop table if exists t_sj_user_last_time;


create table t_sj_user_last_time as
select t1.event_id,
        t2.user_id,
       max(t2.gmt_occur_unix) as gmt_occur_unix_last
from
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix)t1
left outer join
    (select event_id,
            user_id,
            gmt_occur_unix
     from t_sj_train_data_code_unix)t2 on t1.user_id = t2.user_id
where t1.gmt_occur_unix>=t2.gmt_occur_unix
    and t1.event_id !=t2.event_id
group by t1.event_id,t2.user_id;


