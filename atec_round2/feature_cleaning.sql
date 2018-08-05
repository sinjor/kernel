-- 字段名称	解释
-- event_id	事件id
factor-- user_id	虚拟用户ID
-- gmt_occur	事件发生时间
factor-- client_ip	用户IP
factor-- network	网络类型 19
factor-- device_sign	设备ID
-- info_1	信息1
-- info_2	信息2
factor-- ip_prov	IP省
factor-- ip_city	IP市
-- cert_prov	证件省
-- cert_city	证件市
-- card_bin_prov	支付卡bin省
-- card_bin_city	支付卡bin市
-- card_mobile_prov	支付账号省
-- card_mobile_city	支付账号市
-- card_cert_prov	支付卡省
-- card_cert_city	支付卡市
factor -- is_one_people	主次双方证件是否一致 2
factor -- mobile_oper_platform	手机操作平台 4
factor-- operation_channel	支付方式 4
factor-- pay_scene	支付场景 21
numeric-- amt	金额
factor-- card_cert_no	虚拟用户证件号 0.48

factor-- opposing_id	对方虚拟用户ID 257个样本一致
factor-- income_card_no	虚拟用户的收款银行卡号 0.91
facror-- income_card_cert_no	虚拟收款用户的证件号 0.99
factor-- income_card_mobile	虚拟收款用户的手机号 0.99
-- income_card_bank_code	收入账号银行代码 0.95
-- province	收入账号归属省份
-- city	虚拟收款用户归属城市
-- is_peer_pay	是否代付 0.98
-- version	版本号
-- is_fraud	预测标签
 -- 合并训练集表格
 -- 生成unix时间戳表格
drop table if exists t_sj_train_data_code_unix;


create table t_sj_train_data_code_unix as
select unix_timestamp(to_date(t.gmt_occur,"yyyy-mm-dd hh")) as gmt_occur_unix,
       t.*
from t_sj_train_data_code t;

drop table if exists t_sj_train_data_code;

-- /*+ MAPJOIN(t1),MAPJOIN(t2),MAPJOIN(t3),MAPJOIN(t4),MAPJOIN(t5),
-- MAPJOIN(t6),MAPJOIN(t7),MAPJOIN(t12) ,MAPJOIN(t17) ,MAPJOIN(t18) ,MAPJOIN(t19),
-- MAPJOIN(t20),MAPJOIN(t21) ,MAPJOIN(t23) ,MAPJOIN(t25) ,MAPJOIN(t26),
-- MAPJOIN(t29),MAPJOIN(t30)*/
create table t_sj_train_data_code as
select  t0.event_id,
                            t0.gmt_occur,
                            t0.is_fraud,
                            t0.amt,
                            t1.user_id_code as user_id,
                            t2.client_ip_code as client_ip,
                            t3.network_code as network,
                            t4.device_sign_code as device_sign,
                            t5.info_1_code as info_1,
                            t6.info_2_code as info_2,
                            t7.province_code as ip_prov,
                            t8.city_code as ip_city,
                            t9.province_code as cert_prov,
                            t10.city_code as cert_city,
                            t11.province_code as card_bin_prov,
                            t12.city_code as card_bin_city,
                            t13.province_code as card_mobile_prov,
                            t14.city_code as card_mobile_city,
                            t15.province_code as card_cert_prov,
                            t16.city_code as card_cert_city,
                            t17.is_one_people_code as is_one_people,
                            t18.mobile_oper_platform_code as mobile_oper_platform,
                            t19.operation_channel_code as operation_channel,
                            t20.pay_scene_code as pay_scene,
                            t21.card_cert_no_code as card_cert_no,
                            t22.user_id_code as opposing_id,
                            t23.income_card_no_code as income_card_no,
                            t24.card_cert_no_code as income_card_cert_no,
                            t25.income_card_mobile_code as income_card_mobile,
                            t26.income_card_bank_code_code as income_card_bank_code,
                            t27.province_code as province,
                            t28.city_code as city,
                            t29.is_peer_pay_code as is_peer_pay,
                            t30.version_code as version
from atec_1000w_ins_data t0
left outer join t_user_id_code t1 on t0.user_id = t1.user_id
left outer join t_client_ip_code t2 on t0.client_ip = t2.client_ip
left outer join t_network_code t3 on t0.network = t3.network
left outer join t_device_sign_code t4 on t0.device_sign =t4.device_sign
left outer join t_info_1_code t5 on t0.info_1 = t5.info_1
left outer join t_info_2_code t6 on t0.info_2 = t6.info_2
left outer join t_province_code t7 on t0.ip_prov = t7.province
left outer join t_city_code t8 on t0.ip_city = t8.city
left outer join t_province_code t9 on t0.cert_prov = t9.province
left outer join t_city_code t10 on t0.cert_city = t10.city
left outer join t_province_code t11 on t0.card_bin_prov = t11.province
left outer join t_city_code t12 on t0.card_bin_city = t12.city
left outer join t_province_code t13 on t0.card_mobile_prov = t13.province
left outer join t_city_code t14 on t0.card_mobile_city = t14.city
left outer join t_province_code t15 on t0.card_cert_prov = t15.province
left outer join t_city_code t16 on t0.card_cert_city = t16.city
left outer join t_is_one_people_code t17 on t0.is_one_people = t17.is_one_people
left outer join t_mobile_oper_platform_code t18 on t0.mobile_oper_platform = t18.mobile_oper_platform
left outer join t_operation_channel_code t19 on t0.operation_channel = t19.operation_channel
left outer join t_pay_scene_code t20 on t0.pay_scene = t20.pay_scene
left outer join t_card_cert_no_code t21 on t0.card_cert_no = t21.card_cert_no
left outer join t_user_id_code t22 on t0.opposing_id = t22.user_id
left outer join t_income_card_no_code t23 on t0.income_card_no = t23.income_card_no
left outer join t_card_cert_no_code t24 on t0.income_card_cert_no = t24.card_cert_no
left outer join t_income_card_mobile_code t25 on t0.income_card_mobile = t25.income_card_mobile
left outer join t_income_card_bank_code_code t26 on t0.income_card_bank_code = t26.income_card_bank_code
left outer join t_province_code t27 on t0.province = t27.province
left outer join t_city_code t28 on t0.city = t28.city
left outer join t_is_peer_pay_code t29 on t0.is_peer_pay = t29.is_peer_pay
left outer join t_version_code t30 on t0.version = t30.version


-- 随机数编码
drop table if exists t_user_id_code;


create table t_user_id_code as
select t.user_id,
       row_number() over(
                         order by t.user_id) as user_id_code
from
    (select user_id
     from atec_1000w_ins_data
     where user_id is not null
     union select opposing_id as user_id
     from atec_1000w_ins_data
     where opposing_id is not null)t;

 -- client_ip
drop table if exists t_client_ip_code;


create table t_client_ip_code as
select t.client_ip,
       row_number() over(
                         order by t.client_ip) as client_ip_code
from
    (select distinct client_ip as client_ip
     from atec_1000w_ins_data
     where client_ip is not null)t;

 -- network

drop table if exists t_network_code;


create table t_network_code as
select t.network,
       row_number() over(
                         order by t.network) as network_code
from
    (select distinct network as network
     from atec_1000w_ins_data
     where network is not null)t;

 -- device_sign

drop table if exists t_device_sign_code;


create table t_device_sign_code as
select t.device_sign,
       row_number() over(
                         order by t.device_sign) as device_sign_code
from
    (select distinct device_sign as device_sign
     from atec_1000w_ins_data
     where device_sign is not null)t;

 -- info_1

drop table if exists t_info_1_code;


create table t_info_1_code as
select t.info_1,
       row_number() over(
                         order by t.info_1) as info_1_code
from
    (select distinct info_1 as info_1
     from atec_1000w_ins_data
     where info_1 is not null)t;

 -- info_2

drop table if exists t_info_2_code;


create table t_info_2_code as
select t.info_2,
       row_number() over(
                         order by t.info_2) as info_2_code
from
    (select distinct info_2 as info_2
     from atec_1000w_ins_data
     where info_2 is not null)t;

 -- province

drop table if exists t_province_code;


create table t_province_code as
select t.province,
       row_number() over(
                         order by t.province) as province_code
from
    (select province
     from
         (select ip_prov as province
          from atec_1000w_ins_data
          union select cert_prov as province
          from atec_1000w_ins_data
          union select card_bin_prov as province
          from atec_1000w_ins_data
          union select card_mobile_prov as province
          from atec_1000w_ins_data
          union select province
          from atec_1000w_ins_data)t0
     where province is not null)t;

 -- city

drop table if exists t_city_code;


create table t_city_code as
select t.city,
       row_number() over(
                         order by t.city) as city_code
from
    (select city
     from
         (select ip_city as city
          from atec_1000w_ins_data
          union select cert_city as city
          from atec_1000w_ins_data
          union select card_bin_city as city
          from atec_1000w_ins_data
          union select card_mobile_city as city
          from atec_1000w_ins_data
          union select city
          from atec_1000w_ins_data)t0
     where city is not null)t;

 -- is_one_people

drop table if exists t_is_one_people_code;


create table t_is_one_people_code as
select t.is_one_people,
       row_number() over(
                         order by t.is_one_people) as is_one_people_code
from
    (select distinct is_one_people as is_one_people
     from atec_1000w_ins_data
     where is_one_people is not null)t;

 -- mobile_oper_platform

drop table if exists t_mobile_oper_platform_code;


create table t_mobile_oper_platform_code as
select t.mobile_oper_platform,
       row_number() over(
                         order by t.mobile_oper_platform) as mobile_oper_platform_code
from
    (select distinct mobile_oper_platform as mobile_oper_platform
     from atec_1000w_ins_data
     where mobile_oper_platform is not null)t;

 -- operation_channel

drop table if exists t_operation_channel_code;


create table t_operation_channel_code as
select t.operation_channel,
       row_number() over(
                         order by t.operation_channel) as operation_channel_code
from
    (select distinct operation_channel as operation_channel
     from atec_1000w_ins_data
     where operation_channel is not null)t;

 -- pay_scene

drop table if exists t_pay_scene_code;


create table t_pay_scene_code as
select t.pay_scene,
       row_number() over(
                         order by t.pay_scene) as pay_scene_code
from
    (select distinct pay_scene as pay_scene
     from atec_1000w_ins_data
     where pay_scene is not null)t;

 -- card_cert_no

drop table if exists t_card_cert_no_code;


create table t_card_cert_no_code as
select t.card_cert_no,
       row_number() over(
                         order by t.card_cert_no) as card_cert_no_code
from
    (select card_cert_no
     from atec_1000w_ins_data
     where card_cert_no is not null
     union select income_card_cert_no as card_cert_no
     from atec_1000w_ins_data
     where income_card_cert_no is not null)t;

 -- income_card_no

drop table if exists t_income_card_no_code;


create table t_income_card_no_code as
select t.income_card_no,
       row_number() over(
                         order by t.income_card_no) as income_card_no_code
from
    (select distinct income_card_no as income_card_no
     from atec_1000w_ins_data
     where income_card_no is not null)t;

 -- income_card_mobile

drop table if exists t_income_card_mobile_code;


create table t_income_card_mobile_code as
select t.income_card_mobile,
       row_number() over(
                         order by t.income_card_mobile) as income_card_mobile_code
from
    (select distinct income_card_mobile as income_card_mobile
     from atec_1000w_ins_data
     where income_card_mobile is not null)t;

 -- income_card_bank_code

drop table if exists t_income_card_bank_code_code;


create table t_income_card_bank_code_code as
select t.income_card_bank_code,
       row_number() over(
                         order by t.income_card_bank_code) as income_card_bank_code_code
from
    (select distinct income_card_bank_code as income_card_bank_code
     from atec_1000w_ins_data
     where income_card_bank_code is not null)t;

 -- is_peer_pay

drop table if exists t_is_peer_pay_code;


create table t_is_peer_pay_code as
select t.is_peer_pay,
       row_number() over(
                         order by t.is_peer_pay) as is_peer_pay_code
from
    (select distinct is_peer_pay as is_peer_pay
     from atec_1000w_ins_data
     where is_peer_pay is not null)t;

 -- version

drop table if exists t_version_code;


create table t_version_code as
select t.version,
         row_number() over(
                           order by t.version) as version_code
from
    (select distinct version as version
     from atec_1000w_ins_data
     where version is not null)t;


