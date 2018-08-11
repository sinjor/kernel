-- 字段名称	解释
-- event_id	事件id
-- user_id	虚拟用户ID
-- gmt_occur	事件发生时间
-- client_ip	用户IP
-- network	网络类型
-- device_sign	设备ID
-- info_1	信息1
-- info_2	信息2
-- ip_prov	IP省
-- ip_city	IP市
-- cert_prov	证件省
-- cert_city	证件市
-- card_bin_prov	支付卡bin省
-- card_bin_city	支付卡bin市
-- card_mobile_prov	支付账号省
-- card_mobile_city	支付账号市
-- card_cert_prov	支付卡省
-- card_cert_city	支付卡市
-- is_one_people	主次双方证件是否一致
-- mobile_oper_platform	手机操作平台
-- operation_channel	支付方式
-- pay_scene	支付场景
-- amt	金额
-- card_cert_no	虚拟用户证件号
-- opposing_id	对方虚拟用户ID
-- income_card_no	虚拟用户的收款银行卡号
-- income_card_cert_no	虚拟收款用户的证件号
-- income_card_mobile	虚拟收款用户的手机号
-- income_card_bank_code	收入账号银行代码
-- province	收入账号归属省份
-- city	虚拟收款用户归属城市
-- is_peer_pay	是否代付
-- version	版本号
-- is_fraud	预测标签
 -- 合并训练集表格


-- org_train_data
drop table if exists t_sj_train_analysis;


create table t_sj_train_analysis as
select "user_id" as name,
       count(user_id) as cnt,
       count(distinct user_id) as unique_cnt
from atec_1000w_ins_data
union
select "client_ip" as name,
       count(client_ip) as cnt,
       count(distinct client_ip) as unique_cnt
from atec_1000w_ins_data
union
select "network" as name,
       count(network) as cnt,
       count(distinct network) as unique_cnt
from atec_1000w_ins_data
union
select "device_sign" as name,
       count(device_sign) as cnt,
       count(distinct device_sign) as unique_cnt
from atec_1000w_ins_data
union
select "info_1" as name,
       count(info_1) as cnt,
       count(distinct info_1) as unique_cnt
from atec_1000w_ins_data
union
select "info_2" as name,
       count(info_2) as cnt,
       count(distinct info_2) as unique_cnt
from atec_1000w_ins_data
union
select "ip_prov" as name,
       count(ip_prov) as cnt,
       count(distinct ip_prov) as unique_cnt
from atec_1000w_ins_data
union
select "ip_city" as name,
       count(ip_city) as cnt,
       count(distinct ip_city) as unique_cnt
from atec_1000w_ins_data
union
select "cert_prov" as name,
       count(cert_prov) as cnt,
       count(distinct cert_prov) as unique_cnt
from atec_1000w_ins_data
union
select "cert_city" as name,
       count(cert_city) as cnt,
       count(distinct cert_city) as unique_cnt
from atec_1000w_ins_data
union
select "card_bin_prov" as name,
       count(card_bin_prov) as cnt,
       count(distinct card_bin_prov) as unique_cnt
from atec_1000w_ins_data
union
select "card_bin_city" as name,
       count(card_bin_city) as cnt,
       count(distinct card_bin_city) as unique_cnt
from atec_1000w_ins_data
union
select "is_one_people" as name,
       count(is_one_people) as cnt,
       count(distinct is_one_people) as unique_cnt
from atec_1000w_ins_data
union
select "mobile_oper_platform" as name,
       count(mobile_oper_platform) as cnt,
       count(distinct mobile_oper_platform) as unique_cnt
from atec_1000w_ins_data
union
select "operation_channel" as name,
       count(operation_channel) as cnt,
       count(distinct operation_channel) as unique_cnt
from atec_1000w_ins_data
union
select "pay_scene" as name,
       count(pay_scene) as cnt,
       count(distinct pay_scene) as unique_cnt
from atec_1000w_ins_data
union
select "amt" as name,
       count(amt) as cnt,
       count(distinct amt) as unique_cnt
from atec_1000w_ins_data
union
select "card_cert_no" as name,
       count(card_cert_no) as cnt,
       count(distinct card_cert_no) as unique_cnt
from atec_1000w_ins_data
union
select "opposing_id" as name,
       count(opposing_id) as cnt,
       count(distinct opposing_id) as unique_cnt
from atec_1000w_ins_data
union
select "income_card_no" as name,
       count(income_card_no) as cnt,
       count(distinct income_card_no) as unique_cnt
from atec_1000w_ins_data
union
select "income_card_cert_no" as name,
       count(income_card_cert_no) as cnt,
       count(distinct income_card_cert_no) as unique_cnt
from atec_1000w_ins_data
union
select "income_card_mobile" as name,
       count(income_card_mobile) as cnt,
       count(distinct income_card_mobile) as unique_cnt
from atec_1000w_ins_data
union
select "income_card_bank_code" as name,
       count(income_card_bank_code) as cnt,
       count(distinct income_card_bank_code) as unique_cnt
from atec_1000w_ins_data
union
select "province" as name,
       count(province) as cnt,
       count(distinct province) as unique_cnt
from atec_1000w_ins_data
union
select "city" as name,
       count(city) as cnt,
       count(distinct city) as unique_cnt
from atec_1000w_ins_data
union
select "is_peer_pay" as name,
       count(is_peer_pay) as cnt,
       count(distinct is_peer_pay) as unique_cnt
from atec_1000w_ins_data
union
select "version" as name,
       count(version) as cnt,
       count(distinct version) as unique_cnt
from atec_1000w_ins_data;



-- train_code
drop table if exists t_sj_train_code_analysis;

create table t_sj_train_code_analysis as
select "user_id" as name,
       count(user_id) as cnt,
       count(distinct user_id) as unique_cnt
from t_sj_train_data_code
union
select "client_ip" as name,
       count(client_ip) as cnt,
       count(distinct client_ip) as unique_cnt
from t_sj_train_data_code
union
select "network" as name,
       count(network) as cnt,
       count(distinct network) as unique_cnt
from t_sj_train_data_code
union
select "device_sign" as name,
       count(device_sign) as cnt,
       count(distinct device_sign) as unique_cnt
from t_sj_train_data_code
union
select "info_1" as name,
       count(info_1) as cnt,
       count(distinct info_1) as unique_cnt
from t_sj_train_data_code
union
select "info_2" as name,
       count(info_2) as cnt,
       count(distinct info_2) as unique_cnt
from t_sj_train_data_code
union
select "ip_prov" as name,
       count(ip_prov) as cnt,
       count(distinct ip_prov) as unique_cnt
from t_sj_train_data_code
union
select "ip_city" as name,
       count(ip_city) as cnt,
       count(distinct ip_city) as unique_cnt
from t_sj_train_data_code
union
select "cert_prov" as name,
       count(cert_prov) as cnt,
       count(distinct cert_prov) as unique_cnt
from t_sj_train_data_code
union
select "cert_city" as name,
       count(cert_city) as cnt,
       count(distinct cert_city) as unique_cnt
from t_sj_train_data_code
union
select "card_bin_prov" as name,
       count(card_bin_prov) as cnt,
       count(distinct card_bin_prov) as unique_cnt
from t_sj_train_data_code
union
select "card_bin_city" as name,
       count(card_bin_city) as cnt,
       count(distinct card_bin_city) as unique_cnt
from t_sj_train_data_code
union
select "card_mobile_prov" as name,
       count(card_mobile_prov) as cnt,
       count(distinct card_mobile_prov) as unique_cnt
from t_sj_train_data_code
union
select "card_mobile_city" as name,
       count(card_mobile_city) as cnt,
       count(distinct card_mobile_city) as unique_cnt
from t_sj_train_data_code
union
select "card_cert_prov" as name,
       count(card_cert_prov) as cnt,
       count(distinct card_cert_prov) as unique_cnt
from t_sj_train_data_code
union
select "card_cert_city" as name,
       count(card_cert_city) as cnt,
       count(distinct card_cert_city) as unique_cnt
from t_sj_train_data_code
union
select "is_one_people" as name,
       count(is_one_people) as cnt,
       count(distinct is_one_people) as unique_cnt
from t_sj_train_data_code
union
select "mobile_oper_platform" as name,
       count(mobile_oper_platform) as cnt,
       count(distinct mobile_oper_platform) as unique_cnt
from t_sj_train_data_code
union
select "operation_channel" as name,
       count(operation_channel) as cnt,
       count(distinct operation_channel) as unique_cnt
from t_sj_train_data_code
union
select "pay_scene" as name,
       count(pay_scene) as cnt,
       count(distinct pay_scene) as unique_cnt
from t_sj_train_data_code
union
select "amt" as name,
       count(amt) as cnt,
       count(distinct amt) as unique_cnt
from t_sj_train_data_code
union
select "card_cert_no" as name,
       count(card_cert_no) as cnt,
       count(distinct card_cert_no) as unique_cnt
from t_sj_train_data_code
union
select "opposing_id" as name,
       count(opposing_id) as cnt,
       count(distinct opposing_id) as unique_cnt
from t_sj_train_data_code
union
select "income_card_no" as name,
       count(income_card_no) as cnt,
       count(distinct income_card_no) as unique_cnt
from t_sj_train_data_code
union
select "income_card_cert_no" as name,
       count(income_card_cert_no) as cnt,
       count(distinct income_card_cert_no) as unique_cnt
from t_sj_train_data_code
union
select "income_card_mobile" as name,
       count(income_card_mobile) as cnt,
       count(distinct income_card_mobile) as unique_cnt
from t_sj_train_data_code
union
select "income_card_bank_code" as name,
       count(income_card_bank_code) as cnt,
       count(distinct income_card_bank_code) as unique_cnt
from t_sj_train_data_code
union
select "province" as name,
       count(province) as cnt,
       count(distinct province) as unique_cnt
from t_sj_train_data_code
union
select "city" as name,
       count(city) as cnt,
       count(distinct city) as unique_cnt
from t_sj_train_data_code
union
select "is_peer_pay" as name,
       count(is_peer_pay) as cnt,
       count(distinct is_peer_pay) as unique_cnt
from t_sj_train_data_code
union
select "version" as name,
       count(version) as cnt,
       count(distinct version) as unique_cnt
from t_sj_train_data_code;

drop table if exists t_sj_train_code_analysis_pay_scene;

create table t_sj_train_code_analysis_pay_scene as
select "user_id" as name,
       count(user_id) as cnt,
       count(distinct user_id) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "client_ip" as name,
       count(client_ip) as cnt,
       count(distinct client_ip) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "network" as name,
       count(network) as cnt,
       count(distinct network) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "device_sign" as name,
       count(device_sign) as cnt,
       count(distinct device_sign) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "info_1" as name,
       count(info_1) as cnt,
       count(distinct info_1) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "info_2" as name,
       count(info_2) as cnt,
       count(distinct info_2) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "ip_prov" as name,
       count(ip_prov) as cnt,
       count(distinct ip_prov) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "ip_city" as name,
       count(ip_city) as cnt,
       count(distinct ip_city) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "cert_prov" as name,
       count(cert_prov) as cnt,
       count(distinct cert_prov) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "cert_city" as name,
       count(cert_city) as cnt,
       count(distinct cert_city) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "card_bin_prov" as name,
       count(card_bin_prov) as cnt,
       count(distinct card_bin_prov) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "card_bin_city" as name,
       count(card_bin_city) as cnt,
       count(distinct card_bin_city) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "card_mobile_prov" as name,
       count(card_mobile_prov) as cnt,
       count(distinct card_mobile_prov) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "card_mobile_city" as name,
       count(card_mobile_city) as cnt,
       count(distinct card_mobile_city) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "card_cert_prov" as name,
       count(card_cert_prov) as cnt,
       count(distinct card_cert_prov) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "card_cert_city" as name,
       count(card_cert_city) as cnt,
       count(distinct card_cert_city) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "is_one_people" as name,
       count(is_one_people) as cnt,
       count(distinct is_one_people) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "mobile_oper_platform" as name,
       count(mobile_oper_platform) as cnt,
       count(distinct mobile_oper_platform) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "operation_channel" as name,
       count(operation_channel) as cnt,
       count(distinct operation_channel) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "pay_scene" as name,
       count(pay_scene) as cnt,
       count(distinct pay_scene) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "amt" as name,
       count(amt) as cnt,
       count(distinct amt) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "card_cert_no" as name,
       count(card_cert_no) as cnt,
       count(distinct card_cert_no) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "opposing_id" as name,
       count(opposing_id) as cnt,
       count(distinct opposing_id) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "income_card_no" as name,
       count(income_card_no) as cnt,
       count(distinct income_card_no) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "income_card_cert_no" as name,
       count(income_card_cert_no) as cnt,
       count(distinct income_card_cert_no) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "income_card_mobile" as name,
       count(income_card_mobile) as cnt,
       count(distinct income_card_mobile) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "income_card_bank_code" as name,
       count(income_card_bank_code) as cnt,
       count(distinct income_card_bank_code) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "province" as name,
       count(province) as cnt,
       count(distinct province) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "city" as name,
       count(city) as cnt,
       count(distinct city) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "is_peer_pay" as name,
       count(is_peer_pay) as cnt,
       count(distinct is_peer_pay) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18
union
select "version" as name,
       count(version) as cnt,
       count(distinct version) as unique_cnt
from t_sj_train_data_code where pay_scene!=12 and pay_scene!=18;

drop table if exists t_sj_train_code_analysis_with_miss;


create table t_sj_train_code_analysis_with_miss as
select * ,
       (9601365 - cnt) as miss_cnt,
       ((9601365 - cnt) / 9601365) as miss_rate
from t_sj_train_code_analysis;



-- 支不同付场景的正负样本比例
-- 12 18场景下的好人比例最高 
-- 12 0:1 459360:38=12088
-- 18 0:1 243135:4=60783
select t1.pay_scene,
       t2.y0_cnt,
       t3.y1_cnt,
       t2.y0_cnt /t3.y1_cnt as y0_div_y1_rate
from
    (select pay_scene
     from t_sj_train_data_code
     group by pay_scene) t1
left outer join
    (select pay_scene,
            count(*) as y0_cnt
     from
         (select user_id,
                 pay_scene,
                 is_fraud
          from t_sj_train_data_code
          where is_fraud == 0) t
     group by pay_scene) t2 on t1.pay_scene = t2.pay_scene
left outer join
    (select pay_scene,
            count(*) as y1_cnt
     from
         (select user_id,
                 pay_scene,
                 is_fraud
          from t_sj_train_data_code
          where is_fraud != 0) t
     group by pay_scene) t3 on t1.pay_scene = t3.pay_scene

-- 不同时间的好坏交易比例，[0,6] 全量训练集中风险最高
select t2.hour,
       t2.y0_cnt,
       t3.y1_cnt,
       t2.y0_cnt /t3.y1_cnt as y0_div_y1_rate
from
    (select hour,
            count(*) as y0_cnt
     from
         (select user_id,
                 gmt_occur,
                 cast(substr(gmt_occur,12,2) as bigint) as hour,
                 is_fraud
          from t_sj_train_data_code
          where is_fraud == 0) t
     group by hour) t2
left outer join
    (select hour,
            count(*) as y1_cnt
     from
         (select user_id,
                 gmt_occur,
                 cast(substr(gmt_occur,12,2) as bigint) as hour,
                 is_fraud
          from t_sj_train_data_code
          where is_fraud != 0) t
     group by hour) t3 on t2.hour = t3.hour;


-- 不同星期天数的好坏交易比例，无法区分，都接近167:1
select t2.hour,
       t2.y0_cnt,
       t3.y1_cnt,
       t2.y0_cnt /t3.y1_cnt as y0_div_y1_rate
from
    (select hour,
            count(*) as y0_cnt
     from
         (select user_id,
                 gmt_occur,
                 weekday(to_date(gmt_occur,"yyyy-mm-dd hh")) as hour,
                 is_fraud
          from t_sj_train_data_code
          where is_fraud == 0) t
     group by hour) t2
left outer join
    (select hour,
            count(*) as y1_cnt
     from
         (select user_id,
                 gmt_occur,
                 weekday(to_date(gmt_occur,"yyyy-mm-dd hh")) as hour,
                 is_fraud
          from t_sj_train_data_code
          where is_fraud != 0) t
     group by hour) t3 on t2.hour = t3.hour;
