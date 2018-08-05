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


drop table if exists t_sj_feature_base;


create table t_sj_feature_base as
select event_id,
       gmt_occur,
       hour,
       case
           when hour >=0
                and hour <=6 then 0
           else 1
       end as hour_bin,
       network,
       info_1,
       info_2,
       is_one_people,
       mobile_oper_platform,
       operation_channel,
       pay_scene,
       amt
from
    (select event_id,
            gmt_occur,
            cast(substr(gmt_occur,12,2) as bigint) as hour,
            network,
            info_1,
            info_2,
            is_one_people,
            mobile_oper_platform,
            operation_channel,
            pay_scene,
            amt
     from t_sj_train_data_code_unix) t;