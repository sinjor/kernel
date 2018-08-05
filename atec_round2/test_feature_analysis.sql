drop table if exists t_sj_test_analysis;


create table t_sj_test_analysis as
select "user_id" as name,
       count(user_id) as cnt,
       count(distinct user_id) as unique_cnt
from atec_1000w_oota_data
union
select "client_ip" as name,
       count(client_ip) as cnt,
       count(distinct client_ip) as unique_cnt
from atec_1000w_oota_data
union
select "network" as name,
       count(network) as cnt,
       count(distinct network) as unique_cnt
from atec_1000w_oota_data
union
select "device_sign" as name,
       count(device_sign) as cnt,
       count(distinct device_sign) as unique_cnt
from atec_1000w_oota_data
union
select "info_1" as name,
       count(info_1) as cnt,
       count(distinct info_1) as unique_cnt
from atec_1000w_oota_data
union
select "info_2" as name,
       count(info_2) as cnt,
       count(distinct info_2) as unique_cnt
from atec_1000w_oota_data
union
select "ip_prov" as name,
       count(ip_prov) as cnt,
       count(distinct ip_prov) as unique_cnt
from atec_1000w_oota_data
union
select "ip_city" as name,
       count(ip_city) as cnt,
       count(distinct ip_city) as unique_cnt
from atec_1000w_oota_data
union
select "cert_prov" as name,
       count(cert_prov) as cnt,
       count(distinct cert_prov) as unique_cnt
from atec_1000w_oota_data
union
select "cert_city" as name,
       count(cert_city) as cnt,
       count(distinct cert_city) as unique_cnt
from atec_1000w_oota_data
union
select "card_bin_prov" as name,
       count(card_bin_prov) as cnt,
       count(distinct card_bin_prov) as unique_cnt
from atec_1000w_oota_data
union
select "card_bin_city" as name,
       count(card_bin_city) as cnt,
       count(distinct card_bin_city) as unique_cnt
from atec_1000w_oota_data
union
select "is_one_people" as name,
       count(is_one_people) as cnt,
       count(distinct is_one_people) as unique_cnt
from atec_1000w_oota_data
union
select "mobile_oper_platform" as name,
       count(mobile_oper_platform) as cnt,
       count(distinct mobile_oper_platform) as unique_cnt
from atec_1000w_oota_data
union
select "operation_channel" as name,
       count(operation_channel) as cnt,
       count(distinct operation_channel) as unique_cnt
from atec_1000w_oota_data
union
select "pay_scene" as name,
       count(pay_scene) as cnt,
       count(distinct pay_scene) as unique_cnt
from atec_1000w_oota_data
union
select "amt" as name,
       count(amt) as cnt,
       count(distinct amt) as unique_cnt
from atec_1000w_oota_data
union
select "card_cert_no" as name,
       count(card_cert_no) as cnt,
       count(distinct card_cert_no) as unique_cnt
from atec_1000w_oota_data
union
select "opposing_id" as name,
       count(opposing_id) as cnt,
       count(distinct opposing_id) as unique_cnt
from atec_1000w_oota_data
union
select "income_card_no" as name,
       count(income_card_no) as cnt,
       count(distinct income_card_no) as unique_cnt
from atec_1000w_oota_data
union
select "income_card_cert_no" as name,
       count(income_card_cert_no) as cnt,
       count(distinct income_card_cert_no) as unique_cnt
from atec_1000w_oota_data
union
select "income_card_mobile" as name,
       count(income_card_mobile) as cnt,
       count(distinct income_card_mobile) as unique_cnt
from atec_1000w_oota_data
union
select "income_card_bank_code" as name,
       count(income_card_bank_code) as cnt,
       count(distinct income_card_bank_code) as unique_cnt
from atec_1000w_oota_data
union
select "province" as name,
       count(province) as cnt,
       count(distinct province) as unique_cnt
from atec_1000w_oota_data
union
select "city" as name,
       count(city) as cnt,
       count(distinct city) as unique_cnt
from atec_1000w_oota_data
union
select "is_peer_pay" as name,
       count(is_peer_pay) as cnt,
       count(distinct is_peer_pay) as unique_cnt
from atec_1000w_oota_data
union
select "version" as name,
       count(version) as cnt,
       count(distinct version) as unique_cnt
from atec_1000w_oota_data;


drop table if exists t_sj_test_analysis_code;


create table t_sj_test_analysis_code as
select "user_id" as name,
       count(user_id) as cnt,
       count(distinct user_id) as unique_cnt
from t_sj_test_data_code
union
select "client_ip" as name,
       count(client_ip) as cnt,
       count(distinct client_ip) as unique_cnt
from t_sj_test_data_code
union
select "network" as name,
       count(network) as cnt,
       count(distinct network) as unique_cnt
from t_sj_test_data_code
union
select "device_sign" as name,
       count(device_sign) as cnt,
       count(distinct device_sign) as unique_cnt
from t_sj_test_data_code
union
select "info_1" as name,
       count(info_1) as cnt,
       count(distinct info_1) as unique_cnt
from t_sj_test_data_code
union
select "info_2" as name,
       count(info_2) as cnt,
       count(distinct info_2) as unique_cnt
from t_sj_test_data_code
union
select "ip_prov" as name,
       count(ip_prov) as cnt,
       count(distinct ip_prov) as unique_cnt
from t_sj_test_data_code
union
select "ip_city" as name,
       count(ip_city) as cnt,
       count(distinct ip_city) as unique_cnt
from t_sj_test_data_code
union
select "cert_prov" as name,
       count(cert_prov) as cnt,
       count(distinct cert_prov) as unique_cnt
from t_sj_test_data_code
union
select "cert_city" as name,
       count(cert_city) as cnt,
       count(distinct cert_city) as unique_cnt
from t_sj_test_data_code
union
select "card_bin_prov" as name,
       count(card_bin_prov) as cnt,
       count(distinct card_bin_prov) as unique_cnt
from t_sj_test_data_code
union
select "card_bin_city" as name,
       count(card_bin_city) as cnt,
       count(distinct card_bin_city) as unique_cnt
from t_sj_test_data_code
union
select "is_one_people" as name,
       count(is_one_people) as cnt,
       count(distinct is_one_people) as unique_cnt
from t_sj_test_data_code
union
select "mobile_oper_platform" as name,
       count(mobile_oper_platform) as cnt,
       count(distinct mobile_oper_platform) as unique_cnt
from t_sj_test_data_code
union
select "operation_channel" as name,
       count(operation_channel) as cnt,
       count(distinct operation_channel) as unique_cnt
from t_sj_test_data_code
union
select "pay_scene" as name,
       count(pay_scene) as cnt,
       count(distinct pay_scene) as unique_cnt
from t_sj_test_data_code
union
select "amt" as name,
       count(amt) as cnt,
       count(distinct amt) as unique_cnt
from t_sj_test_data_code
union
select "card_cert_no" as name,
       count(card_cert_no) as cnt,
       count(distinct card_cert_no) as unique_cnt
from t_sj_test_data_code
union
select "opposing_id" as name,
       count(opposing_id) as cnt,
       count(distinct opposing_id) as unique_cnt
from t_sj_test_data_code
union
select "income_card_no" as name,
       count(income_card_no) as cnt,
       count(distinct income_card_no) as unique_cnt
from t_sj_test_data_code
union
select "income_card_cert_no" as name,
       count(income_card_cert_no) as cnt,
       count(distinct income_card_cert_no) as unique_cnt
from t_sj_test_data_code
union
select "income_card_mobile" as name,
       count(income_card_mobile) as cnt,
       count(distinct income_card_mobile) as unique_cnt
from t_sj_test_data_code
union
select "income_card_bank_code" as name,
       count(income_card_bank_code) as cnt,
       count(distinct income_card_bank_code) as unique_cnt
from t_sj_test_data_code
union
select "province" as name,
       count(province) as cnt,
       count(distinct province) as unique_cnt
from t_sj_test_data_code
union
select "city" as name,
       count(city) as cnt,
       count(distinct city) as unique_cnt
from t_sj_test_data_code
union
select "is_peer_pay" as name,
       count(is_peer_pay) as cnt,
       count(distinct is_peer_pay) as unique_cnt
from t_sj_test_data_code
union
select "version" as name,
       count(version) as cnt,
       count(distinct version) as unique_cnt
from t_sj_test_data_code;