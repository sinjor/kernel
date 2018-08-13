-- 统计用户在前20次的交易次数，因为有部分用户的交易次数可能不足20次
drop table if exists t_sj_train_device_cnt_20n;


create table t_sj_train_device_cnt_20n as
select t3.event_id,
       t4.device_cnt_20n
from t_sj_train_data_code_unix t3
left outer join
    (select device_sign,
            gmt_occur_unix,
            count(*) as device_cnt_20n
     from
         (select row_number() over(partition by t1.device_sign,t1.gmt_occur_unix
                                   order by t2.gmt_occur_unix desc) as cnt_order,
                 t1.device_sign,
                 t1.gmt_occur_unix
          from
              (select device_sign,
                      gmt_occur_unix
               from t_sj_train_data_code_unix
               group by device_sign,
                        gmt_occur_unix) t1
          left outer join
              (select device_sign,
                      gmt_occur_unix
               from t_sj_train_data_code_unix) t2 on t1.device_sign = t2.device_sign
          where t2.gmt_occur_unix < t1.gmt_occur_unix)t
     where cnt_order <=20
     group by device_sign,
              gmt_occur_unix) t4 on t3.device_sign = t4.device_sign
and t3.gmt_occur_unix = t4.gmt_occur_unix;

-- 统计用户前20次中的变量分布情况
drop table if exists t_sj_train_device_client_ip_20n;


create table t_sj_train_device_client_ip_20n as
select t3.event_id,
       t4.client_ip,
       t4.device_client_ip_cnt_20n
from t_sj_train_data_code_unix t3
inner join
    (select device_sign,
            gmt_occur_unix,
            client_ip,
            count(*) as device_client_ip_cnt_20n
     from
         (select row_number() over(partition by t1.device_sign,t1.gmt_occur_unix
                                   order by t2.gmt_occur_unix desc) as cnt_order,
                 t1.device_sign,
                 t1.gmt_occur_unix,
                 t2.client_ip
          from
              (select device_sign,
                      gmt_occur_unix
               from t_sj_train_data_code_unix
               group by device_sign,
                        gmt_occur_unix) t1
          left outer join
              (select device_sign,
                      gmt_occur_unix,
                      client_ip
               from t_sj_train_data_code_unix) t2 on t1.device_sign = t2.device_sign
          where t2.gmt_occur_unix < t1.gmt_occur_unix)t
     where cnt_order <=20
     group by device_sign,
              gmt_occur_unix,
              client_ip)t4 on t3.device_sign = t4.device_sign
and t3.gmt_occur_unix = t4.gmt_occur_unix;
     
drop table if exists t_sj_train_device_network_20n;


create table t_sj_train_device_network_20n as
select t3.event_id,
       t4.network,
       t4.device_network_cnt_20n
from t_sj_train_data_code_unix t3
inner join
    (select device_sign,
            gmt_occur_unix,
            network,
            count(*) as device_network_cnt_20n
     from
         (select row_number() over(partition by t1.device_sign,t1.gmt_occur_unix
                                   order by t2.gmt_occur_unix desc) as cnt_order,
                 t1.device_sign,
                 t1.gmt_occur_unix,
                 t2.network
          from
              (select device_sign,
                      gmt_occur_unix
               from t_sj_train_data_code_unix
               group by device_sign,
                        gmt_occur_unix) t1
          left outer join
              (select device_sign,
                      gmt_occur_unix,
                      network
               from t_sj_train_data_code_unix) t2 on t1.device_sign = t2.device_sign
          where t2.gmt_occur_unix < t1.gmt_occur_unix)t
     where cnt_order <=20
     group by device_sign,
              gmt_occur_unix,
              network)t4 on t3.device_sign = t4.device_sign
and t3.gmt_occur_unix = t4.gmt_occur_unix;
     drop table if exists t_sj_train_device_user_id_20n;


 drop table if exists t_sj_train_device_user_id_20n;


create table t_sj_train_device_user_id_20n as
select t3.event_id,
       t4.device_sign,
       t4.device_user_id_cnt_20n
from t_sj_train_data_code_unix t3
inner join
    (select device_sign,
            gmt_occur_unix,
            device_sign,
            count(*) as device_user_id_cnt_20n
     from
         (select row_number() over(partition by t1.device_sign,t1.gmt_occur_unix
                                   order by t2.gmt_occur_unix desc) as cnt_order,
                 t1.device_sign,
                 t1.gmt_occur_unix,
                 t2.device_sign
          from
              (select device_sign,
                      gmt_occur_unix
               from t_sj_train_data_code_unix
               group by device_sign,
                        gmt_occur_unix) t1
          left outer join
              (select device_sign,
                      gmt_occur_unix,
                      device_sign
               from t_sj_train_data_code_unix) t2 on t1.device_sign = t2.device_sign
          where t2.gmt_occur_unix < t1.gmt_occur_unix)t
     where cnt_order <=20
     group by device_sign,
              gmt_occur_unix,
              device_sign)t4 on t3.device_sign = t4.device_sign
and t3.gmt_occur_unix = t4.gmt_occur_unix;
     drop table if exists t_sj_train_device_info_1_20n;


create table t_sj_train_device_info_1_20n as
select t3.event_id,
       t4.info_1,
       t4.device_info_1_cnt_20n
from t_sj_train_data_code_unix t3
inner join
    (select device_sign,
            gmt_occur_unix,
            info_1,
            count(*) as device_info_1_cnt_20n
     from
         (select row_number() over(partition by t1.device_sign,t1.gmt_occur_unix
                                   order by t2.gmt_occur_unix desc) as cnt_order,
                 t1.device_sign,
                 t1.gmt_occur_unix,
                 t2.info_1
          from
              (select device_sign,
                      gmt_occur_unix
               from t_sj_train_data_code_unix
               group by device_sign,
                        gmt_occur_unix) t1
          left outer join
              (select device_sign,
                      gmt_occur_unix,
                      info_1
               from t_sj_train_data_code_unix) t2 on t1.device_sign = t2.device_sign
          where t2.gmt_occur_unix < t1.gmt_occur_unix)t
     where cnt_order <=20
     group by device_sign,
              gmt_occur_unix,
              info_1)t4 on t3.device_sign = t4.device_sign
and t3.gmt_occur_unix = t4.gmt_occur_unix;
     drop table if exists t_sj_train_device_info_2_20n;


create table t_sj_train_device_info_2_20n as
select t3.event_id,
       t4.info_2,
       t4.device_info_2_cnt_20n
from t_sj_train_data_code_unix t3
inner join
    (select device_sign,
            gmt_occur_unix,
            info_2,
            count(*) as device_info_2_cnt_20n
     from
         (select row_number() over(partition by t1.device_sign,t1.gmt_occur_unix
                                   order by t2.gmt_occur_unix desc) as cnt_order,
                 t1.device_sign,
                 t1.gmt_occur_unix,
                 t2.info_2
          from
              (select device_sign,
                      gmt_occur_unix
               from t_sj_train_data_code_unix
               group by device_sign,
                        gmt_occur_unix) t1
          left outer join
              (select device_sign,
                      gmt_occur_unix,
                      info_2
               from t_sj_train_data_code_unix) t2 on t1.device_sign = t2.device_sign
          where t2.gmt_occur_unix < t1.gmt_occur_unix)t
     where cnt_order <=20
     group by device_sign,
              gmt_occur_unix,
              info_2)t4 on t3.device_sign = t4.device_sign
and t3.gmt_occur_unix = t4.gmt_occur_unix;
     drop table if exists t_sj_train_device_ip_prov_20n;


create table t_sj_train_device_ip_prov_20n as
select t3.event_id,
       t4.ip_prov,
       t4.device_ip_prov_cnt_20n
from t_sj_train_data_code_unix t3
inner join
    (select device_sign,
            gmt_occur_unix,
            ip_prov,
            count(*) as device_ip_prov_cnt_20n
     from
         (select row_number() over(partition by t1.device_sign,t1.gmt_occur_unix
                                   order by t2.gmt_occur_unix desc) as cnt_order,
                 t1.device_sign,
                 t1.gmt_occur_unix,
                 t2.ip_prov
          from
              (select device_sign,
                      gmt_occur_unix
               from t_sj_train_data_code_unix
               group by device_sign,
                        gmt_occur_unix) t1
          left outer join
              (select device_sign,
                      gmt_occur_unix,
                      ip_prov
               from t_sj_train_data_code_unix) t2 on t1.device_sign = t2.device_sign
          where t2.gmt_occur_unix < t1.gmt_occur_unix)t
     where cnt_order <=20
     group by device_sign,
              gmt_occur_unix,
              ip_prov)t4 on t3.device_sign = t4.device_sign
and t3.gmt_occur_unix = t4.gmt_occur_unix;
     drop table if exists t_sj_train_device_ip_city_20n;


create table t_sj_train_device_ip_city_20n as
select t3.event_id,
       t4.ip_city,
       t4.device_ip_city_cnt_20n
from t_sj_train_data_code_unix t3
inner join
    (select device_sign,
            gmt_occur_unix,
            ip_city,
            count(*) as device_ip_city_cnt_20n
     from
         (select row_number() over(partition by t1.device_sign,t1.gmt_occur_unix
                                   order by t2.gmt_occur_unix desc) as cnt_order,
                 t1.device_sign,
                 t1.gmt_occur_unix,
                 t2.ip_city
          from
              (select device_sign,
                      gmt_occur_unix
               from t_sj_train_data_code_unix
               group by device_sign,
                        gmt_occur_unix) t1
          left outer join
              (select device_sign,
                      gmt_occur_unix,
                      ip_city
               from t_sj_train_data_code_unix) t2 on t1.device_sign = t2.device_sign
          where t2.gmt_occur_unix < t1.gmt_occur_unix)t
     where cnt_order <=20
     group by device_sign,
              gmt_occur_unix,
              ip_city)t4 on t3.device_sign = t4.device_sign
and t3.gmt_occur_unix = t4.gmt_occur_unix;
     drop table if exists t_sj_train_device_mobile_oper_platform_20n;


create table t_sj_train_device_mobile_oper_platform_20n as
select t3.event_id,
       t4.mobile_oper_platform,
       t4.device_mobile_oper_platform_cnt_20n
from t_sj_train_data_code_unix t3
inner join
    (select device_sign,
            gmt_occur_unix,
            mobile_oper_platform,
            count(*) as device_mobile_oper_platform_cnt_20n
     from
         (select row_number() over(partition by t1.device_sign,t1.gmt_occur_unix
                                   order by t2.gmt_occur_unix desc) as cnt_order,
                 t1.device_sign,
                 t1.gmt_occur_unix,
                 t2.mobile_oper_platform
          from
              (select device_sign,
                      gmt_occur_unix
               from t_sj_train_data_code_unix
               group by device_sign,
                        gmt_occur_unix) t1
          left outer join
              (select device_sign,
                      gmt_occur_unix,
                      mobile_oper_platform
               from t_sj_train_data_code_unix) t2 on t1.device_sign = t2.device_sign
          where t2.gmt_occur_unix < t1.gmt_occur_unix)t
     where cnt_order <=20
     group by device_sign,
              gmt_occur_unix,
              mobile_oper_platform)t4 on t3.device_sign = t4.device_sign
and t3.gmt_occur_unix = t4.gmt_occur_unix;
     drop table if exists t_sj_train_device_operation_channel_20n;


create table t_sj_train_device_operation_channel_20n as
select t3.event_id,
       t4.operation_channel,
       t4.device_operation_channel_cnt_20n
from t_sj_train_data_code_unix t3
inner join
    (select device_sign,
            gmt_occur_unix,
            operation_channel,
            count(*) as device_operation_channel_cnt_20n
     from
         (select row_number() over(partition by t1.device_sign,t1.gmt_occur_unix
                                   order by t2.gmt_occur_unix desc) as cnt_order,
                 t1.device_sign,
                 t1.gmt_occur_unix,
                 t2.operation_channel
          from
              (select device_sign,
                      gmt_occur_unix
               from t_sj_train_data_code_unix
               group by device_sign,
                        gmt_occur_unix) t1
          left outer join
              (select device_sign,
                      gmt_occur_unix,
                      operation_channel
               from t_sj_train_data_code_unix) t2 on t1.device_sign = t2.device_sign
          where t2.gmt_occur_unix < t1.gmt_occur_unix)t
     where cnt_order <=20
     group by device_sign,
              gmt_occur_unix,
              operation_channel)t4 on t3.device_sign = t4.device_sign
and t3.gmt_occur_unix = t4.gmt_occur_unix;
     drop table if exists t_sj_train_device_pay_scene_20n;


create table t_sj_train_device_pay_scene_20n as
select t3.event_id,
       t4.pay_scene,
       t4.device_pay_scene_cnt_20n
from t_sj_train_data_code_unix t3
inner join
    (select device_sign,
            gmt_occur_unix,
            pay_scene,
            count(*) as device_pay_scene_cnt_20n
     from
         (select row_number() over(partition by t1.device_sign,t1.gmt_occur_unix
                                   order by t2.gmt_occur_unix desc) as cnt_order,
                 t1.device_sign,
                 t1.gmt_occur_unix,
                 t2.pay_scene
          from
              (select device_sign,
                      gmt_occur_unix
               from t_sj_train_data_code_unix
               group by device_sign,
                        gmt_occur_unix) t1
          left outer join
              (select device_sign,
                      gmt_occur_unix,
                      pay_scene
               from t_sj_train_data_code_unix) t2 on t1.device_sign = t2.device_sign
          where t2.gmt_occur_unix < t1.gmt_occur_unix)t
     where cnt_order <=20
     group by device_sign,
              gmt_occur_unix,
              pay_scene)t4 on t3.device_sign = t4.device_sign
and t3.gmt_occur_unix = t4.gmt_occur_unix;
     drop table if exists t_sj_train_device_amt_20n;


create table t_sj_train_device_amt_20n as
select t3.event_id,
       t4.amt,
       t4.device_amt_cnt_20n
from t_sj_train_data_code_unix t3
inner join
    (select device_sign,
            gmt_occur_unix,
            amt,
            count(*) as device_amt_cnt_20n
     from
         (select row_number() over(partition by t1.device_sign,t1.gmt_occur_unix
                                   order by t2.gmt_occur_unix desc) as cnt_order,
                 t1.device_sign,
                 t1.gmt_occur_unix,
                 t2.amt
          from
              (select device_sign,
                      gmt_occur_unix
               from t_sj_train_data_code_unix
               group by device_sign,
                        gmt_occur_unix) t1
          left outer join
              (select device_sign,
                      gmt_occur_unix,
                      amt
               from t_sj_train_data_code_unix) t2 on t1.device_sign = t2.device_sign
          where t2.gmt_occur_unix < t1.gmt_occur_unix)t
     where cnt_order <=20
     group by device_sign,
              gmt_occur_unix,
              amt)t4 on t3.device_sign = t4.device_sign
and t3.gmt_occur_unix = t4.gmt_occur_unix;
     drop table if exists t_sj_train_device_opposing_id_20n;


create table t_sj_train_device_opposing_id_20n as
select t3.event_id,
       t4.opposing_id,
       t4.device_opposing_id_cnt_20n
from t_sj_train_data_code_unix t3
inner join
    (select device_sign,
            gmt_occur_unix,
            opposing_id,
            count(*) as device_opposing_id_cnt_20n
     from
         (select row_number() over(partition by t1.device_sign,t1.gmt_occur_unix
                                   order by t2.gmt_occur_unix desc) as cnt_order,
                 t1.device_sign,
                 t1.gmt_occur_unix,
                 t2.opposing_id
          from
              (select device_sign,
                      gmt_occur_unix
               from t_sj_train_data_code_unix
               group by device_sign,
                        gmt_occur_unix) t1
          left outer join
              (select device_sign,
                      gmt_occur_unix,
                      opposing_id
               from t_sj_train_data_code_unix) t2 on t1.device_sign = t2.device_sign
          where t2.gmt_occur_unix < t1.gmt_occur_unix)t
     where cnt_order <=20
     group by device_sign,
              gmt_occur_unix,
              opposing_id)t4 on t3.device_sign = t4.device_sign
and t3.gmt_occur_unix = t4.gmt_occur_unix;





drop table if exists t_sj_train_device_scatter_freq_20n;


create table t_sj_train_device_scatter_freq_20n as
select t0.event_id,
       case
           when t2.device_client_ip_cnt_20n is null then 0
           else t2.device_client_ip_cnt_20n/t1.device_cnt_20n
       end as device_client_ip_rate_20n,
       case
           when t3.device_network_cnt_20n is null then 0
           else t3.device_network_cnt_20n/t1.device_cnt_20n
       end as device_network_rate_20n,
       case
           when t4.device_user_id_cnt_20n is null then 0
           else t4.device_user_id_cnt_20n/t1.device_cnt_20n
       end as device_user_id_rate_20n,
       case
           when t5.device_info_1_cnt_20n is null then 0
           else t5.device_info_1_cnt_20n/t1.device_cnt_20n
       end as device_info_1_rate_20n,
       case
           when t6.device_info_2_cnt_20n is null then 0
           else t6.device_info_2_cnt_20n/t1.device_cnt_20n
       end as device_info_2_rate_20n,
       case
           when t7.device_ip_prov_cnt_20n is null then 0
           else t7.device_ip_prov_cnt_20n/t1.device_cnt_20n
       end as device_ip_prov_rate_20n,
       case
           when t8.device_ip_city_cnt_20n is null then 0
           else t8.device_ip_city_cnt_20n/t1.device_cnt_20n
       end as device_ip_city_rate_20n,
       case
           when t9.device_mobile_oper_platform_cnt_20n is null then 0
           else t9.device_mobile_oper_platform_cnt_20n/t1.device_cnt_20n
       end as device_mobile_oper_platform_rate_20n,
       case
           when t10.device_operation_channel_cnt_20n is null then 0
           else t10.device_operation_channel_cnt_20n/t1.device_cnt_20n
       end as device_operation_channel_rate_20n,
       case
           when t11.device_pay_scene_cnt_20n is null then 0
           else t11.device_pay_scene_cnt_20n/t1.device_cnt_20n
       end as device_pay_scene_rate_20n,
       case
           when t12.device_amt_cnt_20n is null then 0
           else t12.device_amt_cnt_20n/t1.device_cnt_20n
       end as device_amt_rate_20n,
       case
           when t13.device_opposing_id_cnt_20n is null then 0
           else t13.device_opposing_id_cnt_20n/t1.device_cnt_20n
       end as device_opposing_id_rate_20n
from t_sj_train_data_code_unix t0
left outer join t_sj_train_device_cnt_20n t1 on t0.event_id = t1.event_id
left outer join t_sj_train_device_client_ip_20n t2 on t0.event_id = t2.event_id
and t0.client_ip = t2.client_ip
left outer join t_sj_train_device_network_20n t3 on t0.event_id = t3.event_id
and t0.network = t3.network
left outer join t_sj_train_device_user_id_20n t4 on t0.event_id = t4.event_id
and t0.user_id = t4.user_id
left outer join t_sj_train_device_info_1_20n t5 on t0.event_id = t5.event_id
and t0.info_1 = t5.info_1
left outer join t_sj_train_device_info_2_20n t6 on t0.event_id = t6.event_id
and t0.info_2 = t6.info_2
left outer join t_sj_train_device_ip_prov_20n t7 on t0.event_id = t7.event_id
and t0.ip_prov = t7.ip_prov
left outer join t_sj_train_device_ip_city_20n t8 on t0.event_id = t8.event_id
and t0.ip_city = t8.ip_city
left outer join t_sj_train_device_mobile_oper_platform_20n t9 on t0.event_id = t9.event_id
and t0.mobile_oper_platform = t9.mobile_oper_platform
left outer join t_sj_train_device_operation_channel_20n t10 on t0.event_id = t10.event_id
and t0.operation_channel = t10.operation_channel
left outer join t_sj_train_device_pay_scene_20n t11 on t0.event_id = t11.event_id
and t0.pay_scene = t11.pay_scene
left outer join t_sj_train_device_amt_20n t12 on t0.event_id = t12.event_id
and t0.amt = t12.amt
left outer join t_sj_train_device_opposing_id_20n t13 on t0.event_id = t13.event_id
and t0.opposing_id = t13.opposing_id;