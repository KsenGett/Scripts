
fo.est_duration,
fo.est_distance,

select date_key, j.id, d.legacy_order_id, j.legacy_order_id,fo.order_gk,
dest_full_address, origin_full_address,
 fo.order_gk,series_original_order_gk, series_final_order_gk,
est_distance, est_duration, lob_key, dest_full_address, origin_full_address,
fo.dropoff_latitude, fo.dropoff_longitude, fo.*
--, fd.pickup_longitude, fd.pickup_latitude, fd.dropoff_latitude, fd.dropoff_longitude


from emilia_gettdwh.dwh_fact_orders_v fo
--left join model_delivery."dwh_fact_deliveries_v" fd on fo.order_gk = fd.order_gk
left join delivery."public".deliveries d on fo.sourceid = d.legacy_order_id
and fo.country_symbol = d.env
left join delivery."public".journeys j on fo.sourceid = j.legacy_order_id
and fo.country_symbol = j.env --and j.id = d.journey_id

--and series_original_order_gk <> series_final_order_gk
where fo.date_key = date'2020-10-08'
and journey_id in (187514,187513,187512)
and order_status_key = 7
--and ordering_corporate_account_gk = 20004730
;
--20001390143898,20001390143904, 20001390143893 --fact orders
--20001390163275, 20001390165155,20001390185708 - 4730

select * from delivery."public".deliveries where legacy_order_id = 1390143893
/*
Москва ул. Академика Янгеля 6к1 ->Москва улица Газопровод 3к1 = 2км 9 мин
                                                    V 7 мин 2.5 км
Москва ул. Академика Янгеля 6к1 -> 117405 г Москва Варшавское шоссе д 141А к 1 = 3.7 км 10 мин

Москва ул. Ивантеевская д. 32 корп. 2 -> Москва Ивантеевская улица 13 600 м 2 мин
                                                 V 1 мин 240 м
Москва ул. Ивантеевская д. 32 корп. 2 - Москва ул Ивантеевская д. 7/20 800 м 2 мин

Мытищи ул. Комарова д. 5 - Московская область Мытищи Рождественская улица 11 1.2 км  мин
                                            V 650 м 3 мин
Мытищи ул. Комарова д. 5 - Московская область Мытищи Рождественская улица 2 1.1 км 3 мин
*/

select * from model_delivery."dwh_fact_deliveries_v"
 where journey_gk = 20001390143893

--22332 journey_id




select
fo.date_key,
/*tp.timecategory,
tp.subperiod,
tp.period,
tp.subperiod2 AS time_period,*/

(case when ct.class_family = 'Premium' then 'NF' else 'OF' end) as platform,

(CASE when fo.lob_key = 6 THEN 'C2C'
   when am.name like '%Delivery%' or ca.account_manager_gk IN(100079, 100096, 100090, 100073, 100088)
   THEN 'eCommerce' ELSE 'Corporate' end ) AS client_type,

ca.corporate_account_name,
--accounts.name_internal,
ca.corporate_account_gk,
loc.city_name,
sum(driver_total_cost),
sum(fo.est_distance) est_distance,
count(fo.est_distance) est_distance_count,
sum(fo.est_duration) est_duration,
count(fo.est_duration) est_duration_count
/*,
count (distinct CASE when fo.order_status_key = 7 THEN fo.order_gk ELSE null end) AS completed_orders,
count(distinct fo.order_gk) gross_orders */


from emilia_gettdwh.dwh_fact_orders_v fo

LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.hour_key = fo.hour_key and tp.date_key = fo.date_key
       and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
          ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key
LEFT JOIN "emilia_gettdwh"."dwh_dim_account_managers_v" am
    ON am."account_manager_gk" = ca."account_manager_gk"
LEFT JOIN sheets."default".delivery_corp_accounts_20191203 AS accounts
    ON cast(accounts.company_gk AS bigint)=fo.ordering_corporate_account_gk
left join emilia_gettdwh.dwh_dim_locations_v loc on fo.origin_location_key = loc.location_key

where tp.timecategory is not null
and fo.date_key >= date'2018-12-01'
and fo.country_key = 2
and fo.lob_key in (5,6)
and ct.class_family = ('Premium')
and ct.class_group not like 'Test'
and ordering_corporate_account_gk not in (200017459, 20004730)
group by 1,2,3,4,5,6


select * from delivery."public".journeys where id = 24062


select j.legacy_order_id
from "model_delivery"."dwh_fact_deliveries_v" fd
left join delivery."public".journeys j on cast(fd.journey_gk as varchar) = concat('2000',cast(j.id as varchar))
where journey_gk = 2000171525

--2000171525 -45 deliveries journey gk from fd
-- legacy order id 1384019667 from j
select * from emilia_gettdwh.dwh_fact_orders_v
where order_gk = 20001384019667
