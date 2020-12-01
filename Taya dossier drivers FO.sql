--new
SELECT
 distinct dc.driver_gk,
 d.name AS driver_name,
 dd.primary_city_id,
 dl.city_name,
 d.city AS gt_city_name,
 dd.driver_status,
dd.registration_date_key,
dd.ftp_date_key,
dd.ltp_date_key,
 dd.phone,
 dd.phone2,
 dd.device_platform,
 dd.fleet_gk,
 df.vendor_name,
 (CASE when df.vendor_name like '%courier car%' THEN 'PHV'
 when df.vendor_name like '%courier pedestrian%' THEN 'pedestrians'
 when df.vendor_name like '%courier scooter%' THEN 'scooter'
  when df.vendor_name like '%courier trike%' THEN 'e-bike'
 ELSE 'taxi' end) AS supply_type,
 dd.car_model,
 dd.car_number,
 dd.is_test,
 dd.is_frozen,
 dd.frozen_comment,
 dd.driver_computed_rating,
 ct."lob_desc",
 dc.from_date_key,
 dc.to_date_key,
 dc.dwh_update_date,
 dd."number_of_rides",
 dd."number_of_rides_last_30_days",
 dc.class_type_key,
ct.class_type_desc,
 ---count(distinct order_gk),
fo.orders deliveries_last_30_days_OF,
md.deliveries deliveries_last_30_days_NF,
md.journeys journeys_last_30_days_NF



 FROM emilia_gettdwh.dwh_fact_drivers_classes_v AS dc
 LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = dc.class_type_key
 LEFT JOIN emilia_gettdwh.dwh_dim_drivers_v AS dd ON dd.driver_gk = dc.driver_gk and
            registration_date_key >= date'2019-01-01'
  LEFT JOIN  "gt-ru".gettaxi_ru_production.drivers d ON cast(dd.driver_gk AS varchar) = concat('2000', cast(d.id AS varchar))
 LEFT JOIN emilia_gettdwh.dwh_dim_locations_v AS dl ON dl.city_id = dd.primary_city_id
 LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v AS df ON df.vendor_gk = dd.fleet_gk

 left join --4 sec
  (
        select
        distinct driver_gk,
        count(distinct order_gk) orders

        from emilia_gettdwh.dwh_fact_orders_v fo
        left join emilia_gettdwh.dwh_dim_class_types_v AS ct
            ON ct.class_type_key = fo.class_type_key

        where fo.lob_key in (5,6)
        and date_key between current_date - interval '30' day and current_date
        and ordering_corporate_account_gk <> 20004730
        and order_status_key = 7
        and fo.country_key = 2
        and ct.class_family <> 'Premium'

        group by 1

    ) fo on fo.driver_gk = dd.driver_gk

    left join --2sec
    (
        select distinct courier_gk,
        count(distinct case when delivery_status_id = 4 then delivery_gk end) compl_deliveries,
        count(distinct journey_gk) journeys

        from model_delivery.dwh_fact_deliveries_v d
        left join model_delivery.dwh_fact_journeys_v j on d.journey_gk = j.

        where
        date(created_at) between current_date - interval '30' day and current_date
        and delivery_status_id = 4

        group by 1

    ) md on md.courier_gk  = dd.driver_gk



 WHERE ct.country_key = 2
 and ct.lob_key IN (5,6)
 and dc.is_current_allocation = 1
 and dd.country_key = 2
 and dd.phone is not NULL
 and lower(dd.driver_name) not like '%тест%'
 and lower(dd.driver_name) not like '%test%'
 and lower(dd.driver_name) not like '%увол%'
 and lower(df.vendor_name) not like '%тест%'
 and lower(df.vendor_name) not like '%test%'
 and dd.phone <> '0'
 and dd.phone <> '8'

 --group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
 ORDER BY





--save origin
SELECT
 dc.driver_gk,
 d.name AS driver_name,
 dd.primary_city_id,
 dl.city_name,
 d.city AS gt_city_name,
 dd.driver_status,
dd.registration_date_key,
dd.ftp_date_key,
dd.ltp_date_key,
 dd.phone,
 dd.phone2,
 dd.device_platform,
 dd.fleet_gk,
 df.vendor_name,
 (CASE when df.vendor_name like '%courier car%' THEN 'PHV'
 when df.vendor_name like '%courier pedestrian%' THEN 'pedestrians'
 when df.vendor_name like '%courier scooter%' THEN 'scooter'
  when df.vendor_name like '%courier trike%' THEN 'e-bike'
 ELSE 'taxi' end) AS supply_type,
 dd.car_model,
 dd.car_number,
 dd.is_test,
 dd.is_frozen,
 dd.frozen_comment,
 dd.driver_computed_rating,
 dc.class_type_key,
 ct.class_type_desc,
 ct."lob_desc",
 dc.from_date_key,
 dc.to_date_key,
 dc.dwh_update_date,
 dd."number_of_rides",
 dd."number_of_rides_last_30_days"
 FROM emilia_gettdwh.dwh_fact_drivers_classes_v AS dc
 LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = dc.class_type_key
 LEFT JOIN emilia_gettdwh.dwh_dim_drivers_v AS dd ON dd.driver_gk = dc.driver_gk
  LEFT JOIN  "gt-ru".gettaxi_ru_production.drivers d ON cast(dd.driver_gk AS varchar) = concat('2000', cast(d.id AS varchar))
 LEFT JOIN emilia_gettdwh.dwh_dim_locations_v AS dl ON dl.city_id = dd.primary_city_id
 LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v AS df ON df.vendor_gk = dd.fleet_gk
 WHERE ct.country_key = 2
 and lob_key IN (5,6)
 and dc.is_current_allocation = 1
 and dd.country_key = 2
 and dd.phone is not NULL
 and lower(dd.driver_name) not like '%тест%'
 and lower(dd.driver_name) not like '%test%'
 and lower(dd.driver_name) not like '%увол%'
 and lower(df.vendor_name) not like '%тест%'
 and lower(df.vendor_name) not like '%test%'
 and dd.phone != '0'
 and dd.phone != '8'
 ORDER BY
 dc.driver_gk asc;

desc model_delivery.dwh_fact_journeys_v;