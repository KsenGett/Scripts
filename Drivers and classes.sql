    with rd as
             (
                 select distinct fo.driver_gk,
                                 d.registration_date_key,
                                 (CASE
                                      when v.vendor_name like '%courier car%' THEN 'PHV'
                                      when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                                      when v.vendor_name is null THEN NULL
                                      ELSE 'taxi' end) AS                                                     supply_type,
--     count(distinct case when ordering_corporate_account_gk <> 20004730 and ct.class_family <> 'Premium'
--                                 then order_gk end) orders_OF,
--     count(distinct case when ordering_corporate_account_gk = 20004730 then order_gk end) orders_NF,
                                 count(distinct
                                       case when ordering_corporate_account_gk <> 20004730 then order_gk end) journeys

                 from emilia_gettdwh.dwh_fact_orders_v fo
                          left join emilia_gettdwh.dwh_dim_drivers_v d on d.driver_gk = fo.driver_gk
                          left join emilia_gettdwh.dwh_dim_vendors_v v on d.fleet_gk = v.vendor_gk
                          left join emilia_gettdwh.dwh_dim_class_types_v ct on ct.class_type_key = fo.class_type_key
                     and ct.country_key = 2 and ct.lob_key in (5, 6)
                     -- driver classes info
                          left join emilia_gettdwh.dwh_fact_drivers_classes_v drcl on drcl.driver_gk = d.driver_gk


                 where fo.lob_key in (5, 6)
                   and fo.country_key = 2
                   and order_status_key = 7
                   and d.driver_status in ('Operational')
                   and fo.date_key >= (current_date - interval '30' day)
                   and origin_location_key = 245
                   --and d.registration_date_key between current_date - interval '30' day and current_date
                   and drcl.TO_DATE_KEY > (current_date - interval '3' day)
                 group by 1, 2, 3
             )

    select *
--coalesce(orders_OF, 0) + coalesce(orders_NF, 0) deliveries

    from rd

    where (supply_type = 'taxi' and journeys > 20)
       or (supply_type = 'PHV' and journeys > 30)
       or (supply_type = 'pedestrian' and journeys > 20);



-- for Pasha K
with mvideo as (
    with rd as
             (
                 select distinct fo.driver_gk,
                                 d.registration_date_key,
                                 (CASE
                                      when v.vendor_name like '%courier car%' THEN 'PHV'
                                      when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                                      when v.vendor_name is null THEN NULL
                                      ELSE 'taxi' end) AS                                                     supply_type,
--     count(distinct case when ordering_corporate_account_gk <> 20004730 and ct.class_family <> 'Premium'
--                                 then order_gk end) orders_OF,
--     count(distinct case when ordering_corporate_account_gk = 20004730 then order_gk end) orders_NF,
                                 count(distinct
                                       case when ordering_corporate_account_gk <> 20004730 then order_gk end) journeys

                 from emilia_gettdwh.dwh_fact_orders_v fo
                          left join emilia_gettdwh.dwh_dim_drivers_v d on d.driver_gk = fo.driver_gk
                          left join emilia_gettdwh.dwh_dim_vendors_v v on d.fleet_gk = v.vendor_gk
                          left join emilia_gettdwh.dwh_dim_class_types_v ct on ct.class_type_key = fo.class_type_key
                     and ct.country_key = 2 and ct.lob_key in (5, 6)
                     -- driver classes info
                          left join emilia_gettdwh.dwh_fact_drivers_classes_v drcl on drcl.driver_gk = d.driver_gk


                 where fo.lob_key in (5, 6)
                   and fo.country_key = 2
                   and order_status_key = 7
                   and d.driver_status in ('Operational')
                   and fo.date_key >= (current_date - interval '30' day)
                   and origin_location_key = 245
                   --and d.registration_date_key between current_date - interval '30' day and current_date
                  and drcl.TO_DATE_KEY > (current_date - interval '3' day)
                 group by 1, 2, 3
             )

    select *,
           (supply_type = 'taxi' and journeys > 20)
       or (supply_type = 'PHV' and journeys > 30)
       or (supply_type = 'pedestrian' and journeys > 20) suits_mvideo
--coalesce(orders_OF, 0) + coalesce(orders_NF, 0) deliveries

    from rd

--     where (supply_type = 'taxi' and journeys > 20)
--        or (supply_type = 'PHV' and journeys > 30)
--        or (supply_type = 'pedestrian' and journeys > 20);
)
select supply_type, suits_mvideo, count(distinct driver_gk) last_30days_active_drivers,
       sum(journeys)/count(distinct driver_gk) avg_journeys_perdriver_last30days
       from mvideo
group by 1,2;


-- couriers and classes desh
/*
Owner - Ekaterina Nesterova
Cube Name - Couriers and Classes
ID - 21C4DD3C11EA47F9E8760080EF95D0E6
*/
with main as (
    SELECT distinct dc.driver_gk,
                    dd.driver_name                AS driver_name,
                    dd.primary_city_id,
                    dl.city_name,
                    --d.city                AS gt_city_name,
                    dd.driver_status,
                    dd.registration_date_key,
                    dd.ftp_date_key,
                    dd.ltp_date_key,
                    dd.phone,
                    dd.phone2,
                    dd.device_platform,
                    dd.fleet_gk,
                    df.vendor_name,
                    df.email              as fleet_email,
                    (CASE
                         when df.vendor_name like '%courier car%' THEN 'PHV'
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
                    fo.orders                deliveries_last_30_days_OF,
                    md.deliveries            deliveries_last_30_days_NF,
                    md.journeys              journeys_last_30_days_NF


    FROM emilia_gettdwh.dwh_fact_drivers_classes_v AS dc
             LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = dc.class_type_key
             LEFT JOIN emilia_gettdwh.dwh_dim_drivers_v AS dd ON dd.driver_gk = dc.driver_gk
        --and registration_date_key >= date'2019-01-01'
--              LEFT JOIN "gt-ru".gettaxi_ru_production.drivers d
--                        ON cast(dd.driver_gk AS varchar) = concat('2000', cast(d.id AS varchar))
             LEFT JOIN emilia_gettdwh.dwh_dim_locations_v AS dl ON dl.city_id = dd.primary_city_id
             LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v AS df ON df.vendor_gk = dd.fleet_gk

             left join --4 sec
        (
            select distinct driver_gk,
                            count(distinct order_gk) orders

            from emilia_gettdwh.dwh_fact_orders_v fo
                     left join emilia_gettdwh.dwh_dim_class_types_v AS ct
                               ON ct.class_type_key = fo.class_type_key

            where fo.lob_key in (5, 6)
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
                            count(distinct delivery_gk) deliveries,
                            count(distinct journey_gk)  journeys

            from model_delivery.dwh_fact_deliveries_v

            where
                date (created_at) between current_date - interval '30' day and current_date
    and delivery_status_id = 4

group by 1

    ) md
on md.courier_gk = dd.driver_gk


WHERE ct.country_key = 2
  and ct.lob_key IN (5
    , 6)
  and dc.is_current_allocation = 1
  and dd.country_key = 2
  and dd.phone is not NULL
  and lower (dd.driver_name) not like '%тест%'
  and lower (dd.driver_name) not like '%test%'
  and lower (dd.driver_name) not like '%увол%'
  and lower (df.vendor_name) not like '%тест%'
  and lower (df.vendor_name) not like '%test%'
  and dd.phone <> '0'
  and dd.phone <> '8'

--group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
ORDER BY
    dc.driver_gk asc
    )
(select
*
from main
where class_type_desc like '%rostov-on-don delivery phv'
and coalesce(deliveries_last_30_days_OF, deliveries_last_30_days_NF, 0) <>0)