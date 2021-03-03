with rd as
    (
    select fo.driver_gk,d.registration_date_key,
    (CASE when v.vendor_name like '%courier car%' THEN 'PHV'
                                  when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                                  when v.vendor_name is null THEN NULL
                                  ELSE 'taxi' end) AS supply_type,
--     count(distinct case when ordering_corporate_account_gk <> 20004730 and ct.class_family <> 'Premium'
--                                 then order_gk end) orders_OF,
--     count(distinct case when ordering_corporate_account_gk = 20004730 then order_gk end) orders_NF,
    count(distinct case when  ordering_corporate_account_gk <> 20004730 then order_gk end) journeys

    from emilia_gettdwh.dwh_fact_orders_v fo
    left join emilia_gettdwh.dwh_dim_drivers_v d on d.driver_gk = fo.driver_gk
    left join emilia_gettdwh.dwh_dim_vendors_v v on d.fleet_gk = v.vendor_gk
    left join emilia_gettdwh.dwh_dim_class_types_v ct on ct.class_type_key = fo.class_type_key

    where fo.lob_key in (5,6)
    and fo.country_key = 2
    and order_status_key = 7
    --and ct.class_family not in ('Premium')
    and d.driver_status in ('Operational')
    and fo.date_key >= (current_date - interval '30' day)
    and origin_location_key = 245
    --and d.registration_date_key between current_date - interval '30' day and current_date
    group by 1,2,3
    )
, rs as
    (
    select fdc.driver_gk, fdc.to_date_key
    from emilia_gettdwh.dwh_fact_drivers_classes_v fdc
    where  TO_DATE_KEY > (current_date - interval '3' day)
    )

select
distinct rs.driver_gk as driver,
registration_date_key,
supply_type,
journeys
--coalesce(orders_OF, 0) + coalesce(orders_NF, 0) deliveries

from rd
right join rs on rd.driver_gk = rs.driver_gk
where (supply_type = 'taxi' and journeys > 20 )
or (supply_type = 'PHV' and journeys > 30 )
or (supply_type = 'pedestrian' and journeys > 20 );
