with mao AS(
    SELECT try_cast( concat('2000',json_extract_scalar(from_utf8(ae.payload), '$.order_id')) AS BIGINT) AS manually_assigned_order_gk
       FROM events AS ae
       WHERE ae.event_name='matching|driver_assigned_by_cc'
       and ae.env IN ('RU')
       and ae.event_date between current_date - interval '3' month and current_date - interval '1' day
       )

SELECT fo.order_gk,
fo.driver_gk,
(CASE when v.vendor_name like '%courier car%' THEN 'PHV'
            when v.vendor_name like '%courier pedestrian%' THEN 'Pedestrian'
            when v.vendor_name like '%courier scooter%' THEN 'Scooters'
            when v.vendor_name like '%courier trike%' THEN 'E-bikes'
            ELSE 'taxi'
       end) AS supply_type,
v.vendor_name,
fo.ordering_corporate_account_gk,
ca.corporate_account_name,
accounts.name_internal,
ct.lob_desc,
ct.class_type_desc,
fo.class_type_key,
date_format (fo.date_key, '%W') AS weekday,
fo.date_key,
tp.timecategory,
tp.subperiod2 AS time_period,

fo.order_datetime,
fo.order_confirmed_datetime,
fo.driver_arrived_datetime,

fo.ride_start_datetime,
fo.ride_end_datetime,
loc.city_name,
fo.origin_full_address,
fo.origin_latitude,
fo.origin_longitude,
fo.est_duration,
fo.est_distance,
fo.est_duration AS est_duration_m,
fo.est_distance AS est_distance_m,
(CASE when mao.manually_assigned_order_gk is null THEN 0 ELSE 1 end) AS is_manualy_assigned,
fo.is_future_order_key,

CASE when date_diff('second', fo.order_datetime,fo.driver_arrived_datetime) < 0 THEN 0
ELSE date_diff('second', fo.order_datetime, fo.driver_arrived_datetime)/60.00 end AS ATA_delivery,

fo.m_order_ata/60.00 AS ATA_default,

CASE when date_diff('second', fo.order_datetime,fo.driver_arrived_datetime) < 0 THEN 0
ELSE date_diff('second', fo.order_datetime,fo.driver_arrived_datetime)/60 end AS ATA_att,

date_diff('second', fo.order_datetime,fo.ride_end_datetime)/60.00 AS total_duration,

date_diff('second', fo.order_datetime,fo.ride_end_datetime)/60 AS total_duration_att,

fo.m_ride_duration/60.00 AS ride_duration_min,
fo.m_ride_duration/60.00 AS ride_duration_min_att,
fo.driver_waiting_duration_on_pickup/60.00 AS waiting_on_pu_min,
fo.driver_waiting_duration_on_pickup/60 AS waiting_on_pu_min_att,
fo.m_order_eta/60.00 AS order_eta_min

FROM emilia_gettdwh.dwh_fact_orders_v fo

LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v ca ON fo.ordering_corporate_account_gk = ca.corporate_account_gk
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v ct ON fo.class_type_key=ct.class_type_key
LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" dd ON dd.driver_gk = fo.driver_gk
LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v v ON v.vendor_gk = dd.fleet_gk
LEFT JOIN emilia_gettdwh.dwh_dim_locations_v loc ON fo.origin_location_key = loc.location_key
LEFT JOIN emilia_gettdwh.dwh_dim_order_cancellation_stages_v cs ON fo.order_cancellation_stage_key = cs.order_cancellation_stage_key
LEFT JOIN sheets."default".delivery_corp_accounts_20191203 AS accounts ON cast(accounts.company_gk AS bigint)=fo.ordering_corporate_account_gk

LEFT JOIN mao ON mao.manually_assigned_order_gk  = fo.order_gk
LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.hour_key = fo.hour_key and tp.date_key = fo.date_key
and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months', '7.Std Hours')
WHERE fo.lob_key IN (5,6)
and fo.country_key = 2
and fo.date_key between (current_date - interval '3' month) and (current_date) --данные за последние 3 месяца
and fo.order_status_key=7
and ct.class_family not IN ('Premium')
and tp.timecategory is not null