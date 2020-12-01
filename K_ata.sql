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
and tp.timecategory is not null;


--Kate OF (SLA final)
with main as (

SELECT fo.order_gk,
fo.driver_gk,
(CASE when v.vendor_name like '%courier car%' THEN 'PHV'
            when v.vendor_name like '%courier pedestrian%' THEN 'Pedestrian'
            when v.vendor_name like '%courier scooter%' THEN 'Scooters'
            when v.vendor_name like '%courier trike%' THEN 'E-bikes'
            ELSE 'taxi'
       end) AS supply_type,

fo.ordering_corporate_account_gk,
ca.corporate_account_name,
accounts.name_internal,
ct.lob_desc,
ct.class_type_desc,
fo.class_type_key,
date_format (fo.date_key, '%W') AS weekday,
fo.date_key,
tp.timecategory,
tp.subperiod,
tp.subperiod2 AS time_period,
fo.order_datetime,
loc.city_name,
fo.is_future_order_key,

CASE when date_diff('second', fo.order_datetime,fo.driver_arrived_datetime) > 0 THEN
date_diff('second', fo.order_datetime, fo.driver_arrived_datetime)/60.00 end AS ATA_delivery,
fo.m_order_ata/60.00 AS ATA_default,
CASE when date_diff('second', fo.order_datetime,fo.driver_arrived_datetime) < 0 THEN 0
ELSE date_diff('second', fo.order_datetime,fo.driver_arrived_datetime)/60 end AS ATA_att

FROM emilia_gettdwh.dwh_fact_orders_v fo
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v ca ON fo.ordering_corporate_account_gk = ca.corporate_account_gk
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v ct ON fo.class_type_key=ct.class_type_key
LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" dd ON dd.driver_gk = fo.driver_gk
LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v v ON v.vendor_gk = dd.fleet_gk
LEFT JOIN emilia_gettdwh.dwh_dim_locations_v loc ON fo.origin_location_key = loc.location_key
LEFT JOIN emilia_gettdwh.dwh_dim_order_cancellation_stages_v cs ON fo.order_cancellation_stage_key = cs.order_cancellation_stage_key
LEFT JOIN sheets."default".delivery_corp_accounts_20191203 AS accounts ON cast(accounts.company_gk AS bigint)=fo.ordering_corporate_account_gk
LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.hour_key = fo.hour_key and tp.date_key = fo.date_key
    and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months', '7.Std Hours')

WHERE fo.lob_key IN (5,6)
and fo.country_key = 2
and fo.order_status_key=7
and ct.class_family not IN ('Premium')
and tp.timecategory is not null
)

(select name_internal, subperiod, sum(ATA_delivery) / count(order_gk) avg_ata1,
sum(ATA_delivery) / count(case when ATA_delivery <> 0 then ATA_delivery end),
count(case when ATA_delivery <> 0 then ATA_delivery end)

from main
where subperiod in ('W37', 'W38','W39','W36')
group by 1,2);
--> DIFFERENCE FROM MINE:
-- 1) I do not calculate 0 ata
-- 2) I calculate all statuses



--- NEW FLOW () - To compare with my scripts output, I added with main as...
-- and extra fields (see in the script)
with main as (
SELECT d.company_id,
date(j.scheduled_at) AS date_key,
  ca.corporate_account_name,
  json_extract_scalar( "initial_actions", '$.actions_list.0.geo.address') AS pickup_address,
  j.supplier_id,
  (CASE when v.vendor_name like '%courier car%' THEN 'PHV'
                              when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                              when v.vendor_name like '%courier scooter%' THEN 'scooter'
                              when  v.vendor_name like '%courier trike%' THEN 'e-bike'
                              when v.vendor_name is null THEN NULL
                              ELSE 'taxi' end) AS supply_type,
  j.scheduled_at,
  tp.timecategory,
  tp.subperiod,
  j.started_at,
  j.ended_at,
  j.id,
  j.status AS journey_status,
 j.display_identifier AS journey_display_identifier,
j.legacy_order_id,
ct.class_type_desc,
fo.est_distance,
fo.est_duration,
fo.is_went_to_cc_key,
fo.m_routing_duration*1.00/60 AS routing_duration_min,
fo.order_confirmed_datetime - interval '3' hour AS driver_assigned_datetime,
 customer_total_cost_inc_vat,
 driver_total_cost_inc_vat,
 driver_total_commission_exc_vat,
  customer_total_cost -  driver_total_cost_inc_vat -  driver_total_commission_exc_vat AS take_rate,
count(d.id) AS all_deliveries,
count (CASE when d.status = 'completed' and d.display_identifier <> 'Returns' THEN d.id end )AS completed,
count (CASE when d.status = 'not_delivered' and d.display_identifier <> 'Returns' THEN d.id end )AS not_delivered,
count( CASE when d.status = 'cancelled' and d.display_identifier <> 'Returns' THEN d.id end) AS cancelled,
count (CASE when d.status = 'rejected' and d.display_identifier <> 'Returns' THEN d.id end ) AS rejected,
count(CASE when d.display_identifier = 'Returns' THEN 1 end) AS has_return,

count(CASE when d.arrived_at is not null THEN d.id end) AS ATA_count,
sum(date_diff('second', j.scheduled_at, d.arrived_at )*1.00/60) AS ATA_sum,
--my insert to check number of not null
count(case when date_diff('second', j.scheduled_at, d.arrived_at )*1.00/60>0 then d.id end) AS ATA_count_notnull,
sum(case when date_diff('second', j.scheduled_at, d.arrived_at )*1.00/60 > 0 then
    date_diff('second', j.scheduled_at, d.arrived_at )*1.00/60 end) AS ATA_sum_positive,

count(CASE when d.dropped_off_at is not null or d.ended_at is not null THEN d.id end) AS TOD_count,
sum(date_diff('second',j.scheduled_at, coalesce (d.dropped_off_at, d.ended_at) )*1.00/60) AS TOD_sum,
sum(date_diff('second',picked_up_at, arrived_to_drop_off_at)*1.00/60) AS ride_duration_sum,
count(CASE when arrived_to_drop_off_at is not null THEN d.id end) AS ride_duration_count

FROM delivery.public.deliveries AS d
LEFT JOIN delivery.public.journeys AS j ON j.id = d.journey_id and j.env = d.env
LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" dd ON dd.source_id = j.supplier_id and j.env = dd.country_symbol
LEFT JOIN "emilia_gettdwh"."dwh_dim_vendors_v" v ON dd.fleet_gk = v.vendor_gk
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca ON cast(ca.source_id AS varchar(128)) = d.company_id and d.env = ca.country_symbol
LEFT JOIN emilia_gettdwh.dwh_fact_orders_v AS fo ON j.legacy_order_id = fo.sourceid
and fo.country_key = 2 and lob_key = 5 and year(fo.date_key) > 2019
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v ct ON fo.class_type_key=ct.class_type_key
LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.date_key = date(j.scheduled_at) and tp.hour_key = 0
        and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')

WHERE
  j.env = 'RU'
  and date(j.scheduled_at) >= date'2020-01-01'
  and ct.class_type_desc like '%ondemand%'
  and d.company_id <> '17459'
 and d.status IN ('completed', 'not_delivered', 'cancelled', 'rejected')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25)

(select corporate_account_name, subperiod, sum(ATA_sum) / sum(ATA_count) avg_ata, sum(ATA_count)
, sum(ata_count_notnull) ata_not_null,  sum(ATA_sum_positive) / sum(ata_count_notnull) avg_ata_notnull
from main
where subperiod in ('W37', 'W38','W39','W36')
group by 1,2
order by corporate_account_name, subperiod);
--> DIFFERENCE FROM MINE:
-- 1) I do not calculate 0 ata,
-- 2) I filter by (date_diff('second', fd.created_at, fd.scheduled_at))*1.00/60 <= 20
-- 3) I do not calculate negative ATA


