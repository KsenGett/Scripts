-- OF
with ata as (
    select date_key, order_gk, order_status_key,

    CASE when date_diff('minute', fo.order_datetime,fo.driver_arrived_datetime) < 0 THEN 0
    ELSE date_diff('minute', fo.order_datetime, fo.driver_arrived_datetime) end AS ata


    from emilia_gettdwh.dwh_fact_orders_v fo
    where lob_key in (5,6)
    and country_key = 2
)
(select
fo.date_key,
tp.timecategory,
tp.subperiod,
tp.period,
tp.subperiod2 AS time_period,
'OF' as platform,

(CASE when fo.lob_key = 6 THEN 'C2C'
   when am.name like '%Delivery%' or ca.account_manager_gk IN( 100079, 100096, 100090, 100073, 100088)
   THEN 'eCommerce' ELSE 'Corporate' end ) AS client_type,

-- CHECKED - is the same with SLA final for company 20004469
avg(ata.ata) avg_ata,


count (distinct CASE when fo.order_status_key = 7 THEN fo.series_original_order_gk ELSE null end) AS completed_orders,

-- TO CHECK - e-com is the same, corporate is different
count (distinct CASE when (fo.order_status_key = 7 or (fo.order_status_key = 4 and driver_total_cost > 0))
THEN fo.order_gk ELSE null end) AS completed_and_cancelled_orders,
count(distinct fo.series_original_order_gk) as net_orders,
count( distinct  fo.order_gk) gross_orders

from emilia_gettdwh.dwh_fact_orders_v fo

LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.hour_key = fo.hour_key and tp.date_key = fo.date_key
       and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
          ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key
LEFT JOIN "emilia_gettdwh"."dwh_dim_account_managers_v" am ON am."account_manager_gk" = ca."account_manager_gk"
left join ata on fo.date_key = ata.date_key and fo.order_gk = ata.order_gk
    and fo.order_status_key = ata.order_status_key

where tp.timecategory is not null
and fo.date_key >= date'2020-08-01'
and fo.country_key = 2
and fo.lob_key in (5,6)
and ct.class_family not IN ('Premium')
and ct.class_group not like 'Test'
group by 1,2,3,4,5,6,7)

union

--NF
with ata2 as (
    select j.scheduled_at, d.id, d.status,
    (CASE when date_diff('minute', j.scheduled_at , d.arrived_at) < 0 THEN 0 --arrived at
    ELSE date_diff('minute', j.scheduled_at , d.arrived_at) end) AS ata


    FROM delivery.public.deliveries AS d
    left join delivery.public.journeys AS j ON j.id = d.journey_id

    where d.env = 'RU'
    and date(j.scheduled_at) >= date'2020-08-01'
)
(SELECT date(j.scheduled_at) AS dates,
    tp.timecategory,
    tp.subperiod,
    tp.period,
    tp.subperiod2 AS time_period,
    'NF' as platform,
    /*(CASE WHEN (timecategory = '2.Dates') THEN date_diff('day', date(j.scheduled_at), current_date)
    WHEN ((timecategory = '3.Weeks') AND (day_of_week(date(j.scheduled_at)) = 0)) THEN (date_diff('week', date(j.scheduled_at), current_date) + 1)
    WHEN (timecategory = '3.Weeks') THEN max(date_diff('week', date(j.scheduled_at), current_date)) OVER (PARTITION BY subperiod2)
    WHEN (timecategory = '4.Months') THEN max(date_diff('month', date(j.scheduled_at), current_date)) OVER (PARTITION BY subperiod2)
    WHEN (timecategory = '5.Quarters') THEN max(date_diff('quarter', date(j.scheduled_at), current_date)) OVER (PARTITION BY subperiod2)
    WHEN (timecategory = '6.Years') THEN max(date_diff('year', date(j.scheduled_at), current_date)) OVER (PARTITION BY subperiod2) ELSE null END) AS period_diff, */
    'eCommerce' AS Client_type,
    avg(ata2.ata) as avg_ata,
    --ca.corporate_account_name AS company,

    -- CHECKED - is the same with delivery performance
    count(distinct CASE when d.status = 'completed' THEN d.id ELSE null end) AS completed_orders,
    count(distinct CASE when d.status IN ('completed', 'not_delivered')THEN d.id end ) AS completed_and_cancelled_orders,
    0 as net_orders,
    count(distinct d.id) AS gross_orders


FROM delivery.public.deliveries AS d
LEFT JOIN delivery.public.journeys AS j ON j.id = d.journey_id
LEFT JOIN delivery.public.cancellation_infos AS c ON d.id = c.cancellable_id and c.cancellable_type = 'deliveries'
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca ON cast(ca.source_id AS varchar(128)) = d.company_id and d.env = ca.country_symbol
LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.date_key = date(j.scheduled_at)
and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')

left join ata2 on j.scheduled_at = ata2.scheduled_at and d.id = ata2.id and d.status = ata2.status

WHERE d.env ='RU'
  and lower(ca.corporate_account_name) not like '%test%'
  and date(j.scheduled_at) >= date'2020-08-01'
  and tp.timecategory is not null
GROUP BY 1,2,3,4,5,6);

