select

 month(fo.date_key) as months,
 fo.class_type_key,
 ct."class_type_desc",

 sum(driver_total_cost + driver_total_commission_exc_vat) as sum_payment,
 count(case when driver_total_cost > 0 then driver_total_cost end) as count_payment,

count(distinct order_gk) orders,
sum(ride_distance_key)*1.00/nullif(count(ride_distance_key),0) as avg_ride_dist,

(sum(date_diff('second', order_confirmed_datetime, driver_arrived_datetime))*1.00/
  nullif(count(date_diff('second',order_confirmed_datetime, driver_arrived_datetime)),0))/60.00 as avg_ATA_minutes,

(sum(fo.m_driver_wait_time)*1.00/nullif(count(case when fo.m_driver_wait_time >0 then fo.m_driver_wait_time end),0))/60.00 as avg_wait_time,
(sum(fo.m_ride_duration)*1.00/nullif(count(case when fo.m_ride_duration >0 then fo.m_ride_duration end),0))/60.00 as avg_ride_duration


from "emilia_gettdwh"."dwh_fact_orders_v" fo
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key
        and fo.Country_Key = 2
        and fo.lob_key IN (5,6)
        and fo.order_datetime >= date '2020-09-01'

WHERE 1 =1
 and class_group not like 'Test'
 and driver_arrived_datetime is not null
 and date_diff('second', order_confirmed_datetime, driver_arrived_datetime) > 0
 and ct.class_family not IN ('Premium')

GROUP BY 1,2,3;

-- nf
select
'NF' platform,
month(fo.order_datetime) as months,
fo.class_type_key,
ct."class_type_desc",

sum(driver_total_cost + driver_total_commission_exc_vat) as sum_payment,

count(case when driver_total_cost > 0 then driver_total_cost end) as count_payment,

count(distinct fo.sourceid) as journeys,
sum(d.deliveries) as deliiveries,

1.00 * sum(ride_distance_key)/nullif(count( case when ride_distance_key >0 then ride_distance_key end),0)
as avg_ride_dist,

(1.00 * (sum(date_diff('second', j.started_at, d.arrived_at)))
/ nullif(count(case when date_diff('second', j.started_at, d.arrived_at) > 0
    then 1 end),0))/60 as ATA,

(1.00 * (sum(date_diff('second',  d.arrived_at, d.picked_up_at)))
/ nullif(count(case when date_diff('second', d.arrived_at,d.picked_up_at) >0
    then 1 end),0))/60 as waiting,

(1.00 * (sum(date_diff('second',  d.in_route_from, j.ended_at)))
/ nullif(count(case when date_diff('second', d.in_route_from, j.ended_at) > 0
    then 1 end),0))/60 as ride_duration,

null TOD -- for OF only


from delivery.public.journeys j
left join
        (
            select
            journey_id,
            min(arrived_at) arrived_at,
            min(picked_up_at) picked_up_at,
            min(in_route_from) in_route_from,
            count(distinct id) deliveries

            from delivery.public.deliveries

            where env = 'RU'
            and date(created_at)  >= current_date - interval '4' month
            group by 1
            order by journey_id

        ) d on j.id = d.journey_id

left join  "emilia_gettdwh"."dwh_fact_orders_v" fo ON j.legacy_order_id = fo.sourceid and fo.country_key = 2
    and fo.lob_key IN (5,6)
    and fo.date_key >= date '2020-08-01'

LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key

WHERE 1 =1
and class_group not like 'Test'
and fo.lob_key IN (5,6)
and fo.date_key >= date'2020-9-1'
and fo.ordering_corporate_account_gk <> 20004730
and fo.sourceid is not null -- only NF
and ct.class_family = 'Premium'
and class_type_desc like '%ondemand%'

GROUP BY 1,2,3,4;