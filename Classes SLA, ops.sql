select
'OF' platform,
 subperiod2,
 timecategory,
 fo.class_type_key,
 ct."class_type_desc",
 fo.ordering_corporate_account_gk,
    ca.corporate_account_name,
    accounts.name_internal,
    loc.city_name,


count(distinct fo.order_gk) journeys,
count(distinct fo.order_gk) deliveries,

sum(driver_total_cost + driver_total_commission_exc_vat) as sum_payment,
count(case when driver_total_cost > 0 then driver_total_cost end) as count_payment,

sum(est_distance)*1.00 ride_dist_sum,
count(est_distance) as ride_dist_count,

sum(date_diff('second', order_confirmed_datetime, driver_arrived_datetime))*1.00/60.00 ATA_min_sum,
(count(date_diff('second', order_confirmed_datetime, driver_arrived_datetime))) as ATA_count,

sum(fo.m_driver_wait_time)*1.00 / 60.00 wait_time_sum,
count(case when fo.m_driver_wait_time >0 then fo.m_driver_wait_time end) as wait_time_count,

sum(fo.m_ride_duration)*1.00/60.00 ride_duration_min_sum,
count(case when fo.m_ride_duration >0 then fo.m_ride_duration end) as ride_duration_count,

SUM(CASE WHEN fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) AS numerator,

(SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL THEN 1 ELSE 0 END)
- SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL AND fof.Is_Withdrawned=1
AND fof.Driver_Response_Key<>1 THEN 1 ELSE 0 END) )  AS denominator


from "emilia_gettdwh"."dwh_fact_orders_v" fo
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key
left join emilia_gettdwh.dwh_fact_offers_v fof
    on fo.order_gk = fof.order_gk
    and fof.country_key  = 2
    and fof.date_key >= date '2020-09-01'
LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.date_key = fo.date_key and tp.hour_key = 0
  and tp.timecategory IN ('3.Weeks', '4.Months')
LEFT JOIN sheets."default".delivery_corp_accounts_20191203 AS accounts
            ON cast(accounts.company_gk AS bigint)=fo.ordering_corporate_account_gk
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
            ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
left join emilia_gettdwh.dwh_dim_locations_v loc on fo.origin_location_key = loc.location_key

WHERE 1 =1
 and class_group not like 'Test'
 and driver_arrived_datetime is not null
 and date_diff('second', order_confirmed_datetime, driver_arrived_datetime) > 0
 and ct.class_family not IN ('Premium')
and fo.Country_Key = 2
and fo.lob_key IN (5,6)
and fo.order_datetime >= date '2020-09-01'

GROUP BY 1,2,3,4,5,6,7,8,9;



-- nf
select
'NF' platform,
subperiod2,
timecategory,
fo.class_type_key,
ct."class_type_desc",
ca.corporate_account_name company_name,
fo.ordering_corporate_account_gk,
loc.city_name,
fo.date_key,
count(distinct fo.order_gk) as journeys,
count( distinct case when fo.order_status_key = 7 then fo.order_gk end ) as completed_journeys,
sum(j.deliveries) as deliiveries,
sum(j.completed_deliveries) as completed_deliveries,
sum(driver_total_cost + driver_total_commission_exc_vat) as sum_payment,

count(case when driver_total_cost > 0 then driver_total_cost end) as count_payment,

sum(est_distance) ride_dist_sum,
count( case when est_distance >0 then est_distance end) as ride_dist_count,

sum(date_diff('second', j.started_at, j.arrived_at))*1.00 / 60.00 ATA_min_sum,
count(case when date_diff('second', j.started_at, j.arrived_at) > 0 then 1 end) as ATA_count,

1.00 * (sum(date_diff('second',  j.arrived_at, j.picked_up_at)))/60.00 waiting_min_sum,
count(case when date_diff('second', j.arrived_at,j.picked_up_at) >0 then 1 end) as waiting_count,

1.00 * (sum(date_diff('second',  j.in_route_from, j.ended_at)))/60.00 ride_duration_sum,
count(case when date_diff('second', j.in_route_from, j.ended_at) > 0 then 1 end) as ride_duration_count

-- SUM(CASE WHEN fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) AS numerator,
--
-- (SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL THEN 1 ELSE 0 END)
-- - SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL AND fof.Is_Withdrawned=1
-- AND fof.Driver_Response_Key<>1 THEN 1 ELSE 0 END) )  AS denominator



from
        (
            select
            company_id,
            journey_id, j.legacy_order_id,
            j.started_at,
            j.ended_at,
            min(d.arrived_at) arrived_at,
            min(d.picked_up_at) picked_up_at,
            min(d.in_route_from) in_route_from,
            count(d.id) deliveries,
            count(case when d.status = 'completed' then d.id end) as completed_deliveries

            from delivery.public.deliveries d
            left join delivery.public.journeys j on j.id = d.journey_id


            where j.env = 'RU'
            and date(d.created_at)  >= current_date - interval '3' month
            --and type = 'default' -- exclude returns
            and date(d.created_at) = date'2021-3-4'
            and company_id = '25140'
            group by 1,2,3,4,5
            order by journey_id

        ) j

join  "emilia_gettdwh"."dwh_fact_orders_v" fo ON j.legacy_order_id = fo.sourceid and fo.country_key = 2
    and fo.lob_key = 5
    and fo.date_key >= current_date - interval '3' month
left join emilia_gettdwh.dwh_dim_locations_v loc on fo.origin_location_key = loc.location_key
left join emilia_gettdwh.dwh_fact_offers_v fof
    on fo.order_gk = fof.order_gk
    and fof.country_key  = 2
    and fof.date_key >= current_date - interval '3' month
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
    ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.date_key = fo.date_key and tp.hour_key = 0
  and tp.timecategory IN ('2.Dates','3.Weeks', '4.Months')


WHERE 1 =1
and class_group not like 'Test'
and fo.lob_key IN (5)
and fo.date_key >= current_date - interval '3' month
and fo.ordering_corporate_account_gk <> 20004730
and fo.sourceid is not null -- only NF
and ct.class_family = 'Premium'
and class_type_desc like '%ondemand%'
--and date_diff('second', j.started_at, d.arrived_at) > 0
and corporate_account_name not like '%test%'
and fo.date_key > date'2021-3-3'
and ca.corporate_account_name like '%Мвидео%'
and tp.timecategory = '2.Dates'

GROUP BY 1,2,3,4,5,6,7,8,9;


select
fof.class_type_key,
ct."class_type_desc",
count(distinct offer_gk) offers,
(count(distinct CASE WHEN fof.Driver_Response_Key=1 THEN offer_gk END)*1.00 /

nullif((count(distinct CASE WHEN fof.Delivered_Datetime IS NOT NULL THEN offer_gk END)
- count(distinct CASE WHEN fof.Delivered_Datetime IS NOT NULL AND fof.Is_Withdrawned=1
AND fof.Driver_Response_Key<>1 THEN offer_gk END) ),0)) *100  AS AR

from emilia_gettdwh.dwh_fact_offers_v fof

JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fof.class_type_key
        and ct.lob_key in (5,6)


where fof.country_key = 2
and date_key >= date'2020-12-01'
and fof.origin_order_location_key in (245,246,354)
and class_group not like 'Test'
group by 1,2


select
subperiod2,
timecategory,
fo.class_type_key,
class_type_group_desc,
loc.city_name,

sum(driver_total_cost + driver_total_commission_exc_vat) as sum_payment,
count(case when driver_total_cost > 0 then driver_total_cost end) as count_payment,

sum(ride_distance_key)*1.00 ride_dist_sum,
count(ride_distance_key) as ride_dist_count,

sum(case when date_diff('second', order_confirmed_datetime, driver_arrived_datetime) > 0 then
 date_diff('second', order_confirmed_datetime, driver_arrived_datetime) end)*1.00/60.00 ATA_min_sum,
(count(case when date_diff('second',order_confirmed_datetime, driver_arrived_datetime)>0 then 1 end)) as ATA_count,

sum(case when fo.m_driver_wait_time >0 then fo.m_driver_wait_time end)*1.00 / 60.00 wait_time_sum,
count(case when fo.m_driver_wait_time >0 then fo.m_driver_wait_time end) as wait_time_count,

sum(case when fo.m_ride_duration > 0 then fo.m_ride_duration end)*1.00/60.00 ride_duration_min_sum,
count(case when fo.m_ride_duration >0 then fo.m_ride_duration end) as ride_duration_count


from "emilia_gettdwh"."dwh_fact_orders_v" fo
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key
LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.date_key = fo.date_key and tp.hour_key = 0
  and tp.timecategory IN ('3.Weeks', '4.Months')
left join emilia_gettdwh.dwh_dim_locations_v loc on fo.origin_location_key = loc.location_key

WHERE 1 =1
 and class_group not like 'Test'
 and driver_arrived_datetime is not null
and date_diff('second', order_confirmed_datetime, driver_arrived_datetime) > 0
 and ct.class_type_group_desc in ('Economy', 'Premium')
and fo.Country_Key = 2
and fo.lob_key not IN (5,6)
and fo.order_datetime >= date '2020-09-01'

GROUP BY 1,2,3,4,5

select * from emilia_gettdwh.dwh_dim_class_types_v
where lob_key not in (5,6)
and country_key = 2

select * from desc model_delivery.dwh_fact_deliveries_v