-- select
--        date_key, driver_id,
--        count(distinct case when unassignment_type='CC' then journey_id end ) cc_journ,
--        sum(cc_unas),
--        count(distinct cc_jorn) cc_j, count(distinct app_jorn) app_j
-- from (
/*
 Owner - Ksenia Kozlova
Cube Name - Leads_all_sources_1.11.20
ID - 0488D98E11EB608647D50080EF75357F
*/

/*
 Owner - Ksenia Kozlova
Cube Name - Leads_all_sources_1.11.20
ID - 0488D98E11EB608647D50080EF75357F
*/

with main as(

    with self_unas as
    (
        select event_at + interval '3' hour event_at,
        cast(json_extract_scalar(from_utf8("payload"), '$.distinct_id') as bigint) driver_id,
               event_name,
        try(cast(json_extract_scalar(from_utf8("payload"), '$.journey_id') as bigint)) journey_id

        from "events"
        where "event_name" in ('courier|journey_details_screen|cancel_order_popup|positive|button_clicked', --Android
                               'courier|journey_details_screen|cancel|button_clicked') -- iOS
        and event_date >= date'2020-10-01'
        and env = 'RU'
    )

    (
    select
    date(fj.scheduled_at) date_key,
    tp.timecategory, tp.subperiod, tp.period, tp.subperiod2 AS time_period,
    dl.city_name,
    fd.company_gk, ca.corporate_account_name,
    fj.courier_gk final_courier, -- it indicates whether a courier was found or not
    fj.journey_id,
    fj.journey_status_id, -- 3 cancelled, 4 completed, 6 rejected
    fj.number_of_completed_deliveries,
    (date_diff('second', fj.created_at, fj.scheduled_at))*1.00/60 >= 20 is_future_order,
    jh.offer_gk, jh.order_gk, jh.is_auto_accept, jh.offer_screen_eta, jh.matching_driving_distance,
    jh.unassigned_driver,
    (case when jh.unas_user <> 'system@gett.com' then 'CC' -- check CC via journey history
        when sa.event_at is not null then 'app' -- check event of self unassignment via events
        -- other corner cases with system unassignment
        else 'no self-unassignment' end) unassignment_type,

    jh.assigned assignment_time,
    (case when jh.unas_user <> 'system@gett.com' then jh.unassigned -- when CC
                -- when not cc but event exists, takes from journey history
                when sa.event_at is not null then jh.unassigned end) unas_time

    -- info about journey - company, status
    from model_delivery.dwh_fact_journeys_v fj
    left join model_delivery.dwh_dim_journey_statuses_v st
                on st.journey_status_id = fj.journey_status_id
    left join  model_delivery.dwh_fact_deliveries_v fd
                on fj.journey_gk = fd.journey_gk
                and fd.country_symbol = 'RU' and fd.requested_schedule_time >= date'2020-10-01'
    LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
                ON ca.corporate_account_gk = fd.company_gk
                and ca.country_symbol = 'RU'
    --info about unassignment
    left join
        (
            select
            jh.journey_id,
            jh.driver_id assigned_driver,
            fof.is_auto_accept, fof.offer_gk, fof.order_gk,
            fof.offer_screen_eta, fof.matching_driving_distance,
            jh.created_at assigned,
            jh.user as_user,
            jhu.driver_id unassigned_driver,
            jhu.created_at unassigned,
            jhu.user unas_user

        from
            (
                select
                journey_id, cast(substring(description, 14) as integer) driver_id,
                created_at, "user",
                rank() over (partition by journey_id, cast(substring(description, 14) as integer) order by created_at) as action_order

                from "delivery"."public".journey_history
                where "action" = 'courier assigned'
                and date(created_at) between current_date - interval '90' day and current_date
                ---and journey_id = 819169
            ) jh

        left join
            (
                select
                journey_id, cast(substring(description, 14) as integer) driver_id,
                created_at, "user",
                rank() over (partition by journey_id, cast(substring(description, 14) as integer) order by created_at) as action_order

                from "delivery"."public".journey_history
                where "action" = 'courier unassigned'
                and date(created_at) between current_date - interval '90' day and current_date
                --and journey_id = 819169
            ) as jhu on jhu.journey_id = jh.journey_id
                and jh.driver_id = jhu.driver_id
                and jh.action_order = jhu.action_order

        left join
            (
            select
            journey_id, fof.offer_gk, fof.order_gk, fof.offer_screen_eta, fof.matching_driving_distance,
            cast(substring(cast(driver_gk as varchar), 5) as integer)  driver_id,
            is_auto_accept,
            rank() over (partition by journey_id, driver_gk order by created_at) action_order

            from model_delivery.dwh_fact_journeys_v fj
            left join emilia_gettdwh.dwh_fact_offers_v fof
                    on fj.order_gk = fof.order_gk
                    and fof.country_key = 2
                    and fof.date_key between current_date - interval '90' day and current_date
                    and fof.offer_screen_eta is not null

            where fj.country_symbol = 'RU'
            ) fof on jh.journey_id = fof.journey_id
                and jh.driver_id = fof.driver_id
                and fof.action_order = jh.action_order

        ) as jh on fj.journey_id = jh.journey_id

        left join self_unas sa
                on jh.unassigned_driver = sa.driver_id
                and jh.journey_id = sa.journey_id
                --and date(jh.created_at) = sa.event_date

    -- city
    left join emilia_gettdwh.dwh_dim_locations_v dl on fj.pickup_location_key = dl.location_key
    -- time
    LEFT JOIN  data_vis.periods_v AS tp
            ON tp.date_key = date(fj.scheduled_at)
            and tp.hour_key = 0
            and tp.timecategory IN ('2.Dates', '3.Weeks')
            and tp.timecategory is not null

    where 1=1
    and date(fj.scheduled_at) between current_date - interval '90' day and current_date
    and fj.country_symbol = 'RU'
    --and fj.pickup_location_key = 245 -- check Moscow
    --and date(fj.scheduled_at) between date'2020-09-28' and date'2020-10-4' -- week 40 check
    --and fj.journey_id = 819169 -- check journey with many unassignments
    )

)
(select
date_key, timecategory, subperiod, "period", time_period,
city_name, company_gk, corporate_account_name,
final_courier, journey_status_id, number_of_completed_deliveries,
journey_id, offer_gk, order_gk, is_auto_accept, offer_screen_eta, matching_driving_distance,
is_future_order, unassigned_driver as driver_id,
assignment_time, unassignment_type,  unas_time,

-- kostyl
   case when unassignment_type ='CC' then journey_id end cc_jorn,
   case when unassignment_type ='app' then journey_id end app_jorn,

-- for correct work of dossier to count cases of 'unassignment' as well as 'no-unasssignment' per a driver
coalesce(cast(unassigned_driver as varchar), substring(cast(final_courier as varchar), 5)) driver_id_2,
d.phone,
-- unas. time based on unassignment from journey history
sum(case when assignment_time <= unas_time  then date_diff('second', assignment_time, unas_time)/60.00 end)
unas_time_sum,
count(case when assignment_time <= unas_time and date_diff('second', assignment_time, unas_time) is not null then 1 end) unas_time_count,
count(distinct case when unassignment_type ='CC' then 1 end) cc_unas,
   count(distinct case when unassignment_type ='app' then 1 end) app_unas


from main
left join emilia_gettdwh.dwh_dim_drivers_v d on
    coalesce(cast(unassigned_driver as varchar), substring(cast(final_courier as varchar), 5)) = cast(d.source_id as varchar)
    and d.country_key = 2 and is_courier = 1
where true
--and unassigned_driver = 1000971
--and  journey_id = 819169
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
)
-- )
-- where date_key between date'2021-03-1' and date'2021-03-30'
--     and timecategory = '2.Dates'
-- --and driver_id_2 = '742237'
-- group by 1,2

-- warnings:
-- 1. Time difference between time in the events and journey history. Like journey id = 541530




--Scripts for check:

-- 1. events
select event_at, event_date,
    cast(json_extract_scalar(from_utf8("payload"), '$.distinct_id') as bigint) driver_id,
    try(cast(json_extract_scalar(from_utf8("payload"), '$.journey_id') as bigint)) journey_id

    from "events"
    where "event_name" in ('courier|journey_details_screen|cancel_order_popup|positive|button_clicked')
    and event_date >= date'2020-10-01'
    and try(cast(json_extract_scalar(from_utf8("payload"), '$.journey_id') as bigint)) = 541530
    and env = 'RU';


-- 2.to count number of app unassignment
with events as (
  select event_at, event_date,
    cast(json_extract_scalar(from_utf8("payload"), '$.distinct_id') as bigint) driver_id,
    try(cast(json_extract_scalar(from_utf8("payload"), '$.journey_id') as bigint)) journey_id

    from "events"
    where "event_name" = 'courier|journey_details_screen|cancel_order_popup|positive|button_clicked'
    and event_date between date'2020-10-26' and date'2020-11-1'
    and env = 'RU'
)
select count(distinct events.journey_id)
from events
left join model_delivery.dwh_fact_journeys_v fj on fj.journey_id = events.journey_id
where pickup_location_key = 245;

-- research
with main as(

    with self_unas as
    (
        select event_at, event_date,
        cast(json_extract_scalar(from_utf8("payload"), '$.distinct_id') as bigint) driver_id,
        try(cast(json_extract_scalar(from_utf8("payload"), '$.journey_id') as bigint)) journey_id

        from "events"
        where "event_name" = 'courier|journey_details_screen|cancel_order_popup|positive|button_clicked'
        and event_date >= date'2020-10-01'
        and env = 'RU'
    )

    (
    select
    date(fj.scheduled_at) date_key,
    fj.scheduled_at journey_timestemp,
    hour(fj.scheduled_at) hour_journey,
    dl.city_name,
    fd.company_gk,
    ca.corporate_account_name,
    fj.courier_gk final_courier, -- it indicates whether a courier was found or not
    fo.origin_latitude, fo.origin_longitude,
    fj.journey_id,
    fj.journey_status_id, -- 3 cancelled, 4 completed, 6 rejected
    fj.number_of_completed_deliveries,
    (date_diff('second', fj.created_at, fj.scheduled_at))*1.00/60 >= 20 is_future_order,
    jh.offer_gk,
    jh.order_gk,
    coalesce (jh.is_auto_accept,(case when fof.driver_gk = fj.courier_gk then fof.is_auto_accept end)) is_auto_accept,
    coalesce(jh.offer_screen_eta, (case when fof.driver_gk = fj.courier_gk then fof.offer_screen_eta end)) offer_screen_eta,
    (CASE when date_diff('second', fj.scheduled_at , fj.picked_up_at)*1.00/60 > 0 THEN
    date_diff('second', fj.scheduled_at , fj.picked_up_at) end)*1.00/60 AS ata,
    jh.matching_driving_distance,
    jh.unassigned_driver,
    (case when jh.unas_user <> 'system@gett.com' then 'CC' -- check CC via journey history
        when sa.event_at is not null then 'app' -- check event of self unassignment via events
        -- other corner cases with system unassignment
        else 'no self-unassignment' end) unassignment_type,

    jh.assigned assignment_time,
    (case when jh.unas_user <> 'system@gett.com' then jh.unassigned -- when CC
                -- when not cc but event exists, takes from journey history
                when sa.event_at is not null then jh.unassigned end) unas_time

    -- info about journey - company, status
    from model_delivery.dwh_fact_journeys_v fj
    left join model_delivery.dwh_dim_journey_statuses_v st
                on st.journey_status_id = fj.journey_status_id
    left join  model_delivery.dwh_fact_deliveries_v fd
                on fj.journey_gk = fd.journey_gk
                and fd.country_symbol = 'RU' and fd.requested_schedule_time >= date'2020-10-01'
    LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
                ON ca.corporate_account_gk = fd.company_gk
                and ca.country_symbol = 'RU'
    left join
        (
        select order_gk, origin_latitude,origin_longitude
        from emilia_gettdwh.dwh_fact_orders_v
        where lob_key in (5,6) and country_key = 2
        and date_key between current_date - interval '90' day and current_date
        ) fo on fo.order_gk = fj.order_gk
    --info about unassignment
    left join
        (
            select
            jh.journey_id,
            jh.driver_id assigned_driver,
            fof.is_auto_accept, fof.offer_gk, fof.order_gk,
            fof.offer_screen_eta, fof.matching_driving_distance,
            jh.created_at assigned,
            jh.user as_user,
            jhu.driver_id unassigned_driver,
            jhu.created_at unassigned,
            jhu.user unas_user

        from
            (
                select
                journey_id, cast(substring(description, 14) as integer) driver_id,
                created_at, "user",
                rank() over (partition by journey_id, cast(substring(description, 14) as integer) order by created_at) as action_order

                from "delivery"."public".journey_history
                where "action" = 'courier assigned'
                and date(created_at) between current_date - interval '90' day and current_date
                ---and journey_id = 819169
            ) jh

        left join
            (
                select
                journey_id, cast(substring(description, 14) as integer) driver_id,
                created_at, "user",
                rank() over (partition by journey_id, cast(substring(description, 14) as integer) order by created_at) as action_order

                from "delivery"."public".journey_history
                where "action" = 'courier unassigned'
                and date(created_at) between current_date - interval '90' day and current_date
                --and journey_id = 819169
            ) as jhu on jhu.journey_id = jh.journey_id
                and jh.driver_id = jhu.driver_id
                and jh.action_order = jhu.action_order

        left join
            (
            select
            journey_id, fof.offer_gk, fof.order_gk, fof.offer_screen_eta, fof.matching_driving_distance,
            cast(substring(cast(driver_gk as varchar), 5) as integer)  driver_id,
            is_auto_accept,
            rank() over (partition by journey_id, driver_gk order by created_at) action_order

            from model_delivery.dwh_fact_journeys_v fj
            left join emilia_gettdwh.dwh_fact_offers_v fof
                    on fj.order_gk = fof.order_gk
                    and fof.country_key = 2
                    and fof.date_key between current_date - interval '90' day and current_date


            where fj.country_symbol = 'RU'
            ) fof on jh.journey_id = fof.journey_id
                and jh.driver_id = fof.driver_id
                and fof.action_order = jh.action_order

        ) as jh on fj.journey_id = jh.journey_id

        left join self_unas sa
                on jh.unassigned_driver = sa.driver_id
                and jh.journey_id = sa.journey_id
                --and date(jh.created_at) = sa.event_date

    -- city
    left join emilia_gettdwh.dwh_dim_locations_v dl on fj.pickup_location_key = dl.location_key
    left join emilia_gettdwh.dwh_fact_offers_v fof
                    on fj.order_gk = fof.order_gk
                    and fof.country_key = 2
                    and fof.date_key between current_date - interval '90' day and current_date
    where 1=1
    and date(fj.scheduled_at) between current_date - interval '90' day and current_date
    and fj.country_symbol = 'RU'
    --and fj.pickup_location_key = 245 -- check Moscow
    --and date(fj.scheduled_at) between date'2020-09-28' and date'2020-10-4' -- week 40 check
    --and fj.journey_id = 819169 -- check journey with many unassignments
    )

)
(select
date_key,
city_name, company_gk,
journey_timestemp,
hour_journey,
origin_latitude, origin_longitude,
corporate_account_name,
final_courier, journey_status_id, number_of_completed_deliveries,
journey_id, offer_gk, order_gk, is_auto_accept, offer_screen_eta, ata,
matching_driving_distance,
is_future_order, unassigned_driver as driver_id,
assignment_time, unassignment_type,  unas_time,
-- for correct work of dossier to count cases of 'unassignment' as well as 'no-unasssignment' per a driver
coalesce(cast(unassigned_driver as varchar), substring(cast(final_courier as varchar), 5)) driver_id_2,
-- unas. time based on unassignment from journey history
sum(case when assignment_time <= unas_time  then date_diff('second', assignment_time, unas_time)/60.00 end)
unas_time_sum,
count(case when assignment_time <= unas_time and date_diff('second', assignment_time, unas_time) is not null then 1 end) unas_time_count

from main
--where journey_id = 819169
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
order by journey_id
)

select *
from "delivery-courier-assignment".public.self_unassignments
where env = 'RU'
limit 5;



select count(id) from "delivery-courier-assignment"."public".self_unassignments
    where env = 'RU'
and driver_id = 957024
and unassigned_at >= date'2021-05-31'



 select
 journey_id,
 cast(substring(description, 14) as integer) driver_id,
 created_at assign
 "user" <> 'system@gett.com' and "user" is not null by_cc_tag
 from "delivery"."public".journey_history
 where "action" = 'courier assigned'
 and date(created_at) between current_date - interval '90' day and current_date


select distinct event_name
from events
where event_name like '%matching%'
and event_date = date'2021-06-01'