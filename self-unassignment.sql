-- select count(distinct journey_id)
-- from (

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
    tp.timecategory, tp.subperiod, tp.period, tp.subperiod2 AS time_period,
    dl.city_name,
    fd.company_gk, ca.corporate_account_name,
    fj.courier_gk final_courier, -- it indicates whether a courier was found or not
    fj.journey_id,
    fj.journey_status_id, -- 3 cancelled, 4 completed, 6 rejected
    fj.number_of_completed_deliveries,
    (date_diff('second', fj.created_at, fj.scheduled_at))*1.00/60 >= 20 is_future_order,

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
final_courier, journey_status_id, number_of_completed_deliveries, journey_id,
is_future_order, unassigned_driver as driver_id, assignment_time, unassignment_type,  unas_time,
-- unas. time based on unassignment from journey history
sum(case when assignment_time <= unas_time  then date_diff('second', assignment_time, unas_time)/60.00 end)
unas_time_sum,
count(case when assignment_time <= unas_time and date_diff('second', assignment_time, unas_time) is not null then 1 end) unas_time_count

from main
--where journey_id = 819169
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)
-- )
-- where time_period like '%W52%'
-- warnings:
-- 1. Time difference between time in the events and journey history. Like journey id = 541530

--Scripts for check:
-- events
select event_at, event_date,
    cast(json_extract_scalar(from_utf8("payload"), '$.distinct_id') as bigint) driver_id,
    try(cast(json_extract_scalar(from_utf8("payload"), '$.journey_id') as bigint)) journey_id

    from "events"
    where "event_name" in ('courier|journey_details_screen|cancel_order_popup|positive|button_clicked')
    and event_date >= date'2020-10-01'
    and try(cast(json_extract_scalar(from_utf8("payload"), '$.journey_id') as bigint)) = 541530
    and env = 'RU';

-- to count number of app unassignment
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

