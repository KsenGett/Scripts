with main as(

with self_unas as (
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
    tp.timecategory,
    tp.subperiod,
    tp.period,
    tp.subperiod2 AS time_period,
dl.city_name,
fd.company_gk,
ca.corporate_account_name,
fj.courier_gk final_courier, -- it indicates whether a courier was found or not
fj.journey_status_id, -- 3 cancelled, 4 completed, 6 rejected
fj.number_of_completed_deliveries,
fj.journey_id ,
cast(substring(jh.description, 14) as integer) driver_id,
(case when jhu.user <> 'system@gett.com' then 'CC' -- check CC via journey history
    when sa.event_at is not null then 'app' -- check event of self unassignment via events
    -- other corner cases with system unassignment
    else 'no self-unassignment' end) unassignment_type,

sa.journey_id self_journey_id,
max(jh.created_at) assignment_time,
max(case when jhu.user <> 'system@gett.com' then jhu.created_at -- when CC
            -- when not cc, takes from events
            else sa.event_at end) unas_time,
max(case when jhu.user <> 'system@gett.com' then jhu.created_at -- when CC
            -- when not cc but event exists, takes from journey history
            when sa.event_at is not null then jhu.created_at end) unas_time2

-- info about journey - company, status
from model_delivery.dwh_fact_journeys_v fj
left join model_delivery.dwh_dim_journey_statuses_v st on st.journey_status_id = fj.journey_status_id
left join  model_delivery.dwh_fact_deliveries_v fd on fj.journey_gk = fd.journey_gk
    and fd.country_symbol = 'RU' and fd.requested_schedule_time >= date'2020-10-01'
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca ON ca.corporate_account_gk = fd.company_gk
            and ca.country_symbol = 'RU'
-- info about unassignment
    left join "delivery"."public".journey_history jh
        on jh.journey_id = fj.journey_id
        and jh."action" in ('courier assigned')
    left join "delivery"."public".journey_history jhu
        on jhu.journey_id = fj.journey_id
        and jhu."action" in ('courier unassigned')

        left join self_unas sa on jhu.journey_id = sa.journey_id
        and sa.driver_id = cast(substring(jhu.description, 14) as integer)
        and date(jhu.created_at) = sa.event_date


-- city
left join emilia_gettdwh.dwh_dim_locations_v dl on fj.pickup_location_key = dl.location_key
-- time
LEFT JOIN  data_vis.periods_v AS tp ON tp.date_key = date(fj.scheduled_at) and tp.hour_key = 0
        and tp.timecategory IN ('2.Dates', '3.Weeks') and tp.timecategory is not null

where 1=1

and date(fj.scheduled_at) >= date'2020-9-28'
and fj.country_symbol = 'RU'
and fj.pickup_location_key = 245 -- Moscow
and (date_diff('second', fd.created_at, fd.scheduled_at))*1.00/60 <= 20 -- exclude Future orders, does it work, Kate?
--and date(fj.scheduled_at) between date'2020-09-28' and date'2020-10-4' -- week 40 check
--and fj.journey_id = 908138 --check
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
order by fj.journey_id
)

)
(select
time_period,
city_name,
--journey_id, assignment_time, unas_time, -- check on the journey level

-- unas. time based on unassignment event time
sum(case when assignment_time < unas_time then date_diff('second', assignment_time, unas_time)/60.00 end)
/count(case when assignment_time < unas_time and date_diff('second', assignment_time, unas_time)is not null then 1 end) unas_time,

-- unas. time based on unassignment from journey history
sum(case when assignment_time < unas_time2 then date_diff('second', assignment_time, unas_time2)/60.00 end)
/count(case when assignment_time < unas_time2 and date_diff('second', assignment_time, unas_time2)is not null then 1 end) unas_time2,

count(distinct case when unassignment_type = 'CC' then journey_id end) cc_un_journeys,
count(distinct case when unassignment_type = 'app' then journey_id end) app_un_journeys,
count(distinct case when unassignment_type = 'app' then journey_id end)*1.00 / count(distinct journey_id) *100 perc_app,

count(distinct case when unassignment_type = 'app' and final_courier = -1 then journey_id end)*1.00
/ count(distinct journey_id) *100 perc_app_cancell,
count(distinct journey_id) gross_journeys

from main
where timecategory = '3.Weeks'
group by 1,2--,3,4
)

-- warnings
-- journey id is less then in events.-> from journeys history less  then from events ?
--- 541530


select * from model_delivery.dwh_dim_journey_statuses_v