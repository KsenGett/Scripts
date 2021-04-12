with cancell as (
        select date(created_at) date_key,
        concat('2000', substring(description, 14)) driver_gk,
        count(distinct journey_id) cancells

        from "delivery"."public".journey_history
        where "action" = 'courier unassigned'
        and date(created_at) >= date'2020-10-01'
        --and substring(description, 14) = '136047'
        group by 1,2
)
, GH AS (
    SELECT driver_gk, fdh.date_key,
    
    count(CASE when fdh.driver_status_key IN (2, 4, 5, 6) then date_key end) days,
    sum(CASE
    when fdh.driver_status_key IN (2, 4, 5, 6) THEN fdh.minutes_in_status ELSE 0 end)/60.0 AS gh,
    sum(case when fdh.driver_status_key IN (5, 6) THEN fdh.minutes_in_status ELSE 0 end)/60.0 gh_in_ride

    FROM emilia_gettdwh.dwh_fact_drivers_hourly_v fdh
     WHERE   1 = 1
                and fdh.date_key >= date'2020-10-01'
                and fdh.country_key = 2
    GROUP BY 1,2
    )
, AR AS (
    SELECT
     date_key,
     driver_gk,
     SUM(  CASE WHEN fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) AS numerator,
                (SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL THEN 1 ELSE 0 END)
                - SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL AND fof.Is_Withdrawned=1
                AND fof.Driver_Response_Key<>1 THEN 1 ELSE 0 END) )  AS denominator

    FROM emilia_gettdwh.dwh_fact_offers_v fof
     LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v cl ON cl.class_type_key = fof.class_type_key
    WHERE lob_key IN (5,6)
    and date_key >= date'2020-10-01'
    GROUP BY 1,2
    )

(select
gh.driver_gk,
gh.date_key,
c.cancells,
ar.numerator*1.000/nullif(ar.denominator,0) ar,
gh.gh gh_sum,
gh.gh_in_ride gh_in_ride_sum,
gh.gh_in_ride *1.00 / nullif(gh.gh, 0) perc_in_ride,
fo.deliveries


from GH gh
join cancell c on gh.driver_gk = cast(c.driver_gk as integer) and c.date_key = gh.date_key
left join AR ar on ar.driver_gk = gh.driver_gk and ar.date_key = gh.date_key

left join (
    select
    distinct driver_gk,
    date_key,
    count(distinct case when ct.class_family <> 'Premium' and  ordering_corporate_account_gk not in (20004730,200017459)
        then order_gk end) +
    count(distinct case when ordering_corporate_account_gk in (20004730,200017459) then order_gk end) deliveries

    from emilia_gettdwh.dwh_fact_orders_v fo
    LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v ct
        ON fo.class_type_key=ct.class_type_key
    where fo.lob_key in (5,6)
    and fo.origin_location_key in (245,246)
    and fo.country_key = 2
    and fo.date_key >= date'2020-10-22'
    --and ordering_company_account_gk <> 20004730
    group by 1,2
    ) fo on fo.driver_gk = gh.driver_gk and gh.date_key = fo.date_key

)


--- after test
select *
--(case when user <> 'system@gett.com' then date_diff('second', j.created_at, jh.created_at) end)/60.00 cancel_time_cc,

from "delivery"."public".journey_history jh

where "action" = 'courier unassigned'
and date(jh.created_at) >= date'2020-10-22'
and user <> 'system@gett.com'

--journey id 438665,437703 -- by system, 439002 438474 - by cc


select *
from  "delivery-courier-assignment"."public"."self_unassignments"

select count(distinct id)
from  desc "delivery"."public".journey_history
where date(created_at) >= date('2020-10-21')
and user <> 'system@gett.com'

select *
from model_delivery.dwh_fact_journeys_v
where journey_id in (438665,437703,439002,438474)

select
order_gk, offer_gk,
order_datetime, -- created order
created_datetime, -- created offer
driver_gk,
is_confirmed,
response_datetime,
is_went_to_cc_key,
driver_unassigned_datetime
from emilia_gettdwh.dwh_fact_offers_v
where order_gk in (20001433656220,10001372306922,20001433658966,20001433631799)

-- journey hist (journey id) - model delivery journeys (order gk) - fact offers response datetime (driver_gk, response datetime)


-- DNMK Couriers unassigned' by CC
select date(jh.created_at), count(distinct journey_id)
from desc "delivery"."public".journey_history jh
where "action" = 'courier unassigned'
and date(jh.created_at) >= date'2020-10-1'
and user <> 'system@gett.com';


-- DNMK GH, Orders, AR

with GH AS (
    SELECT driver_gk, fdh.date_key,

    sum(CASE
    when fdh.driver_status_key IN (2, 4, 5, 6) THEN fdh.minutes_in_status ELSE 0 end)/60.0 AS gh,
    sum(case when fdh.driver_status_key IN (5, 6) THEN fdh.minutes_in_status ELSE 0 end)/60.0 gh_in_ride

    FROM emilia_gettdwh.dwh_fact_drivers_hourly_v fdh
     WHERE   1 = 1
                and fdh.date_key >= date'2020-10-01'
                and fdh.country_key = 2
    GROUP BY 1,2
    )
, AR AS (
    SELECT
     date_key,
     driver_gk,
     SUM(  CASE WHEN fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) AS numerator,
                (SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL THEN 1 ELSE 0 END)
                - SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL AND fof.Is_Withdrawned=1
                AND fof.Driver_Response_Key<>1 THEN 1 ELSE 0 END) )  AS denominator

    FROM emilia_gettdwh.dwh_fact_offers_v fof
     LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v cl ON cl.class_type_key = fof.class_type_key
    WHERE lob_key IN (5,6)
    and date_key >= date'2020-10-01'
    GROUP BY 1,2
    ),
cancell as (
        select date(created_at) date_key,
        concat('2000', substring(description, 14)) driver_gk,
        count(distinct journey_id) cancells

        from "delivery"."public".journey_history
        where "action" = 'courier unassigned'
        and date(created_at) >= date'2020-10-01'
        --and substring(description, 14) = '136047'
        group by 1,2
)

(select
gh.driver_gk,
gh.date_key,
gh.gh gh_sum,
gh.gh_in_ride gh_in_ride_sum,
ar.numerator,
ar.denominator,
gh.gh_in_ride *1.00 / nullif(gh.gh, 0) perc_in_ride,
(case when fo.journeys is not null then fo.journeys end) +
(case when fo.deliveries is not null then fo.deliveries end) deliveries,
c.cancells


from GH gh
join AR ar on ar.driver_gk = gh.driver_gk and ar.date_key = gh.date_key

left join (
    select
    distinct driver_gk,
    date_key,
    count(distinct case when ct.class_family <> 'Premium' and  ordering_corporate_account_gk not in (20004730,200017459)
        then order_gk end) journeys,
    count(distinct case when ordering_corporate_account_gk in (20004730,200017459)
                                    then order_gk end) deliveries

    from emilia_gettdwh.dwh_fact_orders_v fo
    LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v ct
        ON fo.class_type_key=ct.class_type_key
    where fo.lob_key in (5,6)
    --and fo.origin_location_key in (245,246)
    and fo.country_key = 2
    and fo.date_key >= date'2020-10-1'
    --and ordering_company_account_gk <> 20004730
    group by 1,2
    ) fo on fo.driver_gk = gh.driver_gk and gh.date_key = fo.date_key

left join cancell c on c.date_key = gh.date_key  and cast(c.driver_gk as integer) = gh.driver_gk
);


select
date(del.scheduled_at) date_key,
(case when del.delivery_status_id = 2 and jh.journey_id is not null then
    (case when j.courier_gk = -1 or j.courier_gk is null
                            or  j.started_at > del."cancelled_at"
                            or jh.created_at < del."cancelled_at"
                                and (j.courier_gk = -1 or j.courier_gk is null)
                            then 'before driver assignment' end) end) as is_before_dr_assignment,
count(distinct delivery_gk) deliveries

from model_delivery.dwh_fact_deliveries_v del
left join model_delivery.dwh_fact_journeys_v j on del.journey_gk = j.journey_gk
left join "delivery"."public".journey_history jh on
        jh.journey_id = j.journey_id
    and "action" = 'courier unassigned'
    and date(jh.created_at) >= date'2020-10-01'

where 1=1

and del.country_symbol = 'RU'
and date(del.created_at) >= date'2020-10-01'
group by 1,2;


--- time of unasignment

with self_unas as (
    select event_at, event_date,
    cast(json_extract_scalar(from_utf8("payload"), '$.distinct_id') as bigint) driver_id,
    try(cast(json_extract_scalar(from_utf8("payload"), '$.journey_id') as bigint)) journey_id


    from "events"
    where "event_name" = 'courier|journey_details_screen|cancel_order_popup|positive|button_clicked'
    and event_date >= date'2021-03-10'
    and env = 'RU'
)
(
select
cast(substring(description, 14) as integer) driver_id,
jh.journey_id,
(case when "action" = 'courier assigned' then created_at end) assignment_time,
(case when "action" = 'courier assigned' then user end) assignment_user,
(case when "action" = 'courier unassigned' then created_at end) unassignment_time,
(case when "action" = 'courier unassigned' then user end) unassignment_user
,
sa.journey_id self_journey_id,
sa.event_at self_unas_time

from "delivery"."public".journey_history jh
left join self_unas sa on jh.journey_id = sa.journey_id and
cast(substring(jh.description, 14) as integer) = sa.driver_id and date(jh.created_at) = sa.event_date

where "action" in ('courier assigned', 'courier unassigned')
and date(created_at) >= date'2021-3-10'
)





select
week(j.scheduled_at) week,
count(distinct j.journey_gk) total_journeys,
count(distinct jh.journey_id) journeys_unas,
count(distinct jh.journey_id)*1.000/count(distinct j.journey_gk) unas_pers_total,

count(distinct case when user = 'system@gett.com' then jh.journey_id end) self_unas,
count(distinct case when user = 'system@gett.com' then jh.journey_id end)*1.00
/count(distinct j.journey_gk) self_unas_perc

from model_delivery.dwh_fact_journeys_v j
left join "delivery"."public".journey_history jh on jh.journey_id = j.journey_id
    and "action" = 'courier unassigned'
    and date(jh.created_at) >= date'2020-10-01'

where 1=1
and j.created_at >= date'2020-11-1'
group by 1
order by week;


with GH AS (
    SELECT driver_gk, fdh.date_key,

    sum(CASE
    when fdh.driver_status_key IN (2, 4, 5, 6) THEN fdh.minutes_in_status ELSE 0 end)/60.0 AS gh,
    sum(case when fdh.driver_status_key IN (5, 6) THEN fdh.minutes_in_status ELSE 0 end)/60.0 gh_in_ride

    FROM emilia_gettdwh.dwh_fact_drivers_hourly_v fdh
     WHERE   1 = 1
                and fdh.date_key >= date'2020-12-20'
                and fdh.country_key = 2
    GROUP BY 1,2
    )
, AR AS (
    SELECT
     date_key,
     driver_gk,
     SUM(  CASE WHEN fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) AS numerator,
                (SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL THEN 1 ELSE 0 END)
                - SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL AND fof.Is_Withdrawned=1
                AND fof.Driver_Response_Key<>1 THEN 1 ELSE 0 END) )  AS denominator

    FROM emilia_gettdwh.dwh_fact_offers_v fof
     LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v cl ON cl.class_type_key = fof.class_type_key
    WHERE lob_key IN (5,6)
    and date_key >= date'2020-12-20'
    GROUP BY 1,2
    )

select fo.date_key, fo.driver_gk, dd.phone, dd.driver_name,
        city_name,
        gh.gh, gh.gh_in_ride, ar.numerator, ar.denominator,
        count(distinct case when  ordering_corporate_account_gk = 20004730
        then order_gk end) orders_nf,
        count(distinct case when class_family <> 'Premium' and ordering_corporate_account_gk <> 20004730
        then order_gk end) orders_of

            from emilia_gettdwh.dwh_fact_orders_v fo
            left join emilia_gettdwh.dwh_dim_drivers_v dd on fo.driver_gk = dd.driver_gk
            left join emilia_gettdwh.dwh_dim_locations_v l on l.location_key = fo.origin_location_key
            LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key
            left join GH gh on gh.date_key = fo.date_key and gh.driver_gk = fo.driver_gk
            left join AR ar on ar.driver_gk = fo.driver_gk and ar.date_key = fo.date_key


             where 1=1
            and fo.date_key >= date'2020-12-20'
             and fo.origin_location_key in (245,246, 354)
             and fo.lob_key in (5,6)
            and fo.order_status_key = 7
            --and fo.driver_gk in ({drivers})
            and dd.is_frozen<>1
            group by 1,2,3,4,5,6,7,8,9;



-- check self unas tables
with self_unas as (
    select event_at, event_date,
    cast(json_extract_scalar(from_utf8("payload"), '$.distinct_id') as bigint) driver_id,
    try(cast(json_extract_scalar(from_utf8("payload"), '$.journey_id') as bigint)) journey_id


    from "events"
    where "event_name" = 'courier|journey_details_screen|cancel_order_popup|positive|button_clicked'
    and event_date >= date'2020-12-01'
    and env = 'RU'
)

select distinct substring(description, 14) as courier_id,
dd.fleet_gk,
v.vendor_name,
 jh.journey_id,
 case when user = 'system@gett.com' then 'self-unassignment' else 'CC' end as unassignment_jh,
jh.created_at unass_time,
( case when sa.journey_id  is not null then 'self-unassignment' -- was event
            --when user <> 'system@gett.com' then 'CC' else 'system' end self_an_event,
            end)  self_un_event,
event_at event_time,
 d."company_id",
 ca."corporate_account_name",
 date(jh.created_at) as date_key

 from "delivery"."public".journey_history jh
left join delivery."public"."deliveries" d on d."journey_id" = jh.journey_id and d.env = 'RU'
left join "emilia_gettdwh"."dwh_dim_corporate_accounts_v" ca on cast(ca."source_id" as varchar) = d."company_id" and  ca."country_symbol" = 'RU'
left join "emilia_gettdwh".dwh_dim_drivers_v dd on substring(description, 14) = cast(dd.source_id AS varchar) and dd.country_key = 2
left join "emilia_gettdwh".dwh_dim_vendors_v v on dd.fleet_gk = v.vendor_gk
left join self_unas sa on jh.journey_id = sa.journey_id and sa.driver_id = cast(substring(jh.description, 14) as integer)
where "action" = 'courier unassigned'
--and jh.journey_id in (746653, 819169,748294,918306,894202) -- маршруты с несколькими откреплениями через CC и фичу
and d.env = 'RU'
and jh.created_at >= date'2020-12-20'-- bag report
--and event_at < jh.created_at
order by journey_id, courier_id, jh.created_at

-- 747549