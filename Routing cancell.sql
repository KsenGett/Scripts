with main as (
SELECT
    fo.date_key,
    tp.timecategory,
    tp.subperiod,
    tp.period,
    tp.subperiod2 AS time_period,
    "city_name",

    'OF' platform,
    -- client type
    (case when am.name like '%Delivery%' or ca.account_manager_gk IN (100079, 100096, 100090, 100073, 100088)
           THEN 'eCommerce' ELSE 'Corporate' end ) AS client_type,

    fo.ordering_corporate_account_gk company_gk,
    ca.corporate_account_name corp_name,
    intn.name_internal company_name,

--fo.order_status_key,
cs.order_cancellation_stage_desc,
ct.cancellation_type_desc,
--date_diff('second',fo.order_create_datetime, fo.order_confirmed_datetime)*1.000/60 routing_time,
--date_diff('second',"order_create_datetime", "ride_cancelled_datetime" )*1.000/60 AS cancellation_time,
count(case when "cancellation_type_desc" = 'Customer' and order_status_key = 4
    and date_diff('second',fo.order_create_datetime, fo.ride_cancelled_datetime )*1.000/60 <= 5 then (fo.order_gk) end)
cancelled_5min,

count(fo."order_gk") gross_orders,
count(distinct CASE when (fo.order_status_key = 7 or (fo.order_status_key = 4 and driver_total_cost > 0))
THEN fo.order_gk ELSE null end) CAA_orders,
count(case when fo.order_status_key = 4 and driver_total_cost > 0 then fo.order_gk end) nd_orders,
count(case when fo.order_status_key = 4 then fo.order_gk end) cancelled_orders,
count(case when fo.order_status_key = 7 then fo.order_gk end) completed_orders

FROM "emilia_gettdwh"."dwh_fact_orders_v" fo
    --left join emilia_gettdwh.dwh_dim_order_statuses_v st on fo.order_status_key = st.order_status_key
    -- city
    LEFT JOIN "emilia_gettdwh"."dwh_dim_locations_v" l ON fo."origin_location_key" = l."location_key"
    -- cancellation stage
    LEFT JOIN "emilia_gettdwh"."dwh_dim_order_cancellation_stages_v" cs ON cs."order_cancellation_stage_key" = fo."order_cancellation_stage_key"
    -- accounts
    LEFT JOIN hive.emilia_gettdwh.dwh_dim_corporate_accounts_v ca ON ca.corporate_account_gk=fo.ordering_corporate_account_gk
    LEFT JOIN sheets."default".delivery_corp_accounts_20191203
        AS intn ON cast(intn.company_gk AS bigint)=fo.ordering_corporate_account_gk
    -- time
    LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.hour_key = fo.hour_key and tp.date_key = fo.date_key
       and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
    -- account manager
    LEFT JOIN "emilia_gettdwh"."dwh_dim_account_managers_v" am ON am."account_manager_gk" = ca."account_manager_gk"
    -- class
    left join emilia_gettdwh.dwh_dim_class_types_v dc on
     fo.class_type_key = dc.class_type_key
    --cancellations
    LEFT JOIN "emilia_gettdwh"."dwh_dim_cancellation_types_v" ct ON "cancelled_by_type_key" = ct."cancellation_type_key"

WHERE fo.country_key = 2
and fo.lob_key = 5
and fo.date_key >= date '2020-08-01'
--and dc.class_family <> 'Premium'
and dc.class_group not like 'Test'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13

union
-- NF
select
date(fd.created_at) date_key,
    tp.timecategory,
    tp.subperiod,
    tp.period,
    tp.subperiod2 AS time_period,
    l.city_name,
    'NF' platform,
    'eCommerce' client_type,

    fd.company_gk,
    ca.corporate_account_name corp_name,
    ca.corporate_account_name company_name,
--fd.delivery_status_id,
order_cancellation_stage_desc,
"cancellation_type_desc",
count(case when fd.delivery_status_id=2 and
    date_diff('second', fd.created_at, fd.cancelled_at )*1.000/60 <= 5 then delivery_gk end) cancelled_5min,
count(fd.delivery_gk) gross_orders,
count(case when delivery_status_id in (4,7) then delivery_gk end) CAA_orders, --completed and not delivered
count(case when delivery_status_id = 7 then delivery_gk end) nd_orders, --not-delivered
count(case when delivery_status_id = 2 then delivery_gk end) cancelled_orders,
count(case when delivery_status_id = 4 then delivery_gk end) completed_orders

from "model_delivery"."dwh_fact_deliveries_v" fd
    LEFT JOIN model_delivery.dwh_fact_journeys_v j ON fd.journey_gk = j.journey_gk
    and j.country_symbol ='RU' and date(j.created_at) >= date'2020-08-01'
    left join emilia_gettdwh.dwh_dim_class_types_v dc on j.class_type_key = dc.class_type_key
    LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca ON ca.corporate_account_gk = fd.company_gk
                and ca.country_symbol = 'RU'
    LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.date_key = date(fd.scheduled_at) and tp.hour_key = 0
            and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
    LEFT JOIN "emilia_gettdwh"."dwh_dim_locations_v" l ON fd.pickup_location_key = l."location_key"
    -- cancellations
    left join "emilia_gettdwh"."dwh_fact_orders_v" fo on fd.order_gk = fo.order_gk
    and fo.lob_key = 5 and fo.date_key >= date'2020-08-01' and fo.country_key =2
    LEFT JOIN "emilia_gettdwh"."dwh_dim_order_cancellation_stages_v" cs
    ON cs."order_cancellation_stage_key" = fo."order_cancellation_stage_key"
    LEFT JOIN "emilia_gettdwh"."dwh_dim_cancellation_types_v" ct ON "cancelled_by_type_key" = ct."cancellation_type_key"

where fd.country_symbol ='RU'
and fd.company_gk  not in (200017459, 20004730)
and dc.class_type_desc like '%ondemand%'
and date(fd.created_at) >= date'2020-08-01'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13

)

(select platform, subperiod, corp_name, company_gk,
"cancellation_type_desc",
order_cancellation_stage_desc,

sum(gross_orders) gross_orders,
sum(cancelled_orders) cncl_orders_sum
--sum(CAA_orders) sum_CAA,
--sum(nd_orders) sum_nd

from main
where company_gk in (200025399, 200025140)
and timecategory = '3.Weeks'
group by 1,2,3,4,5,6);


--for GUY
select
'NF' as platform, ordering_corporate_account_gk,
order_cancellation_stage_desc, cancellation_type_desc,
count(order_gk) orders

from emilia_gettdwh.dwh_fact_orders_v fo
LEFT JOIN "emilia_gettdwh"."dwh_dim_cancellation_types_v" ct
ON "cancelled_by_type_key" = ct."cancellation_type_key"
LEFT JOIN "emilia_gettdwh"."dwh_dim_order_cancellation_stages_v" cs
ON cs."order_cancellation_stage_key" = fo."order_cancellation_stage_key"

where fo.ordering_corporate_account_gk in (200025399, 200025140) --MVideo
and lob_key in (5,6)
and fo.date_key >= date'2020-10-01'
group by 1,2,3,4


select * from delivery."public".deliveries
where status = 'cancelled'limit 2
--871398929 legacy 640532 delivery request 612847 id

select *
from delivery."public".cancellation_infos
where cancellable_id = 612847






SELECT
    fo.date_key,
    tp.timecategory,
    tp.subperiod,
    tp.period,
    tp.subperiod2 AS time_period,
    "city_name",

    'NF' platform,
    -- client type
    (case when am.name like '%Delivery%' or ca.account_manager_gk IN (100079, 100096, 100090, 100073, 100088)
           THEN 'eCommerce' ELSE 'Corporate' end ) AS client_type,

    fo.ordering_corporate_account_gk company_gk,
    ca.corporate_account_name corp_name,
    intn.name_internal company_name,

fo.order_status_key,
cs.order_cancellation_stage_desc,
ct.cancellation_type_desc,
--date_diff('second',"order_create_datetime", "ride_cancelled_datetime" )*1.000/60 AS cancellation_time,
count(case when "cancellation_type_desc" = 'Customer' and date_diff('second',fo.order_create_datetime, fo.ride_cancelled_datetime )*1.000/60 <= 5 then (fo.order_gk) end)
cancelled_5min,

count(case when dc.class_type_desc like '%ondemand%' then delivery_gk end) gross_orders,
count(case when dc.class_type_desc like '%ondemand%' and delivery_status_id in (4,7) then delivery_gk end) CAA_orders, --completed and not delivered
count(case when dc.class_type_desc like '%ondemand%' and  delivery_status_id = 7 then delivery_gk end) nd_orders, --not-delivered
count(case when dc.class_type_desc like '%ondemand%' and delivery_status_id = 2 then delivery_gk end) cancelled_orders,
count(case when dc.class_type_desc like '%ondemand%' and delivery_status_id = 4 then delivery_gk end) completed_orders


FROM "emilia_gettdwh"."dwh_fact_orders_v" fo
    -- city
    LEFT JOIN "emilia_gettdwh"."dwh_dim_locations_v" l ON fo."origin_location_key" = l."location_key"
    -- cancellation stage
    LEFT JOIN "emilia_gettdwh"."dwh_dim_order_cancellation_stages_v" cs ON cs."order_cancellation_stage_key" = fo."order_cancellation_stage_key"
    -- accounts
    LEFT JOIN hive.emilia_gettdwh.dwh_dim_corporate_accounts_v ca ON ca.corporate_account_gk=fo.ordering_corporate_account_gk
    LEFT JOIN sheets."default".delivery_corp_accounts_20191203
        AS intn ON cast(intn.company_gk AS bigint)=fo.ordering_corporate_account_gk
    -- time
    LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.hour_key = fo.hour_key and tp.date_key = fo.date_key
       and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
    -- account manager
    LEFT JOIN "emilia_gettdwh"."dwh_dim_account_managers_v" am ON am."account_manager_gk" = ca."account_manager_gk"
    -- class
    left join emilia_gettdwh.dwh_dim_class_types_v dc on
     fo.class_type_key = dc.class_type_key
    --cancellations
    LEFT JOIN "emilia_gettdwh"."dwh_dim_cancellation_types_v" ct ON "cancelled_by_type_key" = ct."cancellation_type_key"
    --
    left join "model_delivery"."dwh_fact_deliveries_v" fd on fd.order_gk = fo.order_gk
    and fd.country_symbol ='RU'

WHERE fo.country_key = 2
and fo.lob_key = 5
and fo.date_key >= date '2020-08-01'
and dc.class_family = 'Premium'
and dc.class_group not like 'Test'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14)

(select platform, order_status_key,
order_cancellation_stage_desc,
"cancellation_type_desc",
sum(cancelled_5min) m5_cncl,
sum(cancelled_orders) cncl_orders_sum,
sum(CAA_orders) sum_CAA,
sum(nd_orders) sum_nd

from main
--where "cancellation_type_desc" = 'Customer'

group by 1,2,3,4);



-- OF: cancelled 5- are the part of CAA (where cancelled ana payment)
--- NF cancelled 5- are the part only of cancelled status (not not delivered)


--katya save
with fof as (
SELECT distinct order_gk,
avg("offer_screen_eta")*1.00/60 as offer_screen_eta_min
from "emilia_gettdwh"."dwh_fact_offers_v"
where date_key >= date '2020-08-01'
and country_key = 2
and "ordering_corporate_account_gk" = 200022121
and order_status_key = 4
and "driver_response_key" = 1 --driver_accepted_the_offer
group by 1
)

SELECT fo.date_key,
 "city_name",
 fo.ordering_corporate_account_gk,
 ca.corporate_account_name,
 fo."origin_full_address",
 fo."origin_latitude",
 fo."origin_longitude",
 order_cancellation_stage_desc,
 date_diff('second',"order_create_datetime", "ride_cancelled_datetime" )*1.000/60 AS cancellation_time,
 fo."m_routing_duration"*1.00/60 as m_routing_duration_min,
 fof.offer_screen_eta_min,
 fo."order_gk"
FROM "emilia_gettdwh"."dwh_fact_orders_v" fo
left join fof on fo."order_gk" = fof.order_gk
LEFT JOIN "emilia_gettdwh"."dwh_dim_locations_v" l ON "origin_location_key" = "location_key"
LEFT JOIN "emilia_gettdwh"."dwh_dim_order_cancellation_stages_v" cs ON cs."order_cancellation_stage_key" = fo."order_cancellation_stage_key"
LEFT JOIN "emilia_gettdwh"."dwh_dim_cancellation_types_v" ct ON "cancelled_by_type_key" = ct."cancellation_type_key"
LEFT JOIN hive.emilia_gettdwh.dwh_dim_corporate_accounts_v ca ON ca.corporate_account_gk=fo.ordering_corporate_account_gk
WHERE fo.country_key = 2
and fo.order_status_key = 4
and fo."ordering_corporate_account_gk" = 200022121
--and "order_cancellation_stage_desc" = 'On The Way'
and "cancellation_type_desc" = 'Customer'
--and "origin_location_key" = 245 --MSK only
and fo.date_key >= date '2020-08-01';



-- NF to save
select
date(fd.created_at) date_key,
    tp.timecategory,
    tp.subperiod,
    tp.period,
    tp.subperiod2 AS time_period,
    l.city_name,
    'NF' platform,
    'eCommerce' client_type,

    fd.company_gk,
    ca.corporate_account_name corp_name,
    ca.corporate_account_name company_name,
fd.delivery_status_id,
order_cancellation_stage_desc,
"cancellation_type_desc",
count(case when date_diff('second', fd.created_at, fd.cancelled_at )*1.000/60 <= 5 then delivery_gk end) cancelled_5min,
count(fo.order_gk) gross_orders,
count(case when delivery_status_id in (4,7) then delivery_gk end) CAA_orders, --completed and not delivered
count(case when delivery_status_id = 7 then delivery_gk end) nd_orders, --not-delivered
count(case when delivery_status_id = 2 then delivery_gk end) cancelled_orders,
count(case when delivery_status_id = 4 then delivery_gk end) completed_orders

from "model_delivery"."dwh_fact_deliveries_v" fd
    LEFT JOIN model_delivery.dwh_fact_journeys_v j ON fd.journey_gk = j.journey_gk
    and j.country_symbol ='RU' and date(j.created_at) >= date'2020-08-01'
    left join emilia_gettdwh.dwh_dim_class_types_v dc on j.class_type_key = dc.class_type_key
    LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca ON ca.corporate_account_gk = fd.company_gk
                and ca.country_symbol = 'RU'
    LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.date_key = date(fd.scheduled_at) and tp.hour_key = 0
            and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
    LEFT JOIN "emilia_gettdwh"."dwh_dim_locations_v" l ON fd.pickup_location_key = l."location_key"
    -- cancellations
    left join "emilia_gettdwh"."dwh_fact_orders_v" fo on fd.order_gk = fo.order_gk
    and fo.lob_key = 5 and fo.date_key >= date'2020-08-01'
    LEFT JOIN "emilia_gettdwh"."dwh_dim_order_cancellation_stages_v" cs
    ON cs."order_cancellation_stage_key" = fo."order_cancellation_stage_key"
    LEFT JOIN "emilia_gettdwh"."dwh_dim_cancellation_types_v" ct ON "cancelled_by_type_key" = ct."cancellation_type_key"

where fd.country_symbol ='RU'
and fd.company_gk  not in (200017459, 20004730)
and dc.class_type_desc like '%ondemand%'
and date(fd.created_at) >= date'2020-08-01'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14




-- cancellations new
select company_id,
     date(d."cancelled_at") as date_key,
 j."legacy_order_id",
 j.id,
 d.order_id_representation,
 d."created_at",
 d."cancelled_at",
 j.started_at,
 j."supplier_id",
 coalesce (d."arrived_at", d.picked_up_at) as arrived_at_pickup,
(case when supplier_id = 0 or supplier_id is null or  j.started_at > d.cancelled_at
        or jh.created_at < d.cancelled_at then 'before driver assignment'
 when coalesce (d."arrived_at", d.picked_up_at) is null then 'on the way'
 when coalesce (d."arrived_at", d.picked_up_at) is not null then 'after driver arrival'
 else 'error' end) as cancellation_stage,
 count(d.id) over (partition by j.legacy_order_id) as deliveries_in_a_journey


from "delivery"."public"."deliveries" d
left join "delivery"."public"."journeys" j on d.journey_id = j.id
left join "delivery"."public".journey_history jh on jh.journey_id = j.id and "action" = 'courier unassigned'

where company_id in ('25576', '25540')  -- X5 companies
and date(d."cancelled_at") = date'2020-10-21'
and d.status in ('cancelled', 'not_delivered')
and d.env = 'RU';

