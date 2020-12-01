(;
select
d.created_at ,
case when company_id in ('25399', '25140') then 'MVideo' end company,
cast(company_id as integer) company_id,

subperiod,
d.id,
date_diff('second', d.scheduled_at, d.cancelled_at)*1.00/60 cancell_time,
case when cancellation_stage is not null then cancellation_stage else 'completed' end cancell_stage,
cancelled_by,
(case when d.started_at<d.cancelled_at and cancellation_stage = 'cancelled' and cancelled_by = 'customer'
then 'On the way by customer' else 'other' end)
cancellattion_desc
--count(d.id) orders

from delivery."public".deliveries d
left join delivery."public".cancellation_infos ci on
d.id = ci.cancellable_id
 LEFT JOIN  emilia_gettdwh.periods_v AS tp ON
 tp.date_key = date(d.created_at) and tp.hour_key = 0
            and tp.timecategory IN ('3.Weeks')

where company_id in ('25399', '25140')
and date(d.created_at) >= date'2020-09-01';
--group by 1,2,3,4,5,6,7
)

union

(select 'Inventive' as company,
ordering_corporate_account_gk,
subperiod,
order_cancellation_stage_desc cancell_stage,
cancellation_type_desc cancelled_by,
case when cancellation_type_desc = 'Customer' and order_cancellation_stage_desc = 'On the way'
then 'On the way by customer' end cancellattion_desc,
count(order_gk) orders

from emilia_gettdwh.dwh_fact_orders_v fo
LEFT JOIN "emilia_gettdwh"."dwh_dim_cancellation_types_v" ct
ON "cancelled_by_type_key" = ct."cancellation_type_key"
LEFT JOIN "emilia_gettdwh"."dwh_dim_order_cancellation_stages_v" cs
ON cs."order_cancellation_stage_key" = fo."order_cancellation_stage_key"
LEFT JOIN  emilia_gettdwh.periods_v AS tp ON
 tp.date_key = fo.date_key and tp.hour_key = 0
            and tp.timecategory IN ('3.Weeks')


where ordering_corporate_account_gk in (20007748,200010173,200010174,200010175,200010176,200020229)
and lob_key = 5
and country_key = 2
and fo.date_key >= date'2020-09-01'
group by 1,2,3,4,5,6);

select * from delivery."public".deliveries d

--started at < cancelled_at -- on  the way cancellation stage 'cancell'
-- before dr assn started at > cancelled_at OR | started at is null and cancellation stage 'cancell'
-- after dr arrival cancellation_stage 'not delivered'
--to Yosy and Taya , Pasha , Katya


select d.*, j.*,
d.created_at ,
case when company_id in ('25399', '25140') then 'MVideo' end company,
cast(company_id as integer) company_id,
subperiod,
date_diff('second', d.created_at, d.cancelled_at)*1.00/60 cancell_time,
case when cancellation_stage is not null then cancellation_stage else 'completed' end cancell_stage,
cancelled_by,
(case when j.started_at<d.cancelled_at and cancellation_stage = 'cancelled' and cancelled_by = 'customer'
and d.arrived_at is null and supplier_id <> 0
then 'On the way by customer' else 'other' end)
cancellattion_desc
--count(d.id) orders

from delivery."public".deliveries d
left join delivery."public".cancellation_infos ci on
d.id = ci.cancellable_id
 LEFT JOIN  emilia_gettdwh.periods_v AS tp ON
 tp.date_key = date(d.created_at) and tp.hour_key = 0
            and tp.timecategory IN ('3.Weeks')
LEFT JOIN delivery.public.journeys AS j ON j.id = d.journey_id

where company_id in ('25399', '25140')
and date(d.created_at) >= date'2020-09-01'
--and d.started_at < d.cancelled_at
--and cancellation_stage = 'cancelled'
--and cancelled_by = 'customer'
--and subperiod = 'W41'
--and supplier_id <> 0

-- For Yosi
select d.*, j.*,
case when cancellation_stage is not null then cancellation_stage else 'completed' end cancell_stage,
cancelled_by,
(case when j.started_at<d.cancelled_at and cancellation_stage = 'cancelled' and cancelled_by = 'customer'
and d.arrived_at is null and supplier_id <> 0
then 'On the way by customer' else 'other' end)
cancellattion_desc


from delivery."public".deliveries d
left join delivery."public".cancellation_infos ci on
d.id = ci.cancellable_id
LEFT JOIN delivery.public.journeys AS j ON j.id = d.journey_id

where j.id in (186396,195738)
and d.status = 'cancelled'


select d.id, j.id,
d.status,
d."order_index", --delivery index (in system / fact delivery order) ???
d.created_at,  -- delivery creation time
json_extract_scalar(d."pickup", '$.address') as pickup_address,
j.scheduled_at, -- delivery schedule time
d."started_at", -- equal to delivery creation time
d."cancelled_at",
j.started_at,  -- driver accepted the offer / manually assigned by CC / FO order switched to ASAP (20 min before scheduled_At)
d."arrived_at", --courier arrived to pick up
d."picked_up_at", -- delivery marked as picked up
d."in_route_from", --???
d."arrived_to_drop_off_at", -- arrived to drop off (receiver)
json_extract_scalar(d."drop_off", '$.address') as address_address,
"dropped_off_at", -- courier delivered order to the receiver
d.ended_at, -- delivery changed its status to final (completed/cancelled/not_delivered/rejected)
j.ended_at, --journey changed its status to final (completed/cancelled/rejected)
ci.cancelled_by


from "delivery"."public"."deliveries" d
left join "delivery"."public"."journeys" j on d.journey_id = j.id
left join delivery."public".cancellation_infos ci on
d.id = ci.cancellable_id


where 1=1
and d.status = 'not_delivered'
and d.cancelled_at is not null
--and d.cancelled_at > picked_up_at
and d.env = 'RU'
--and date(d.created_at) >= date'2020-06-01'
--and d.cancelled_at > picked_up_at
--133021 108128 not delivered
-- cancell after pick up at 117827, 78491




select company_id,
     date(d."cancelled_at") as date_key,
 j."legacy_order_id",
 d.id delivery_id,
 j.id journey_id,
 d.order_id_representation,
 d."created_at",
 d."cancelled_at",
 j.started_at,
 j."supplier_id",
 coalesce (d."arrived_at", d.picked_up_at) as arrived_at_pickup,
(case when supplier_id = 0 or supplier_id is null or  j.started_at > d.cancelled_at  then 'before driver assignment'
 when coalesce (d."arrived_at", d.picked_up_at) is null then 'on the way'
 when coalesce (d."arrived_at", d.picked_up_at) is not null then 'after driver arrival'
 else 'error' end) as cancellation_stage,
 count(d.id) over (partition by j.legacy_order_id) as deliveries_in_a_journey

 from "delivery"."public"."deliveries" d
left join "delivery"."public"."journeys" j on d.journey_id = j.id

where 1=1
--and company_id in ('25576', '25540')  -- X5 companies
--and date(d."cancelled_at") = date'2020-10-21'
and d.status in ('cancelled', 'not_delivered')
and d.env = 'RU'
and j.id in (195738,182599,196561)



select company_id,
     date(d."cancelled_at") as date_key,
 j."legacy_order_id", d.id delivery_id,
 j.id,
 d.order_id_representation,
 d."created_at",
 d."cancelled_at",
 j.started_at,
 j."supplier_id",
 coalesce (d."arrived_at", d.picked_up_at) as arrived_at_pickup,
(case when supplier_id = 0 or supplier_id is null
                            or  j.started_at > d.cancelled_at
                            or jh.created_at < d.cancelled_at then 'before driver assignment'
    when coalesce (d."arrived_at", d.picked_up_at) is null then 'on the way'
    when coalesce (d."arrived_at", d.picked_up_at) is not null then 'after driver arrival'
    else 'error' end) as cancellation_stage,
 count(d.id) over (partition by j.legacy_order_id) as deliveries_in_a_journey

 from "delivery"."public"."deliveries" d
left join "delivery"."public"."journeys" j on d.journey_id = j.id
left join "delivery"."public".journey_history jh on jh.journey_id = j.id and "action" = 'courier unassigned'


where 1=1
--and company_id in ('25576', '25540')  -- X5 companies
--and date(d."cancelled_at") = date'2020-10-21'
--and d.status in ('cancelled', 'not_delivered')
and d.env = 'RU'
and j.id in (195738,196561); --182599


--- NEW GOOD
with AR AS (
    SELECT
     date_key,
     driver_gk,
     SUM(  CASE WHEN fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) AS numerator,
                (SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL THEN 1 ELSE 0 END) - SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL AND fof.Is_Withdrawned=1 AND fof.Driver_Response_Key<>1 THEN 1 ELSE 0 END) )  AS denominator
    FROM emilia_gettdwh.dwh_fact_offers_v fof
     LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v cl ON cl.class_type_key = fof.class_type_key
    WHERE lob_key IN (5,6)
    and date_key >= date'2020-08-01'
    GROUP BY 1,2
),

GH AS (
    SELECT driver_gk,
    date_key,
    sum(CASE
                                      when fdh.driver_status_key IN (2, 4, 5, 6)
                                              THEN fdh.minutes_in_status
                                      ELSE 0 end)/60.0 AS gh
    FROM emilia_gettdwh.dwh_fact_drivers_hourly_v fdh
     WHERE   1 = 1
                and fdh.date_key >= date'2020-08-01'
                and fdh.country_key = 2
                and driver_gk IN (SELECT cast(driver_gk AS bigint) FROM "sheets"."default".driver_promo_3500)
                GROUP BY 1,2
),

-- fleet_commission AS ( SELECT order_id,
-- cast(json_extract_scalar(metadata, '$.amount_inc_tax') AS decimal(10,2)) AS fleet_commission_inc_Vat
-- FROM "driverearnings"."public"."transactions_v"
--  WHERE env = 'ru'
-- and transaction_type_id = 71 -- fleet commission
-- and "created_at" >= date '2020-08-31'
-- ),

driver_promo AS
(
    SELECT dp.driver_gk,
    vendor_name,
    dd."fleet_gk",
    min(dp.from_date_key) AS from_date_key
    FROM "sheets"."default".driver_promo_3500 dp
    LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" dd ON dd."driver_gk" = cast(dp.driver_gk AS bigint)
    LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v v ON v.vendor_gk = dd.fleet_gk

    GROUP BY 1,2,3
)


SELECT
            dp.driver_gk,
            dd.driver_name,
            v."vendor_gk",
            v.vendor_name,
            gh.date_key,
            ar.numerator*1.000/nullif(ar.denominator,0) AS AR,
            gh,
            count(distinct CASE when order_status_key = 7 THEN order_gk ELSE null end) AS completed_orders,
            sum("driver_total_cost") AS driver_total_cost,
             sum("driver_total_commission_inc_vat") AS driver_total_comission_inc_vat
           --sum(fc.fleet_commission_inc_Vat) AS fleet_commission_inc_vat
     FROM driver_promo dp
     LEFT JOIN gh ON gh.driver_gk = cast(dp.driver_gk as bigint) and  gh."date_key" >= cast (from_date_key AS date)
    LEFT JOIN AR ON ar.driver_gk = cast(dp.driver_gk as bigint) and ar.date_key = gh.date_key
    left join emilia_gettdwh.dwh_fact_orders_v fo ON cast(dp.driver_gk AS bigint) = fo.driver_gk  and fo.date_key = gh.date_key
            and fo.country_key = 2
            and fo.class_type_key not IN (2000642, 2000886, 2000957, 20001129, 20001260, 20001286) --exclude routes
            and fo.lob_key IN (5,6)
            and fo.driver_total_cost > 0
    --LEFT JOIN fleet_commission fc ON fc.order_id = fo.sourceid
            LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v cl ON cl.class_type_key = fo.class_type_key
    LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" dd ON cast(dp.driver_gk as bigint) = dd.driver_gk
        LEFT JOIN "emilia_gettdwh"."dwh_dim_vendors_v" v ON dd."fleet_gk" = v."vendor_gk"

    WHERE   1 = 1

GROUP BY 1,2,3,4,5,6,7;


(;
select
'NF',
d.company_id,
date(d.created_at) as date_key,
 --j."legacy_order_id",
 j.id,
 d.order_id_representation,
 d."cancelled_at",

(case when d.status in ('cancelled', 'not_delivered')
            and ci.cancelled_by = 'customer'
            then
(case when supplier_id = 0 or supplier_id is null or
                j.started_at > d.cancelled_at or jh.created_at < d.cancelled_at
                then 'before driver assignment'
 when coalesce (d."arrived_at", d.picked_up_at) is null then 'on the way'
 when coalesce (d."arrived_at", d.picked_up_at) is not null then 'after driver arrival'
 else 'error' end) end)as cancellation_stage,

 --count(d.id) over (partition by j.id) as deliveries_in_a_journey,
 count(d.id) deliveries_gross,
 count(distinct
            case when d.status in ('cancelled', 'not_delivered')
            and ci.cancelled_by = 'customer'
            then d.id end) deliveries_cancelled


 from "delivery"."public"."deliveries" d
left join "delivery"."public"."journeys" j on d.journey_id = j.id
left join "delivery"."public".journey_history jh
    on jh.journey_id = j.id and "action" = 'courier unassigned'
left join delivery."public".cancellation_infos ci on
    d.id = ci.cancellable_id and ci.cancellable_type = 'deliveries'

where d.company_id in ('25576', '25540')  -- X5 companies
and date(d.created_at) >= date'2020-10-25'
and d.env = 'RU'

group by 1,2,3,4,5,6,7;
)

union







-- GT DOSSIER Cancellations
select *,
case when cancellation_time <= 5 then '1. 1-5'
when  cancellation_time >5 and cancellation_time <=10 then '2. 6-10'
when  cancellation_time >10 and cancellation_time <=20 then '3. 11-20'
when  cancellation_time >20 and cancellation_time <=30 then '4. 21-30'
when  cancellation_time >30 and cancellation_time <=40 then '5. 31-40'
when  cancellation_time >40 and cancellation_time <=50 then '6. 41-50'
when  cancellation_time >50 then '7. 51+' end cancellation_time_group
from
(select
'GT' platform,
loc.city_name,
fo.date_key,
tp.timecategory,
tp.subperiod,
tp.period,
tp.subperiod2 AS time_period,
--ride_cancelled_datetime cancelled_at,
ca.corporate_account_name,
accounts.name_internal,
ca.corporate_account_gk,
ride_cancelled_datetime,
st.order_status_desc,
order_gk,
(case when fo.order_status_key = 4 and cancellation_type_desc = 'Customer' then order_cancellation_stage_desc end) cancellation_stage,
(case when fo.order_status_key = 4 then date_diff('second', order_create_datetime, ride_cancelled_datetime)*1.000/60 end) cancellation_time

from emilia_gettdwh.dwh_fact_orders_v fo
-- cancellation info
LEFT JOIN "emilia_gettdwh"."dwh_dim_cancellation_types_v" ct
    ON "cancelled_by_type_key" = ct."cancellation_type_key"
LEFT JOIN "emilia_gettdwh"."dwh_dim_order_cancellation_stages_v" cs
    ON cs."order_cancellation_stage_key" = fo."order_cancellation_stage_key"
left join emilia_gettdwh.dwh_dim_order_statuses_v st on st.order_status_key = fo.order_status_key
-- time
LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.hour_key = 0 and tp.date_key = fo.date_key
       and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
-- company name
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
          ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key
LEFT JOIN "emilia_gettdwh"."dwh_dim_account_managers_v" am ON am."account_manager_gk" = ca."account_manager_gk"
LEFT JOIN sheets."default".delivery_corp_accounts_20191203 AS accounts
    ON cast(accounts.company_gk AS bigint)=fo.ordering_corporate_account_gk
-- region
LEFT JOIN emilia_gettdwh.dwh_dim_locations_v AS loc
      ON loc.location_key = fo.origin_location_key

where fo.lob_key in (5,6)
and fo.country_key = 2
and fo.date_key between current_date - interval '90' day and current_date
and ct.class_family not IN ('Premium')
and ct.class_group not like 'Test'
and ordering_corporate_account_gk <> 20004730
);

-- EDP
select *,
case when cancellation_time <= 5 then '1. 1-5'
when  cancellation_time >5 and cancellation_time <=10 then '2. 6-10'
when  cancellation_time >10 and cancellation_time <=20 then '3. 11-20'
when  cancellation_time >20 and cancellation_time <=30 then '4. 21-30'
when  cancellation_time >30 and cancellation_time <=40 then '5. 31-40'
when  cancellation_time >40 and cancellation_time <=50 then '6. 41-50'
when  cancellation_time >50 then '7. 51+' end cancellation_time_group

--select cancellation_stage, count(distinct delivery_id)
from (

select
'EDP' platform,
loc.city_name,
date(d.created_at) date_key,
tp.timecategory,
tp.subperiod,
tp.period,
tp.subperiod2 AS time_period,
ca.corporate_account_name,
company_id,
d.cancelled_at,
 d.status delivery_status,
 d.id delivery_id,
 j.id journey_id,

 --j.started_at,
 supplier_id,
(case when c.cancelled_by = 'customer' and d.status = 'cancelled' then
    (case when supplier_id = 0 or supplier_id is null
                                or  j.started_at > d.cancelled_at
                                or jh.created_at < d.cancelled_at
                                    and (supplier_id = 0 or supplier_id is null)
                                then 'before driver assignment'
        when coalesce (d."arrived_at", d.picked_up_at) is null then 'on the way'
        when coalesce (d."arrived_at", d.picked_up_at) is not null then 'after driver arrival'
        else 'error' end)
end) as cancellation_stage,
--count(distinct d.id) gross_deliveries,
(case when d.status = 'cancelled' and c.cancelled_by = 'customer'
then date_diff('second', d.created_at, d.cancelled_at)/60.00 end) as cancellation_time


from "delivery"."public"."deliveries" d
left join "delivery"."public"."journeys" j on d.journey_id = j.id
-- cancellation info
left join "delivery"."public".journey_history jh on jh.journey_id = j.id and "action" = 'courier unassigned'
left join delivery.public.cancellation_infos c on d.id = c.cancellable_id and cancellable_type = 'deliveries'
-- company name
left join "emilia_gettdwh"."dwh_dim_corporate_accounts_v" ca
    ON cast(ca.source_id AS varchar) = d.company_id and ca.country_symbol = 'RU'
-- location
LEFT JOIN emilia_gettdwh.dwh_fact_orders_v AS fo ON j.legacy_order_id = fo.sourceid
    and fo.country_key = 2 and lob_key = 5 and year(fo.date_key) > 2019
    and fo.date_key between current_date - interval '90' day and current_date
left join emilia_gettdwh.dwh_dim_locations_v loc on fo.origin_location_key = loc.location_key
-- time
LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.date_key = date(j.created_at) and tp.hour_key = 0
and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')

where 1=1
-- for detailed info
--and company_id in ('25576', '25540')  -- X5 companies
--and company_id = '25140' --MVideo
--and date(d.created_at) >= date'2020-11-10'

and date(d.created_at) between current_date - interval '90' day and current_date
--and d.status IN ('not_delivered', 'completed', 'cancelled', 'rejected')
and d.env = 'RU'
--and d.id = 1921856
)
--group by 1


--- MODEL DELIVERY
select *,
case when cancellation_time <= 5 then '1. 1-5'
when  cancellation_time >5 and cancellation_time <=10 then '2. 6-10'
when  cancellation_time >10 and cancellation_time <=20 then '3. 11-20'
when  cancellation_time >20 and cancellation_time <=30 then '4. 21-30'
when  cancellation_time >30 and cancellation_time <=40 then '5. 31-40'
when  cancellation_time >40 and cancellation_time <=50 then '6. 41-50'
when  cancellation_time >50 then '7. 51+' end cancellation_time_group

--select cancellation_stage, count(distinct delivery_id)
from
(
select
'EDP' platform,
loc.city_name,
date(del."created_at") as date_key,
tp.timecategory,
tp.subperiod,
tp.period,
tp.subperiod2 AS time_period,
 company_gk,
 ca.corporate_account_name,
 --d.order_id_representation,

st.delivery_status_desc,
del.delivery_gk,
 del.journey_gk,
 --del."created_at",
 del."cancelled_at",
 jor.started_at,
 jor.courier_gk,
 coalesce (del."arrived_at", del.picked_up_at) as arrived_at_pickup,

 (case when st.delivery_status_desc = 'cancelled' and c.cancelled_by = 'customer' then
    (case when jor.courier_gk = -1 or jor.courier_gk is null
                            or  jor.started_at > del."cancelled_at"
                            or jh.created_at < del."cancelled_at"
                                and (jor.courier_gk = -1 or jor.courier_gk is null)
                            then 'before driver assignment'
    when coalesce (del."arrived_at", del.picked_up_at) is null then 'on the way'
    when coalesce (del."arrived_at", del.picked_up_at) is not null then 'after driver arrival'
    else 'error' end) end) as cancellation_stage,

(case when st.delivery_status_desc = 'cancelled' and c.cancelled_by = 'customer'
then date_diff('second', del.created_at, del.cancelled_at)/60.00 end) as cancellation_time


from model_delivery.dwh_fact_deliveries_v del
left join model_delivery.dwh_fact_journeys_v jor on jor.journey_gk = del.journey_gk
-- cancell info
left join "delivery"."public".journey_history jh on jh.journey_id = jor.journey_id
						and "action" = 'courier unassigned'
left join delivery.public.cancellation_infos c on del.source_id = c.cancellable_id
						and cancellable_type = 'deliveries'
-- company name
left join "emilia_gettdwh"."dwh_dim_corporate_accounts_v" ca ON ca.corporate_account_gk = del.company_gk
					and ca.country_symbol = 'RU'
left join model_delivery.dwh_dim_delivery_statuses_v st on del.delivery_status_id = st.delivery_status_id
-- city
left join emilia_gettdwh.dwh_dim_locations_v loc on del.pickup_location_key = loc.location_key
-- time
LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.date_key = date(del.created_at) and tp.hour_key = 0
and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')

where 1=1
and del.country_symbol = 'RU'
--and del.spurce_id  = 20001899593
and date(del.created_at) between current_date - interval '90' day and current_date
--and delivery_gk = 20001921856
)









