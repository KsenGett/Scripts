select
       fof.date_key, j.journey_id, fof.driver_gk, j.created_at, number_of_deliveries, number_of_completed_deliveries,
       fof.order_gk, offer_gk, is_received, is_withdrawned, fof.driver_response_key, driver_response_desc,
       offer_screen_eta, fb.order_id is not null was_fb, fo.ordering_corporate_account_gk, fo.origin_full_address,
       driver_total_cost, est_distance, est_duration,
       ct.class_family = 'Premium' is_nf,
       fof.class_type_key,
       class_type_desc,
       delivered_datetime, response_datetime,
       calc.price, calc.created_at,
       (case when fof.driver_response_key in (1,3) then date_diff('second', delivered_datetime, response_datetime) end) had_time

from emilia_gettdwh.dwh_fact_offers_v fof
left join emilia_gettdwh.dwh_fact_orders_v fo on fo.order_gk = fof.order_gk and fo.country_key=2
                                                     and fo.lob_key in (5,6)
                                                     and fo.date_key >= date'2021-03-01'
left join emilia_gettdwh.dwh_dim_driver_responses resp on resp.driver_response_key = fof.driver_response_key
left join emilia_gettdwh.dwh_dim_class_types_v ct on fof.class_type_key = ct.class_type_key
                                                    and ct.country_key = 2
left join model_delivery.dwh_fact_journeys_v j on fo.order_gk = j.order_gk
                                                    and j.country_symbol = 'RU'
                                                    and j.date_key >= date'2021-03-01'
left join
        (
            select *
            from
        (
            select
            created_at, journey_id, amount price, supplier_id,
            min(created_at) over (partition by journey_id,  supplier_id) craeted_at_min


            from "delivery-pricing"."public".transactions
            where side = 'supplier'
            and created_at >= date'2021-03-01'
            and env = 'RU'
        )
            where created_at = craeted_at_min
            ) as calc
                    on j.journey_id = calc.journey_id

left join
    (
    select cast(json_extract_scalar(from_utf8("payload"), '$.order_id') as bigint) as order_id
     from "events"

    where "event_name" = 'matching|upgrade_class_automatically'
    and event_date >= date'2021-03-01'
    and env = 'RU'
    ) fb on fo.sourceid = fb.order_id


where fof.country_key = 2
and fof.date_key >= date'2021-03-01'
and fo.lob_key in (5,6)
--and pof.journey_id is not null
and fof.origin_order_location_key = 245
order by fo.order_gk, fo.driver_gk


-- delivery pricing calculations
--     (select * from
--         (select *,
--         min(event_at) over (partition  by journey_id, driver) event_at_min
--         from
--         (
--             select
--                    event_at,
--             consumed_at,
--                 cast(json_extract_scalar(from_utf8("payload"), '$.journey_id')  as bigint) journey_id,
--                json_extract_scalar(from_utf8("payload"), '$.amount') price,
--                cast(json_extract(json_extract(json_extract(from_utf8("payload"), '$.data'), '$.supplier') , '$.id')as varchar) driver
--         --         json_extract(json_extract(json_extract(from_utf8("payload"), '$.result'), '$.context'), '$.side') side
--
--             from events
--             where "event_name" = 'delivery-pricing|calculations'
--             and event_date >= date'2021-03-01'
--             and cast(json_extract(json_extract(json_extract(from_utf8("payload"), '$.result'), '$.context'), '$.side') as varchar) = 'supplier'
--             and env = 'RU'
--         ))
--     where event_at = event_at_min) as pof on cast(concat('2000', pof.driver) as bigint) = fof.driver_gk
--     and j.journey_id = pof.journey_id


select
*, cast(json_extract_scalar(from_utf8("payload"), '$.journey_id')  as bigint) journey_id,
       json_extract_scalar(from_utf8("payload"), '$.amount') price,
       cast(json_extract(json_extract(json_extract(from_utf8("payload"), '$.data'), '$.supplier') , '$.id')as bigint) driver,
        json_extract(json_extract(json_extract(from_utf8("payload"), '$.result'), '$.context'), '$.side') side

from events
    where "event_name" = 'delivery-pricing|calculations'
    and event_date >= date'2021-03-01'
    and cast(json_extract(json_extract(json_extract(from_utf8("payload"), '$.result'), '$.context'), '$.side') as varchar) = 'supplier'
    and env = 'RU'
limit 2

;
with distinct_offers as (
select
 fof.date_key,
 fof.driver_gk,
 fof.order_id,
 count(distinct offer_gk) as offers,
 count(distinct fof.class_type_key) as classes ,
 count(distinct ct.class_group) as classes1,
 count(distinct case when fof.is_received = 0 then offer_gk else null end) as cc ,
 count(distinct case when fof.driver_response_key is not null then offer_gk else null end) as not_null_driver_responce,
 count(distinct case when fof.driver_response_key = 1 then offer_gk else null end) as accept_driver_responce,
 count(distinct case when fof.driver_response_key = 2 then offer_gk else null end) as ignored_driver_responce,
 count(distinct case when fof.driver_response_key = 3 then offer_gk else null end) as rejected_driver_responce
from emilia_gettdwh.dwh_fact_offers_v AS fof
 left JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
  ON ct.class_type_key = fof.class_type_key
WHERE ct.country_key = 2
 and ct.lob_key IN (5,6)
 and fof.country_key = 2
 and fof.date_key >= date '2021-05-01'
group by 1,2,3
)
select
 df.*,
 offers - cc as not_cc_offers
from distinct_offers df
where offers > 1
 AND classes > 1



select cast(json_extract_scalar(from_utf8("payload"), '$.order_id') as bigint) as order_id,

       payload

     from "events"


    where "event_name" = 'matching|after_create_offer'
    and event_date >= date'2020-08-20'
    and env = 'RU'
limit 2

 select *
            from
        (
            select
            created_at, journey_id, amount price, supplier_id,
            min(created_at) over (partition by journey_id,  supplier_id) craeted_at_min


            from "delivery-pricing"."public".transactions
            where side = 'supplier'
            and created_at >= date'2021-03-01'
            and env = 'RU'
            and journey_id = 3441749
        )
            where created_at = craeted_at_min