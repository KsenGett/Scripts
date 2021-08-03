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
            and journey_id = 2889781
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



-- version 2

select
       fof.date_key, j.journey_id, fof.driver_gk, j.created_at, number_of_deliveries, number_of_completed_deliveries,
       fof.order_gk, offer_gk, is_received, is_withdrawned, fof.driver_response_key, driver_response_desc,
       offer_screen_eta, fb.order_id is not null was_fb, fo.ordering_corporate_account_gk, fo.origin_full_address,
       driver_total_cost, est_distance, est_duration, fof.distance_from_order_on_creation,
       ct.class_family = 'Premium' is_nf,
       fof.class_type_key, area_desc,
       class_type_desc,
       delivered_datetime, response_datetime,
       calc.price,
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

join "emilia_gettdwh"."dwh_dim_areas_v" areas  on ST_Contains(ST_GeometryFromText(areas.borders)
            , ST_Point(fof.origin_order_longitude, fof.origin_order_latitude))
            and "area_desc" like '%Moscow delivery district%'
            and area_desc not like '%2%' and area_desc not like '%11.03%'
            and areas.country_key = 2
left join
        (
            select * from (
                  select event_at, journey_id, price, min(event_at) over (partition by journey_id) min_time
                  from (
                           select *,cast(json_extract_scalar(from_utf8("payload"), '$.journey_id') as bigint) journey_id,
                                  json_extract_scalar(from_utf8("payload"), '$.amount')                     price,
                                  event_at

                           from events
                           where "event_name" = 'delivery-pricing|calculations'
                             and event_date >= date '2021-03-01'
                             and json_extract_scalar(
                                         json_extract(json_extract(from_utf8("payload"), '$.data'), '$.supplier'),
                                         '$.id') =
                                 '0'
                             --and json_extract_scalar(from_utf8("payload"), '$.journey_id') = '2889783'
                             and cast(json_extract(
                                   json_extract(json_extract(from_utf8("payload"), '$.result'), '$.context'),
                                   '$.side') as varchar) = 'supplier'
                             and env = 'RU'
                             and event_date = date '2021-06-15'
                            and json_extract_scalar(from_utf8("payload"), '$.journey_id') = '3716560'
                       )
              )
        where event_at = min_time
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
and ct.class_family = 'Premium'
--and j.order_gk = 20001674201142
--and pof.journey_id is not null
and fof.origin_order_location_key = 245
order by fo.order_gk, fo.driver_gk


select *
            from
        (
            select *
--             created_at, journey_id, amount price, supplier_id,
--             min(created_at) over (partition by journey_id,  supplier_id) craeted_at_min


            from show tables from "delivery-pricing"."public".calculations
            where True--side = 'supplier'

                --and created_at >= date'2021-03-01'
            --and env = 'RU'
        )
            where True
            and journey_id = 2889781


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

select * from (
                  select event_at, journey_id, price, min(event_at) over (partition by journey_id) min_time
                  from (
                           select cast(json_extract_scalar(from_utf8("payload"), '$.journey_id') as bigint) journey_id,
                                  json_extract_scalar(from_utf8("payload"), '$.amount')                     price,
                                  event_at

                           from events
                           where "event_name" = 'delivery-pricing|calculations'
                             and event_date >= date '2021-03-01'
                             and json_extract_scalar(
                                         json_extract(json_extract(from_utf8("payload"), '$.data'), '$.supplier'),
                                         '$.id') =
                                 '0'
                             and json_extract_scalar(from_utf8("payload"), '$.journey_id') = '2889783'
                             and cast(json_extract(
                                   json_extract(json_extract(from_utf8("payload"), '$.result'), '$.context'),
                                   '$.side') as varchar) = 'supplier'
                             and env = 'RU'
                       )
              )
where event_at = min_time
--limit 2

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


select distinct area_desc from "emilia_gettdwh"."dwh_dim_areas_v"
where area_desc like '%Moscow delivery district%'
and area_desc not like '%2%' and area_desc not like '%11.03%'
and country_key = 2


select *, json_extract_scalar (payload, '$.accept_button_string'),
       json_extract_scalar(payload, '$.order_id')
from events

where event_name = 'dbx|offer_screen|taken|popup_appears'
and env = 'RU'
and json_extract_scalar(payload, '$.order_id') = '1713800168'
and event_date between date'2021-06-14' and date'2021-06-16'
limit 20

select order_gk,
       offer_gk,
       driver_gk,
       fof.class_type_key,
       fb.old_class,
       fb.new_class

from emilia_gettdwh.dwh_fact_offers_v fof
left join (
    select payload, env,
           cast(json_extract_scalar(from_utf8("payload"), '$.order_id') as bigint) as order_id,
           json_extract_scalar(from_utf8("payload"), '$.data.old_class') old_class,
           json_extract_scalar(from_utf8("payload"), '$.data.new_class') new_class


    from "events"
    where "event_name" = 'matching|upgrade_class_automatically'
    and event_date >= date'2021-06-01'

    ) fb on fb.order_id = fof.order_id and fb.env = fof.country_symbol

where fof.country_key = 2
and date_key >= date'2021-06-01'
and fb.order_id is not null
and cast(order_gk as varchar) not like '%500%'
order by order_gk;

-- v3 with area
select
       fof.date_key, j.journey_id, fof.driver_gk, j.created_at,
       number_of_deliveries, number_of_completed_deliveries,
       fof.order_gk, offer_gk, is_received, is_withdrawned,
       fof.driver_response_key, driver_response_desc,
       offer_screen_eta,
       ct.class_type_key final_class, orcl.class_type_key offer_class,
       fo.ordering_corporate_account_gk,
       fo.origin_full_address, fo.dest_full_address,
       fo.user_comment,
       area_desc, area_id,
       fo.origin_longitude, fo.origin_latitude,
        (case when origin_latitude = -1 then null
         else round(ST_Distance(to_spherical_geography(ST_Point(origin_longitude, origin_latitude)),
                               to_spherical_geography(ST_Point(dest_longitude, dest_latitude)))/1000,3) end)
            pickup_dest_distance,
       driver_total_cost,
       est_distance, est_duration,
       fof.distance_from_order_on_creation,
       ct.class_family = 'Premium' is_nf,
       fof.class_type_key final_class,
       class_type_desc final_class, orcl.class_type_key original_class,
       delivered_datetime, response_datetime,
       calc.price,
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

join "emilia_gettdwh"."dwh_dim_areas_v" areas  on ST_Contains(ST_GeometryFromText(areas.borders)
            , ST_Point(fof.origin_order_longitude, fof.origin_order_latitude))
            and "area_desc" like '%Moscow delivery district%'
            and area_desc not like '%2%' and area_desc not like '%11.03%'
            and areas.country_key = 2
left join
        (
            select * from (
                  select event_at, journey_id, price, rank() over (partition by journey_id order by event_at) calc_order
                  from (
                           select cast(json_extract_scalar(from_utf8("payload"), '$.journey_id') as bigint) journey_id,
                                  json_extract_scalar(from_utf8("payload"), '$.amount')                     price,
                                  event_at

                           from events
                           where "event_name" = 'delivery-pricing|calculations'
                             and event_date >= date '2021-03-01'
                             and json_extract_scalar(
                                         json_extract(json_extract(from_utf8("payload"), '$.data'), '$.supplier'),
                                         '$.id') =
                                 '0'
                             --and json_extract_scalar(from_utf8("payload"), '$.journey_id') = '2889783'
                             and cast(json_extract(
                                   json_extract(json_extract(from_utf8("payload"), '$.result'), '$.context'),
                                   '$.side') as varchar) = 'supplier'
                             and env = 'RU'
                      --
                      and json_extract_scalar(from_utf8("payload"), '$.journey_id') = '2318136'


                       )
              )
        where calc_order = 2
           ) as calc
                    on j.journey_id = calc.journey_id

left join
        (select ec.*, cl.class_type_key
            from (
                     select *,
                            max(event_at) over (partition by order_id,driver_id) last_calc

                         from (
                                  select --payload,
                                         event_date,
                                         event_at + interval '3' hour event_at,
                                         cast(json_extract_scalar(from_utf8("payload"), '$.order_id') as bigint) order_id,
                                         json_extract_scalar(
                                                 json_extract(json_extract(json_extract(from_utf8("payload"), '$.pricing'),
                                                                           '$.supplier'), '$.service_class'),
                                                 '$.id') internal_class_id,
                                         json_extract_scalar(json_extract(from_utf8("payload"), '$.odr'),
                                                             '$.driver_id') driver_id

                                  from events
                                  where "event_name" = 'charging|calculation'
                                    and event_date >= date '2021-03-01'
                                    and env = 'RU'
                              )

                     group by 1, 2, 3, 4, 5
                 ) ec
        left join emilia_gettdwh.dwh_dim_class_types_v cl
             on cl.internal_class_id = cast(ec.internal_class_id as bigint)
             and cl.country_key = 2
            and event_at = last_calc

        where event_at = last_calc and driver_id is not null

) orcl on fof.driver_gk = cast(concat('2000', orcl.driver_id) as bigint)
                   and fof.order_id = orcl.order_id

where fof.country_key = 2
and fof.date_key >= date'2021-03-01'
and fo.lob_key in (5,6)
and fof.offer_screen_eta is not null
  and fof.offer_screen_eta < 1000
and ct.class_family = 'Premium'
  and fof.is_received = 1
and fof.origin_order_location_key = 245
order by fo.order_gk, fo.driver_gk
limit 50;



left join
 (
                      select
                          distinct
                          concat('2000', json_extract_scalar(from_utf8(payload),'$.offer_id')) offer_gk,
                          concat('2000', json_extract_scalar(from_utf8(payload),'$.class_id')) class_type_key,
                          cast(json_extract_scalar(from_utf8(payload),'$.order_id') as bigint) order_id,
                          event_date

                      from events as ae

                      where event_name = 'server|order|offer_sent_to_driver'
                      and ae.event_date >=  date'2021-03-01'
                      and env = 'RU'
                      --and json_extract_scalar(from_utf8(payload),'$.order_id') = '1582919311'

                      --order by cast(json_extract_scalar(from_utf8(payload),'$.order_id') as bigint)
         ) as ec on cast(ec.offer_gk as bigint) = fof.offer_gk
