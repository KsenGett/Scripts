select count(distinct offer_gk) event, count(distinct fof_offer_gk) fof_offers
from (
         select ec.*, fof.order_gk fof_order_gk, fof.offer_gk fof_offer_gk, fof.class_type_key fof_class
         from (
                  select distinct concat('2000', json_extract_scalar(from_utf8(payload), '$.offer_id')) offer_gk,
                                  concat('2000', json_extract_scalar(from_utf8(payload), '$.class_id')) class_type_key,
                                  cast(json_extract_scalar(from_utf8(payload), '$.order_id') as bigint) order_id,
                                  event_date

                  from events as ae

                  where event_name = 'server|order|offer_sent_to_driver'
                    and ae.event_date >= date '2021-07-01'
                    and env = 'RU'
                  --and json_extract_scalar(from_utf8(payload),'$.order_id') = '1582919311'

                  --order by cast(json_extract_scalar(from_utf8(payload),'$.order_id') as bigint)
              ) as ec
                  right join emilia_gettdwh.dwh_fact_offers_v fof
                             on cast(ec.offer_gk as bigint) = fof.offer_gk and fof.country_key = 2
         and fof.date_key >= date '2021-07-01'

         where true
--and ec.order_id = 1582919311
         order by fof.order_gk
--limit 10;
     )

select
 distinct cast(json_extract_scalar(from_utf8(payload),'$.offer_id') as bigint) offer_id,
        cast(json_extract_scalar(from_utf8(payload),'$.class_id') as bigint) class_id,
     cast(json_extract_scalar(from_utf8(payload),'$.order_id') as bigint) order_id,
                  event_date
  from events as ae

  where event_name = 'server|order|offer_sent_to_driver'
  and ae.event_date >=  date'2021-03-01'
  and env = 'RU'
 --and json_extract_scalar(from_utf8(payload),'$.order_id') = '1582919311'

order by cast(json_extract_scalar(from_utf8(payload),'$.order_id') as bigint)


select * from "events$partitions"
where event_date = date '2021-06-22'
and event_name like '%offer%'
order by event_name

-- first version
select ec.*, cl.class_type_key
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
                        and json_extract_scalar(from_utf8("payload"), '$.order_id') in ('1583230232', '1583230232', '1583230232')
                        and env = 'RU'
                  )
             group by 1, 2, 3, 4, 5
         ) as ec

left join emilia_gettdwh.dwh_dim_class_types_v cl on cl.internal_class_id = cast(ec.internal_class_id as bigint)
and cl.country_key = 2
where event_at = last_calc
      and driver_id is not null