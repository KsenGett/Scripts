---MAIN
-- with main as (
with class_upgrade as
    (
    select *,cast(json_extract_scalar(from_utf8("payload"), '$.order_id') as bigint) as order_id,
    concat(json_extract_scalar(from_utf8("payload"), '$.data.old_class'), ' to ',
    json_extract_scalar(from_utf8("payload"), '$.data.new_class')) upgrade_details,

    concat(dc_old.class_group, ' to ', dc_new.class_group) upgrade_words,
    dc_old.class_group old_class

     from "events"

     left join emilia_gettdwh.dwh_dim_class_types_v dc_old on
     json_extract_scalar(from_utf8(payload), '$.data.old_class') = substring(cast(dc_old.class_type_key as varchar),5)
     and cast(dc_old.class_type_key as varchar) like '2000%'
     left join emilia_gettdwh.dwh_dim_class_types_v dc_new on
     json_extract_scalar(from_utf8(payload), '$.data.new_class') = substring(cast(dc_new.class_type_key as varchar),5)
     and cast(dc_new.class_type_key as varchar) like '2000%'

    where "event_name" = 'matching|upgrade_class_automatically'
    and event_date = date'2021-03-01'
      and json_extract_scalar(from_utf8("payload"), '$.order_id') = '1582919311'
    and env = 'RU'
    )
(
select
tp.timecategory,
tp.subperiod,
tp.period,
tp.subperiod2 AS time_period,
fo.date_key,
"city_name",
fo.ordering_corporate_account_gk,
ca.corporate_account_name,
accounts.name_internal company_name,
fo.origin_latitude, fo.origin_longitude,
origin_full_address,
st.order_status_desc_full,
(case when dc.class_family = 'Premium' then 'NF' else 'OF' end) platform,

(case when t1.old_class is null then dc.class_group else t1.old_class end) class,

case when t1.upgrade_details is null then substring(cast(fo.class_type_key as varchar),5)
    else t1.upgrade_details end as upgrade_det,

case when t1.upgrade_words is null then dc.class_group
    else t1.upgrade_words end as upgrade_det_words,

count(distinct fo.order_gk) gross_orders,
count(distinct offer_gk) offers

FROM "emilia_gettdwh"."dwh_fact_orders_v" fo
    left join class_upgrade as t1 on t1.order_id = fo."sourceid"
    LEFT JOIN "emilia_gettdwh"."dwh_dim_locations_v" l ON fo."origin_location_key" = l."location_key"
    LEFT JOIN "emilia_gettdwh".dwh_dim_corporate_accounts_v ca ON ca.corporate_account_gk=fo.ordering_corporate_account_gk
    left join "emilia_gettdwh"."dwh_dim_class_types_v" ct on ct."class_type_key" = fo."class_type_key"
    left join emilia_gettdwh.dwh_fact_offers_v fof on fof.order_gk = fo.order_gk
     and fof.country_key = 2 and fof.date_key >= date'2020-08-30'
    LEFT JOIN sheets."default".delivery_corp_accounts_20191203
        AS accounts ON cast(accounts.company_gk AS bigint)=fo.ordering_corporate_account_gk
    LEFT JOIN  data_vis.periods_v AS tp ON tp.hour_key = 0 and tp.date_key = fo.date_key
       and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
    left join emilia_gettdwh.dwh_dim_order_statuses_v st on fo.order_status_key = st.order_status_key
    left join emilia_gettdwh.dwh_dim_class_types_v dc on
      fo.class_type_key = dc.class_type_key and cast(dc.class_type_key as varchar) like '2000%'

WHERE fo.country_key = 2
and fo.lob_key = 5
and fo.date_key >= date '2020-07-20'
and timecategory is not null
and fo.ordering_corporate_account_gk <> 20004730
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
--)

-- (select
-- --platform,
-- distinct main.order_gk,
-- jo.order_gk
--
-- from main
-- left join
--
--     (
--     select
--     scheduled_at,
--     date(scheduled_at) date_key,
--     order_gk,
--     journey_gk
--
--     from model_delivery.dwh_fact_journeys_v
--
--     where 1=1
--     --date(scheduled_at) between date'2020-12-14' and date'2020-12-20'
--     and date(scheduled_at + interval '3' hour) in (date'2020-12-14')
--     and country_symbol = 'RU'
--     order by journey_gk
--     ) jo on main.order_gk = jo.order_gk
--
--
-- where 1=1--platform = 'NF'
-- and main.date_key =date'2020-12-14'
-- and timecategory = '2.Dates'
-- and jo.order_gk is null
-- --and order_status_desc_full = 'Completed'
-- and platform = 'NF'
-- --group by 1
-- )

;



--info
select class_type_key, class_group, class_type_desc, internal_class_id
from emilia_gettdwh.dwh_dim_class_types_v
where cast(class_type_key as varchar) like '2000%'



-- check double fallback

select cast(json_extract_scalar(from_utf8("payload"), '$.order_id') as bigint) as order_id,
    event_date,
    concat(json_extract_scalar(from_utf8("payload"), '$.data.old_class'), ' to ',
    json_extract_scalar(from_utf8("payload"), '$.data.new_class')) upgrade_details

     from "events"

    where "event_name" = 'matching|upgrade_class_automatically'
    and event_date >= date'2020-08-20'
    and json_extract_scalar(from_utf8("payload"), '$.data.old_class') =
    json_extract_scalar(from_utf8("payload"), '$.data.new_class')
    and env = 'RU'

