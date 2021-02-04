with class_upgrade as
    (
    select cast(json_extract_scalar(from_utf8("payload"), '$.order_id') as bigint) as order_id,

    json_extract_scalar(from_utf8("payload"), '$.data.old_class') old_class,
    dc_old.class_group old_class_word,
    json_extract_scalar(from_utf8("payload"), '$.data.new_class') new_class,
    dc_new.class_group new_class_word

     from "events"

     left join emilia_gettdwh.dwh_dim_class_types_v dc_old on
     json_extract_scalar(from_utf8(payload), '$.data.old_class') = substring(cast(dc_old.class_type_key as varchar),5)
     and cast(dc_old.class_type_key as varchar) like '2000%'
     left join emilia_gettdwh.dwh_dim_class_types_v dc_new on
     json_extract_scalar(from_utf8(payload), '$.data.new_class') = substring(cast(dc_new.class_type_key as varchar),5)
     and cast(dc_new.class_type_key as varchar) like '2000%'

    where "event_name" = 'matching|upgrade_class_automatically'
    and event_date >= date'2020-08-20'
    and env = 'RU'
    )
(
select
--fo.date_key,
--fo.ordering_corporate_account_gk,
(case when ordering_corporate_account_gk = -1 then 'C2C' else coalesce(accounts.name_internal,corporate_account_name) end) company_name,
fo.origin_latitude, fo.origin_longitude,
origin_full_address,
--(case when dc.class_family = 'Premium' then 'NF' else 'OF' end) platform,

-- if there is a fallback then old class from events, if is no fallback - from fact orders/dim classes
coalesce(t1.old_class_word, dc.class_group) original_class,
coalesce(t1.new_class_word, dc.class_group) final_class,

-- completed journeys total per week
count(distinct case when order_status_key = 7 then fo.order_gk end) completed_journeys_total,
-- w.average completed journeys per day
count(distinct case when order_status_key = 7 then fo.order_gk end)*1.00/count(distinct fo.date_key) completed_journeys,

-- gross journeys total per week
count(distinct fo.order_gk) gross_journeys_total,
-- w.average gross journeys per day
count(distinct fo.order_gk)*1.00/count(distinct fo.date_key) gross_journeys,

-- w.average CR
(count(distinct case when order_status_key = 7 then fo.order_gk end)*1.00 /
count(distinct fo.order_gk)) * 100.00 CR

FROM "emilia_gettdwh"."dwh_fact_orders_v" fo
    left join class_upgrade as t1 on t1.order_id = fo."sourceid"
    LEFT JOIN "emilia_gettdwh".dwh_dim_corporate_accounts_v ca
        ON ca.corporate_account_gk=fo.ordering_corporate_account_gk
    left join "emilia_gettdwh"."dwh_dim_class_types_v" ct on ct."class_type_key" = fo."class_type_key"
    LEFT JOIN sheets."default".delivery_corp_accounts_20191203
        AS accounts ON cast(accounts.company_gk AS bigint)=fo.ordering_corporate_account_gk

    left join emilia_gettdwh.dwh_dim_class_types_v dc on
      fo.class_type_key = dc.class_type_key and cast(dc.class_type_key as varchar) like '2000%'

WHERE fo.country_key = 2
and fo.lob_key in (5,6)
and origin_location_key = 245
and fo.date_key between current_date - interval '8' day and current_date
and fo.ordering_corporate_account_gk <> 20004730

group by 1,2,3,4,5,6--,7--,8
);

select (6 *1.00 / 13) * 100.00


