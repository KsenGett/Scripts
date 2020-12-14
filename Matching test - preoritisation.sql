-- Gross Orders
-- GCR
-- AR%
-- Cancelled %
-- select sum(gross_orders)
-- from (

with groups as (
        select
        order_gk,

        -- 1. md5(cast(cast(order_gk as varchar) as varbinary)) - get Hashed key for an order_gk
        -- 2. to_hex() - get hexadecimal (16, hex) number as varchar
        -- 3. substring(to_hex, -1) - get last bit. ('2','4','6','8','A','C','E') are even bits = control group,
        -- odd bits - test group
        substring(to_hex(md5(cast(cast(sourceid as varchar) as varbinary))), -1) last_bit,
        case when
        substring(to_hex(md5(cast(cast(sourceid as varchar) as varbinary))), -1) in ('2','4','6','8','A','C','E')
        then 'control' else 'test' end "group"

        from emilia_gettdwh.dwh_fact_orders_v fo
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key
                    and ct.country_key = 2

        where date_key >= date'2020-10-29' -- test start
        and fo.lob_key in (5,6)
        and fo.country_key = 2
        and fo.ordering_corporate_account_gk <> 20004730
        and ct.class_family <> 'Premium'

)
(
select
fo.date_key,
loc.city_name,
ca.corporate_account_name,
accounts.name_internal,
ca.corporate_account_gk,
ca.corporate_account_gk in (20007665,20008770,20005031,20004469, 200025241,
20007748, 200025140, 200010175, 200010176, 200025197,
200025281, 200019144) priority_company,
groups."group",

'OF' platform,
count(distinct fo.order_gk) gross_orders,
count(distinct case when fo.order_status_key = 7 and fo.driver_gk <> 200013 then fo.order_gk end) completed_orders,
count(distinct CASE when ((fo.order_status_key = 7 and fo.driver_gk <> 200013) or (fo.order_status_key = 4 and fo.driver_total_cost > 0))
THEN fo.order_gk ELSE null end) AS completed_and_cancelled_orders,
-- cancelled/rej orders
count(distinct case when fo.order_status_key = 4 or fo.driver_gk = 200013 then fo.order_gk end) cancelled_orders,
--count(distinct canc.journey_gk) cancelled_orders,
--count(distinct case when canc.cancellation_stage = 'before driver assignment' then canc.journey_gk end) cancelled_BDA,
count(distinct case when fo.order_status_key = 9 then fo.order_gk end) rejected_orders,
-- offers
count(offer_gk) offers,
count(distinct case when is_received = 1 then fof.driver_gk end ) drivers_recieved_offers,
count(distinct case when is_received = 1 and matching_driving_eta/60.00 > 10 then fof.offer_gk end) -- check
offers_10more_eta,
-- eta
sum(matching_driving_eta) matching_eta_sum,
sum(case when matching_driving_eta is not null then 1 end) matching_eta_count,
--- AR
SUM(CASE WHEN fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) AS numerator,

(SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL THEN 1 ELSE 0 END)
- SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL AND fof.Is_Withdrawned=1
AND fof.Driver_Response_Key<>1 THEN 1 ELSE 0 END) )  AS denominator,

-- withdrawned
count(distinct case when is_received  = 1 and is_withdrawned = 1 then fof.offer_gk end) withdrawned, -- is received =1 is_withdrawned = 1 check
--
count(case when fof.order_gk is null then fo.order_gk end) unoffered,
count(distinct case when driver_unassigned_datetime <> timestamp '1900-01-01 00:00:00' then fof.offer_gk end) unassigned


from emilia_gettdwh.dwh_fact_orders_v fo
-- test control group
LEFT JOIN groups on groups.order_gk = fo.order_gk
-- offers
left join emilia_gettdwh.dwh_fact_offers_v fof on fo.order_gk = fof.order_gk
        and fof.country_key = 2 and fof.date_key >= date'2020-10-29'
-- company info
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
          ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
          and ca.country_key = 2
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key
            and ct.country_key = 2
LEFT JOIN sheets."default".delivery_corp_accounts_20191203 AS accounts
    ON cast(accounts.company_gk AS bigint)=fo.ordering_corporate_account_gk
-- locations
left join emilia_gettdwh.dwh_dim_locations_v loc on fo.origin_location_key = loc.location_key
            and loc.country_key = 2
where 1=1
and fo.country_key = 2
and fo.lob_key in (5,6)
and ct.class_family <> 'Premium'
and fo.ordering_corporate_account_gk <> 20004730
and fo.date_key >= date'2020-10-29' -- test start

group by 1,2,3,4,5,6,7
)
-- )
-- where date_key between date'2020-10-29' and date'2020-12-06'
-- and priority_company = TRUE
-- and "group" = 'test'


--- michael's events check
        sELECT distinct
                cast(json_extract_scalar(json_parse(from_utf8(e.payload)), '$.is_a_group') AS bigint) AS source_id,
                e.payload
                FROM events e
                WHERE event_name = 'dynamic_matching|delivery_priority_experiment'
                and event_date >= date_add('day', -60, current_date)
                and env = 'RU'
                limit 100

-- {"batch_id":"ru:0:1607941987002","created_at":1607941989652,
-- "env":"RU","event_name":"dynamic_matching|delivery_priority_experiment",
-- "is_a_group":{"1473007464":false,"1473024500":false,"1473024910":false,"1473024915":false,"1473024920":false,"1473024925":true,"1473024945":true,"1473024966":true,"1473025237":false},
-- "scrum":"prod"}

select substring(to_hex(md5(cast(cast(20001473024945 as varchar) as varbinary))), -1) last_bit,
        case when
        substring(to_hex(md5(cast(cast(20001473024945 as varchar) as varbinary))), -1) in ('2','4','6','8','A','C','E')
        then 'control' else 'test' end "group"
