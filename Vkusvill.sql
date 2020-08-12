/*У нас всплыло мошенничество у ВкусВилла:
Давай проверим, как часто у нас совпадают контакты клиентов ВВ и курьеров, которые возят
его заказы на обеих платформах.
 */
with all_orders as (
select
cast(substring(cast(fo.order_gk as varchar), 4) as integer) order_id,
fo.driver_gk curier_gk,
cast(fo.driver_total_cost as varchar) driver_total_cost,
substring(du.phone, strpos(du.phone, '9')) client_phone,
substring(dr.phone, strpos(dr.phone, '9')) driver_phone,
fo.date_key,
(CASE when fo.ordering_corporate_account_gk = cast(internal.company_gk AS integer)
        THEN internal.name_internal ELSE
        ca.corporate_account_name end) company_name,
fo.ordering_corporate_account_gk company_gk,
 'GT' as platform

from emilia_gettdwh.dwh_fact_orders_v fo
left join emilia_gettdwh.dwh_dim_drivers_v dr on fo.driver_gk = dr.driver_gk
left join emilia_gettdwh.dwh_dim_users_v du
on fo.riding_user_gk  = du.user_gk

LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v ca
ON fo.ordering_corporate_account_gk = ca.corporate_account_gk
--company internal name
LEFT JOIN sheets."default".delivery_corp_accounts_20191203 internal ON
cast(internal.company_gk AS integer) = fo.ordering_corporate_account_gk

where fo.order_status_key = 7
and date_key > date'2020-06-01'
and fo.lob_key = 5
and fo.country_key = 2
and fo.ordering_corporate_account_gk in (200023153, 200025119)
union
 -----
select
d.id order_id,
dr.driver_gk curier_gk,
'Was not extracted' driver_total_cost,
substring (
    json_extract_scalar("drop_off_contact", '$.phone_number'),
    strpos(json_extract_scalar("drop_off_contact", '$.phone_number'), '9')
            ) client_phone,
substring(dr.phone, strpos(dr.phone, '9')) driver_phone,
d.dropped_off_at date_key,
(CASE when concat('2000', cast(d.company_id as varchar)) = internal.company_gk
                    THEN internal.name_internal ELSE
                        ca.corporate_account_name end) company_name,
ca.corporate_account_gk company_gk,
'EDP' as platform

--user number EDP - drop_off_contact
from "delivery"."public"."deliveries" d
left join "delivery"."public"."journeys" j on j.id = d."journey_id" and d."env"= j."env"

--driver info
left join emilia_gettdwh.dwh_dim_drivers_v dr
on cast(concat('2000', cast(j.supplier_id as varchar)) as bigint) = dr.driver_gk

--company info
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v ca
ON concat('2000', cast(d.company_id as varchar)) = cast(ca.corporate_account_gk as varchar)
LEFT JOIN sheets."default".delivery_corp_accounts_20191203 internal ON
cast(internal.company_gk AS integer) = ca.corporate_account_gk

where j.status = 'completed'
and d.company_id in ('23861')
and d.env = 'RU'
and d.dropped_off_at > timestamp'2020-06-01 00:00:00'
)

select order_id, curier_gk, driver_total_cost, client_phone, driver_phone,
date_key, company_name, platform
from all_orders
where client_phone in
    (select client_phone from all_orders
        intersect
    select driver_phone from all_orders)
and driver_phone <> '';


--Check how intersect works. List of numbers corresponds to the Python result
---
with all_orders as (
select
cast(substring(cast(fo.order_gk as varchar), 4) as integer) order_id,
fo.driver_gk curier_gk,
cast(fo.driver_total_cost as varchar) driver_total_cost,
substring(du.phone, strpos(du.phone, '9')) client_phone,
substring(dr.phone, strpos(dr.phone, '9')) driver_phone,
fo.date_key,
(CASE when fo.ordering_corporate_account_gk = cast(internal.company_gk AS integer)
        THEN internal.name_internal ELSE
        ca.corporate_account_name end) company_name,
fo.ordering_corporate_account_gk company_gk,
 'GT' as platform

from emilia_gettdwh.dwh_fact_orders_v fo
left join emilia_gettdwh.dwh_dim_drivers_v dr on fo.driver_gk = dr.driver_gk
left join emilia_gettdwh.dwh_dim_users_v du
on fo.riding_user_gk  = du.user_gk

LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v ca
ON fo.ordering_corporate_account_gk = ca.corporate_account_gk
--company internal name
LEFT JOIN sheets."default".delivery_corp_accounts_20191203 internal ON
cast(internal.company_gk AS integer) = fo.ordering_corporate_account_gk

where fo.order_status_key = 7
and date_key > date'2020-06-01'
and fo.lob_key = 5
and fo.country_key = 2
and fo.ordering_corporate_account_gk in (200023153, 200025119)
union
 -----
select
d.id order_id,
dr.driver_gk curier_gk,
'Was not extracted' driver_total_cost,
substring (
    json_extract_scalar("drop_off_contact", '$.phone_number'),
    strpos(json_extract_scalar("drop_off_contact", '$.phone_number'), '9')
            ) client_phone,
substring(dr.phone, strpos(dr.phone, '9')) driver_phone,
d.dropped_off_at date_key,
(CASE when concat('2000', cast(d.company_id as varchar)) = internal.company_gk
                    THEN internal.name_internal ELSE
                        ca.corporate_account_name end) company_name,
ca.corporate_account_gk company_gk,
'EDP' as platform

--user number EDP - drop_off_contact
from "delivery"."public"."deliveries" d
left join "delivery"."public"."journeys" j on j.id = d."journey_id" and d."env"= j."env"

--driver info
left join emilia_gettdwh.dwh_dim_drivers_v dr
on cast(concat('2000', cast(j.supplier_id as varchar)) as bigint) = dr.driver_gk

--company info
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v ca
ON concat('2000', cast(d.company_id as varchar)) = cast(ca.corporate_account_gk as varchar)
LEFT JOIN sheets."default".delivery_corp_accounts_20191203 internal ON
cast(internal.company_gk AS integer) = ca.corporate_account_gk

where j.status = 'completed'
and d.company_id in ('23861')
and d.env = 'RU'
and d.dropped_off_at > timestamp'2020-06-01 00:00:00'
)

select client_phone from all_orders
where client_phone <> ''
intersect
select driver_phone from all_orders
where driver_phone <> ''

-- values check
select du.phone, username
from emilia_gettdwh.dwh_fact_orders_v fo
join emilia_gettdwh.dwh_dim_users_v du
on fo.riding_user_gk = du.user_gk
where date_key > date'2020-06-01'
and order_status_key = 7
and fo.country_key = 2
and fo.ordering_corporate_account_gk in (200023153, 200025119)
limit 10

--------------USEFUL FUNCTIONS
--extract phone numbers
select substring('number +79168153003', strpos('number +79168153003', '9'))
--check whether the value is in a column
select contains(array[213,234,213,234], 213)
-- find intersect values
select array_intersect(array[213,234,213,234], array[23,234,213])
--frequency of values
sELECT histogram(x) FROM UNNEST(ARRAY[1111, 1111, 22, 22, 1111]) t(x);