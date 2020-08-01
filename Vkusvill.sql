select
fo.order_gk order_id,
fo.driver_gk curier_gk,
cast(fo.driver_total_cost as varchar),
du.phone client_phone, dr.phone driver_phone,
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
json_extract_scalar("drop_off_contact", '$.phone_number') client_phone,
dr.phone driver_phone,
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



select du.phone, username
from emilia_gettdwh.dwh_fact_orders_v fo
join emilia_gettdwh.dwh_dim_users_v du
on fo.riding_user_gk = du.user_gk
where date_key > date'2020-06-01'
and order_status_key = 7
and fo.country_key = 2
and fo.ordering_corporate_account_gk in (200023153, 200025119)
limit 10