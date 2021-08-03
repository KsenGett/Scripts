/*
Owner - Ekaterina Nesterova
Cube Name - Retention
ID - BCE39D3C11EB323E10B70080EFD537C8
*/


with t1 as (
        select
            coalesce(reftr.date_key, d.ftp_date_key) ftp_date_key,
                                     reftr.driver_gk is not null is_reftr,
        (CASE when v.vendor_name like '%courier car%' THEN 'PHV'
        when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
        when v.vendor_name is null THEN NULL
        ELSE 'others'
        end) AS supply_type,
        (case when dl.external_source is null then dl.source else external_source end )as lead_source ,
        fo.driver_gk,
        (case when coalesce(reftr.date_key, d.ftp_date_key) >= fo.date_key
            then date_diff('month', coalesce(reftr.date_key, d.ftp_date_key), fo.date_key) end) cohort_period,
        city_name,
        count(order_gk) as orders
        from "emilia_gettdwh"."dwh_fact_orders_v" fo
        left join "emilia_gettdwh".dwh_dim_locations_v l on l.location_key = fo.origin_location_key and l.country_key = 2
        left join "emilia_gettdwh"."dwh_dim_drivers_v" d on d.driver_gk = fo.driver_gk
        left join analyst.delivery_leads dl on dl.driver_gk = d.driver_gk
        LEFT JOIN "emilia_gettdwh"."dwh_dim_vendors_v" v ON d.fleet_gk = v.vendor_gk
        left join (select distinct driver_gk, max(date_key)
                        from analyst.reftr_delivery where ride_type = 'reFTR'
                        and is_courier = 1
                        group by 1) reftr on d.driver_gk = reftr.driver_gk
        where year(fo.date_key) > 2018
        and year(d.ftp_date_key) > 2018
        and fo.lob_key in (5,6)
        and order_status_key = 7
        and fo.country_key = 2
        and vendor_name like '%courier%'
        group by 1,2,3,4,5,6,7
        )

(select year(ftp_date_key) as ftp_year,
month(ftp_date_key) as ftp_month,
'Months' as timecategory,
date_format( ftp_date_key, '%Y-%m') AS ftp_period,
city_name,
cohort_period,
supply_type,
lead_source,
   is_reftr,
count(distinct driver_gk) as drivers
from t1
group by 1,2,3,4,5,6,7,8,9)

union all

(select year(ftp_date_key) as ftp_year,
month(ftp_date_key) as ftp_month,
'Weeks' as timecategory,
date_format( ftp_date_key, '%x-W%v') AS ftp_period,
city_name,
cohort_period,
supply_type,
lead_source,
   is_reftr,
count(distinct driver_gk) as drivers
from (
select
        coalesce(reftr.date_key, d.ftp_date_key) ftp_date_key,
                                     reftr.driver_gk is not null is_reftr,
        (CASE when v.vendor_name like '%courier car%' THEN 'PHV'
        when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
        when v.vendor_name is null THEN NULL
        ELSE 'others'
        end) AS supply_type,
        (case when dl.external_source is null then dl.source else external_source end )as lead_source ,
        fo.driver_gk,
        (case when coalesce(reftr.date_key, d.ftp_date_key) >= fo.date_key
            then date_diff('month', coalesce(reftr.date_key, d.ftp_date_key), fo.date_key) end) cohort_period,
        city_name,
        count(order_gk) as orders
        from "emilia_gettdwh"."dwh_fact_orders_v" fo
        left join "emilia_gettdwh".dwh_dim_locations_v l on l.location_key = fo.origin_location_key and l.country_key = 2
        left join "emilia_gettdwh"."dwh_dim_drivers_v" d on d.driver_gk = fo.driver_gk
        left join analyst.delivery_leads dl on dl.driver_gk = d.driver_gk
        LEFT JOIN "emilia_gettdwh"."dwh_dim_vendors_v" v ON d.fleet_gk = v.vendor_gk
        left join (select distinct driver_gk, max(date_key)
                        from analyst.reftr_delivery where ride_type = 'reFTR'
                        and is_courier = 1
                        group by 1) reftr on d.driver_gk = reftr.driver_gk
where year(fo.date_key) > 2019
and year(d.ftp_date_key) > 2019
and fo.lob_key in (5,6)
and order_status_key = 7
and fo.country_key = 2
and vendor_name like '%courier%'
group by 1,2,3,4,5,6 ,7)
group by 1,2,3,4,5,6,7,8,9)



select count(distinct driver_gk)
from emilia_gettdwh.dwh_dim_drivers_v
where is_courier = 1
and country_key = 2
and registration_date_key <> date'1900-01-01'
  --and is_test <> 1
  --and  is_frozen <> 1
and ftp_date_key between date'2021-06-01' and date'2021-06-30'

select count(distinct driver_gk)
from analyst.reftr_delivery
where is_courier = 1
and date_key between date'2021-06-01' and date'2021-06-30'
and ride_type = 'FTR'