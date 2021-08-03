drop table if exists temp.reftr_delivery

/*
Concerns:
can differ from dim drivers as it considers only completed orders (status 7)
*/


create table temp.reftr_delivery
as;

with main as (
    with ftr as (
        with drivers_all as (
            select distinct d.driver_gk, d.is_courier, d.primary_city_id,
                            fo.fleet_gk               ftr_fleet,
                            d.fleet_gk                current_fleet,
                            fo.fleet_gk <> d.fleet_gk change_fleet,
                            d.ftp_date_key            ftr,
                            date_key,
                            substring(areas.area_desc, 25) moscow_region
            from emilia_gettdwh.dwh_fact_orders_v fo
                     join emilia_gettdwh.dwh_dim_drivers_v d on fo.driver_gk = d.driver_gk
            left join "emilia_gettdwh"."dwh_dim_areas_v" areas  on ST_Contains(ST_GeometryFromText(areas.borders)
            , ST_Point(fo.origin_longitude, fo.origin_latitude)) and "area_desc" like '%Moscow regions deliver%'

                and d.country_key = 2
                 --and d.is_courier = 1
                 and is_test <> 1
                --and ftp_date_key >= date '2019-01-01'
            where fo.country_key = 2
--and date_key >= date'2019-01-01'
              --and date_key >= date '2019-01-01'
              --and lob_key in (5, 6)
              and order_status_key = 7
              and ordering_corporate_account_gk <> 20004730
              and d.driver_gk <> 200013
            and fo.driver_gk = 20001072774 -- 04-11 ftr but no in table
            order by driver_gk, date_key
        )
            (select *,
                    lag(date_key, 1) over (partition by driver_gk order by date_key) as previous_date
             from drivers_all)
    )
        (select *, date_diff('day', date_key, previous_date) days_between
         from ftr
         where (date_diff('day', date_key, previous_date) is null or date_diff('day', date_key, previous_date) <= -60)
        )
)
(select driver_gk, (case when days_between is null and ftr <> date'1900-01-01' then ftr else date_key end) date_key, primary_city_id, moscow_region,
        ftr_fleet, current_fleet, change_fleet, is_courier,
        (case when ftr = date_key or days_between is null then 'FTR'
            when days_between is not null then 'reFTR' end) ride_type,
        abs(days_between) reftr_pause
    from main
    order by driver_gk, date_key)
    ;

grant all privileges on temp.reftr_delivery to role public with grant option;