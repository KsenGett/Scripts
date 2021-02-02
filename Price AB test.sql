-- select
-- avg(numerator*1.00 / nullif(denominator,0)) mean,
-- sum(numerator)*1.00/sum(denominator) w_mean
--
-- from
-- (
with GH AS (
    SELECT
    fdh.driver_gk,
    count(CASE when (fdh.date_key between dd.ltp_date_key - interval '20' day and dd.ltp_date_key)
     and fdh.driver_status_key IN (2, 4, 5, 6) then date_key end) days,

    sum(CASE when (fdh.date_key between dd.ltp_date_key - interval '20' day and dd.ltp_date_key)
                    and fdh.driver_status_key IN (2, 4, 5, 6) THEN fdh.minutes_in_status ELSE 0 end)/60.0 AS gh,
    sum(case when (fdh.date_key between dd.ltp_date_key - interval '20' day and dd.ltp_date_key)
                    and fdh.driver_status_key IN (5, 6) THEN fdh.minutes_in_status ELSE 0 end)/60.0 gh_in_ride

    FROM emilia_gettdwh.dwh_fact_drivers_hourly_v fdh
    left join emilia_gettdwh.dwh_dim_drivers_v dd on fdh.driver_gk = dd.driver_gk and dd.country_key = 2
     WHERE   1 = 1
                and fdh.date_key between current_date - interval '130' day and current_date - interval '10' day
                and fdh.country_key = 2
    GROUP BY 1
    )

, AR AS (
    SELECT
     fof.driver_gk,
     SUM(CASE WHEN (fof.date_key between dd.ltp_date_key - interval '20' day and dd.ltp_date_key)
     and fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) AS numerator, -- accepted


    (SUM(CASE WHEN (fof.date_key between dd.ltp_date_key - interval '20' day and dd.ltp_date_key)
                and fof.Delivered_Datetime IS NOT NULL or fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) --received
           - SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL AND fof.Is_Withdrawned=1
           AND fof.Driver_Response_Key<>1 THEN 1 ELSE 0 END) )  AS denominator

    FROM emilia_gettdwh.dwh_fact_offers_v fof
    left join emilia_gettdwh.dwh_dim_drivers_v dd on fof.driver_gk = dd.driver_gk and dd.country_key = 2
     LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v cl ON cl.class_type_key = fof.class_type_key
    WHERE lob_key IN (5,6)
    and date_key  between current_date - interval '130' day and current_date - interval '10' day
    GROUP BY 1
    )
(
select
distinct source_id,
dd.phone, dd.driver_name,
fo.days,
fo.city_name,
dd.ltp_date_key,
fo.orders_OF + coalesce(orders_NF, 0) deliveries,
GH.gh, GH.days gh_days, GH.gh_in_ride,
AR.numerator, AR.denominator

from emilia_gettdwh.dwh_dim_drivers_v dd
left join emilia_gettdwh.dwh_dim_vendors_v fl on dd.fleet_gk = fl.vendor_gk
    left join --4 sec
      (
            select
            distinct fo.driver_gk,
            l.city_name,
            count(distinct
             case when (fo.date_key between dd.ltp_date_key - interval '20' day and dd.ltp_date_key)
             then date_key end) days,
            count(distinct case when
                            (fo.date_key between dd.ltp_date_key - interval '20' day and dd.ltp_date_key) and
                            ordering_corporate_account_gk <> 20004730 and ct.class_family <> 'Premium'
                            then order_gk end) orders_OF,
             count(distinct case when
                            (fo.date_key between dd.ltp_date_key - interval '20' day and dd.ltp_date_key) and
                            ordering_corporate_account_gk = 20004730 then order_gk end) orders_NF

            from emilia_gettdwh.dwh_fact_orders_v fo
            left join emilia_gettdwh.dwh_dim_class_types_v AS ct
                ON ct.class_type_key = fo.class_type_key
            left join "emilia_gettdwh"."dwh_dim_locations" l on fo.origin_location_key = l.location_key
            left join emilia_gettdwh.dwh_dim_drivers_v dd on fo.driver_gk = dd.driver_gk and dd.country_key = 2

            where fo.lob_key in (5,6)
            and order_status_key = 7
            and fo.country_key = 2
            and fo.date_key between current_date - interval '200' day and current_date - interval '20' day


            group by 1,2

        ) fo on fo.driver_gk = dd.driver_gk

left join AR on dd.driver_gk = AR.driver_gk
left join GH on dd.driver_gk = GH.driver_gk

where dd.country_key = 2
and fl.vendor_name like '%courier%'
and ltp_date_key between current_date - interval '120' day and current_date - interval '20' day
)
-- )



-- Check number of drivers
select
count (distinct source_id)

from emilia_gettdwh.dwh_dim_drivers_v dd
left join emilia_gettdwh.dwh_dim_vendors_v fl on dd.fleet_gk = fl.vendor_gk

where dd.country_key = 2
and fl.vendor_name like '%courier%'
and ltp_date_key between current_date - interval '60' day and current_date - interval '14' day


select *
from sheets."default".