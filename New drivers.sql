with main as (
select
    date(rftr.ftr) as date_key,
    tp.timecategory,
    tp.subperiod,
    tp.period,
    tp.subperiod2 AS time_period,
    (CASE when v.vendor_name like '%courier car%' THEN 'PHV'
                                  when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                                  when v.vendor_name like '%courier scooter%' THEN 'scooter'
                                  when  v.vendor_name like '%courier trike%' THEN 'e-bike'
                                  ELSE 'taxi' end) AS supply_type,
     v."vendor_name",
     fo.city_name,
     dd.driver_gk,
     (case when dl.external_source is null then dl.source else external_source end )as lead_source ,
     fo.orders orders_OF,
    md.deliveries deliveries_NF,
    md.journeys journeys_NF,
    rftr.is_reftr


    from "emilia_gettdwh"."dwh_dim_drivers_v" dd
    join "emilia_gettdwh"."dwh_dim_vendors_v" v on v.vendor_gk = dd.fleet_gk and vendor_name like '%courier%'
    -- reftr
    left join (

            select d.driver_gk,
            max(case when rftr.ride_type = 'ReFTRD' then 1 else 0 end) is_reftr,
            max(case when rftr.date_key is not null then rftr.date_key else rftr.ftp_date_key end) ftr

            from "emilia_gettdwh"."dwh_dim_drivers_v" d
            left join bp_ba.sm_ftr_reftr_drivers rftr on d.driver_gk = rftr.driver_gk
            where d.country_key = 2
            group by 1

        ) rftr on rftr.driver_gk = dd.driver_gk


     left join --4 sec
      (
            select
            distinct driver_gk,
            l.city_name,
            count(distinct case when ordering_corporate_account_gk <> 20004730 and
             ct.class_family <> 'Premium' then order_gk end) orders

            from emilia_gettdwh.dwh_fact_orders_v fo
            left join emilia_gettdwh.dwh_dim_class_types_v AS ct
                ON ct.class_type_key = fo.class_type_key
            left join "emilia_gettdwh"."dwh_dim_locations" l on fo.origin_location_key = l.location_key

            where fo.lob_key in (5,6)
            --and date_key between current_date - interval '30' day and current_date
            and order_status_key = 7
            and fo.country_key = 2

            group by 1,2

        ) fo on fo.driver_gk = dd.driver_gk

    left join
        (
            select
            distinct courier_gk,
            count(distinct delivery_gk) deliveries,
            count(distinct journey_gk) journeys

            from model_delivery.dwh_fact_deliveries_v fd
            where 1 = 1
            and date(scheduled_at) between current_date - interval '90' day and current_date
            and delivery_status_id = 4
            and delivery_type_id = 1 -- not returns

            group by 1

        ) md on md.courier_gk  = dd.driver_gk
    --change
    LEFT JOIN  data_vis.periods_v AS tp ON tp."hour_key" = 0 and tp.date_key = date(rftr.ftr)
    and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months', '5.Quarters', '7.Std Hours')
    left join analyst.delivery_leads dl on dl.driver_gk = dd.driver_gk

    where 1=1
    and dd.country_key = 2
    --and dd.ftp_date_key between date '2020-01-01' and current_date - interval '1' day
    and tp.timecategory is not null
)

(select
city_name,
count(distinct driver_gk)
from main
where date_key between date'2020-12-7' and date'2020-12-13'
group by 1)



select
source ,
count(distinct driver_gk)
from analyst.delivery_leads
where coalesce( date(reftr), first_ftr) = date'2021-1-17'
group by 1;


-- to check
with ftr as (
            select d.driver_gk,
            max(case when rftr.ride_type = 'ReFTRD' then 1 else 0 end) is_reftr,
            max(case when rftr.date_key is not null then rftr.date_key else rftr.ftp_date_key end) ftr

            from "emilia_gettdwh"."dwh_dim_drivers_v" d
            left join bp_ba.sm_ftr_reftr_drivers rftr on d.driver_gk = rftr.driver_gk
            join "emilia_gettdwh"."dwh_dim_vendors_v" v on v.vendor_gk = d.fleet_gk and v.vendor_name like '%courier%'
            where d.country_key = 2
            group by 1)
select
count(distinct driver_gk)
from ftr
where date(ftr) between date'2020-12-7' and date'2020-12-13'; --w50




/*
Owner - Ekaterina Nesterova
Cube Name - Churned Drivers
ID - C09E509211EAF1493B290080EF15A834
*/
with main as (
(
select ltp_date_key+ interval '7' day as date_key,
tp.timecategory,
tp.subperiod,
tp.period,
tp.subperiod2 AS time_period,
(CASE when v.vendor_name like '%courier car%' THEN 'PHV'
                              when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                              when v.vendor_name like '%courier scooter%' THEN 'scooter'
                              when  v.vendor_name like '%courier trike%' THEN 'e-bike'
                              ELSE 'taxi' end) AS supply_type,
 v."vendor_name",
 l.city_name,
 driver_gk
from "emilia_gettdwh"."dwh_dim_drivers_v" dd
left join "emilia_gettdwh"."dwh_dim_vendors_v" v on v.vendor_gk = dd.fleet_gk
left join "emilia_gettdwh"."dwh_dim_locations" l on dd."primary_city_id" = l.city_id
LEFT JOIN  data_vis.periods_v AS tp ON tp."hour_key" = hour(dd."ltp_datetime") and tp.date_key = (ltp_date_key+ interval '7' day)
and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months', '5.Quarters', '7.Std Hours')
where vendor_name like '%courier%'
and dd.country_key = 2
and tp.timecategory is not null
and "ltp_date_key" between date '2019-12-02' and current_date - interval '7' day)
)
(select
date_key,
count(distinct driver_gk)

from main
where date_key >= date'2021-1-1'
group by 1);
