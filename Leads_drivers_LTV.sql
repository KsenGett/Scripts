with check as (;

with main as (

    select
    distinct
    l.driver_gk,
    l.registration_name,
    l.phone_number,
    l.source,
    fo.date_key,
    tp.timecategory,
    tp.period,
    tp.subperiod,
    fl.vendor_name,
    l.first_ftr, l.reftr,
    l.reftr is not null is_reftr,
    (case when date(l.reftr) is not null then date(l.reftr) else date(l.first_ftr) end) ftr,
    -- activity
    fo.city_name,
    fo.TR,
    fo.of_orders deliveries_OF,
    fo.nf_orders deliveries_NF,
    fo.journeys_total journeys_total,

    coalesce(fo.of_orders,0)  + coalesce(fo.nf_orders,0) deliveries,
    count(fo.date_key) over(partition by l.driver_gk) LT,
    max(fo.date_key) over(partition by l.driver_gk) last_date

    from
        (

        select
        distinct
                d.driver_gk,
                d.phone phone_number,
                d.driver_name registration_name,
                d.fleet_gk,

                (case when d.driver_gk = ref.driver_gk then 'Reff'
                when d.fleet_gk in (200014202,200016265,200016266,200016267,200016359,200016361) then 'Gorizont'
                when d.fleet_gk in (200017083,200017177,200017412,200017342,200017205,200017203, 200017524,200017523,200017517,200017430) then 'Scouts'
                when d.fleet_gk = 200017111 then 'D_Uspekha'
                else 'Fleet' end) source,

                d.registration_date_key,
                nullif(d.ftp_date_key, date'1900-1-1') first_ftr,
                max(case when ride_type = 'ReFTRD' then date(rftr.date_key) end) reFTR

            from emilia_gettdwh.dwh_dim_drivers_v d
            left join bp_ba.sm_ftr_reftr_drivers rftr on d.driver_gk = rftr.driver_gk
                -- to filter by fleet name selecting only couriers
                left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
                -- reff
                left join
                (
                    select
                    d.driver_gk,
                    ( case when date(rftr.date_key) is null then d.ftp_date_key else date(rftr.date_key) end)
                     between cast("start" as date) and cast("end" as date)

                    from emilia_gettdwh.dwh_dim_drivers_v d
                    left join bp_ba.sm_ftr_reftr_drivers rftr on d.driver_gk = rftr.driver_gk
                    left join "sheets"."default".ru_fleet_promo ref on cast(ref.fleet_gk as integer) = d.fleet_gk

                    where 1=1
                    -- select drivers who were led by reff
                    and (
                        (( case when date(rftr.date_key) is null then d.ftp_date_key else date(rftr.date_key) end)
                                                between cast("start" as date) and cast("end" as date))
                        or (d.registration_date_key between cast("start" as date) and cast("end" as date))
                            )

                    and d.country_key = 2

                ) ref on ref.driver_gk = d.driver_gk

                where 1=1
                and d.phone is not null
                and d.driver_gk <> 2000683923 -- some old bug
                and d.is_courier = 1
                and d.country_key = 2
                --and d.registration_date_key >= date'2020-07-01'
                group by 1,2,3,4,5,6,7

            ) l

    -- Deliveries, region name, TR, last_date, FTR
    left join
    (       select
            distinct driver_gk,
            date_key,
            city_name,
            --finance
            (sum(customer_total_cost) - sum(driver_total_cost_inc_vat))
            + (sum(driver_total_commission_exc_vat)*(-1)) TR,
            -- orders only on OF
            count(distinct case when ct.class_family <> 'Premium'
             and ordering_corporate_account_gk <> 20004730 then  order_gk end) of_orders,
            count(distinct case when ordering_corporate_account_gk = 20004730 then  order_gk end) nf_orders,
            count(distinct case when ordering_corporate_account_gk <> 20004730 then  order_gk end) journeys_total


            from emilia_gettdwh.dwh_fact_orders_v fo
            left join emilia_gettdwh.dwh_dim_class_types_v AS ct
                ON ct.class_type_key = fo.class_type_key
            left join emilia_gettdwh.dwh_dim_locations_v loc on
                fo.origin_location_key = loc.location_key and loc.country_id = 2

            where fo.lob_key in (5,6)
            and date_key between current_date - interval '6' month and current_date
            --and order_status_key = 7
            and fo.country_key = 2
            --and driver_gk = 2000747210

            group by 1,2,3

        ) fo on l.driver_gk = fo.driver_gk

    -- timecategory
    LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.hour_key = 0 and tp.date_key = fo.date_key
           and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
           and tp.timecategory is not null
    -- fleet names
    left join emilia_gettdwh.dwh_dim_vendors_v fl on l.fleet_gk = fl.vendor_gk
    )

(
select
*
from main
where ftr between current_date - interval '6' month and current_date
)
-- to check
select
date_key,
count(distinct driver_gk)
from check
where timecategory = '2.Dates'
and date_key = ftr
and date_key > date'2021-3-2'
group by 1




select count(distinct driver_gk)
from analyst.delivery_leads
where date(reftr) between date'2020-12-14' and date'2020-12-20' -- w51


select count(distinct d.driver_gk)
from emilia_gettdwh.dwh_dim_drivers_v d
left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
left join bp_ba.sm_ftr_reftr_drivers rftr on d.driver_gk = rftr.driver_gk

where d.country_key = 2
and vendor_name like '%courier%'
and date(rftr.date_key) between date'2020-12-14' and date'2020-12-20'
and ride_type = 'ReFTRD'

select * from model_delivery.dwh_dim_delivery_statuses_v
--group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18;

















--- retention agents
select
distinct d.driver_gk,
fo.ftr, week(fo.ftr) ftr_week, month(fo.ftr) ftr_month,
d.ftp_date_key, fo.last_date,
fo.ftr = d.ftp_date_key is_ftp_date_key,
d.registration_date_key,
fo.orders + md.deliveries deliveries,
date_diff('month', fo.ftr, fo.last_date) cohort_period_month,
date_diff('day', fo.ftr, fo.last_date) cohort_period_day,
date_diff('week', fo.ftr, fo.last_date) cohort_period_weeks


from "emilia_gettdwh"."dwh_dim_drivers_v" d
-- Deliveries OF, ftr, region name, LT, TR
LEFT JOIN --14 sec
  (
        SELECT
        distinct driver_gk,
        region_name,
        --finance
        (sum(customer_total_cost) - sum(driver_total_cost_inc_vat))
        + (sum(driver_total_commission_exc_vat)*(-1)) TR,

        --count(distinct CASE when order_status_key = 7 THEN date_key end) LT_days,
        -- ftr ON any platform
        min(date_key) ftr,
        max(date_key) last_date,
        -- orders only ON OF
        count(distinct CASE when ct.class_family <> 'Premium' THEN order_gk end) orders

        FROM emilia_gettdwh.dwh_fact_orders_v fo
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
            ON ct.class_type_key = fo.class_type_key
        LEFT JOIN emilia_gettdwh.dwh_dim_locations_v loc ON
        fo.origin_location_key = loc.location_key and loc.country_id = 2

        WHERE fo.lob_key IN (5,6)
        and date_key >= date'2020-07-01'
        and ordering_corporate_account_gk <> 20004730
        and order_status_key = 7
        and fo.country_key = 2
        and fo.fleet_gk in (200014202,200016265,200016266,
                    200016267,200016359,200016361)

        GROUP BY 1,2

    ) fo ON fo.driver_gk = d.driver_gk

-- Deliveries NF
 LEFT JOIN --2sec
    (
        SELECT distinct courier_gk,

        count(distinct delivery_gk) deliveries,
        count(distinct journey_gk) journeys

        FROM model_delivery.dwh_fact_deliveries_v

        WHERE
        date(created_at) >= date'2020-07-01'
        and delivery_status_id = 4

        GROUP BY 1

    ) md ON md.courier_gk  = d.driver_gk



where d.fleet_gk in (200014202,200016265,200016266,
                    200016267,200016359,200016361)
and d.ftp_date_key >= date'2020-07-01' ;







---Agent Retention
with t1 as (
    select
    distinct d.ftp_date_key,
    fo.driver_gk,
    date_diff('day', d.ftp_date_key, fo.date_key) cohort_period_day,
    date_diff('month', d.ftp_date_key, fo.date_key) cohort_period_month,
    count(order_gk) as orders

    from "emilia_gettdwh"."dwh_fact_orders_v" fo
    left join "emilia_gettdwh"."dwh_dim_drivers_v" d on d.driver_gk = fo.driver_gk
    LEFT JOIN "emilia_gettdwh"."dwh_dim_vendors_v" v ON d.fleet_gk = v.vendor_gk

    where 1=1
    and d.ftp_date_key >= date'2020-10-01'
    and fo.date_key between date'2020-10-01' and date'2020-10-31'
    and fo.lob_key in (5,6)
    and order_status_key = 7
    and fo.country_key = 2
    and fo.fleet_gk in (200014202,200016265,200016266,
                    200016267,200016359,200016361)

    group by 1,2,3,4 )

(select
--year(ftp_date_key) as ftp_year,
month(ftp_date_key) as ftp_month,
week(ftp_date_key) ftp_week,
cohort_period_day,
cohort_period_month,
count(distinct driver_gk) as drivers

from t1
group by 1,2,3,4,5
order by 4,1,2,3 asc)


select distinct rides,
count(distinct driver_gk) drivers, sum(TR) sum_TR

from

(select
distinct d.driver_gk,
    (sum(customer_total_cost) - sum(driver_total_cost_inc_vat))
        + (sum(driver_total_commission_exc_vat)*(-1)) TR,
    count(distinct order_gk) rides

    from "emilia_gettdwh"."dwh_fact_orders_v" fo
    left join "emilia_gettdwh"."dwh_dim_drivers_v" d on d.driver_gk = fo.driver_gk

    where 1=1
    and d.registration_date_key >= date'2020-10-01'
    and fo.date_key between date'2020-10-01' and date'2020-10-31'
    and fo.lob_key in (5,6)
    and order_status_key = 7
    and fo.country_key = 2
    and d.fleet_gk in (200014202,200016265,200016266,
                    200016267,200016359,200016361)

    group by 1)
group by 1;




-- select distinct deliv,
-- count(distinct driver_gk) drivers,
-- sum(TR) TR
--
-- from
-- (
    select
    distinct fo.driver_gk,
    fo.journeys_OF,
    (case when md.deliveries is not null then md.deliveries else 0 end) + fo.orders_OF deliv,
    --md.journeys_nf + fo.orders_OF journeys,
    sum(fo.TR) TR

    from
  (
        SELECT
        distinct d.driver_gk,
        --finance
        (sum(customer_total_cost) - sum(driver_total_cost_inc_vat))
        + (sum(driver_total_commission_exc_vat)*(-1)) TR,

        count(distinct case when ct.class_family <> 'Premium' then order_gk end) orders_OF,
        count(distinct order_gk) journeys_OF

        FROM emilia_gettdwh.dwh_fact_orders_v fo
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
            ON ct.class_type_key = fo.class_type_key
        LEFT JOIN emilia_gettdwh.dwh_dim_locations_v loc ON
        fo.origin_location_key = loc.location_key and loc.country_id = 2

        left join "emilia_gettdwh"."dwh_dim_drivers_v" d
                    on d.driver_gk = fo.driver_gk

        WHERE fo.lob_key IN (5,6)
        -- orders for 30 days
        and date_key between d.registration_date_key and (registration_date_key + interval '30' day)
        and ordering_corporate_account_gk <> 20004730
        and order_status_key = 7
        and fo.country_key = 2
        and d.fleet_gk in (200014202,200016265,200016266,
                    200016267,200016359,200016361)
        and (registration_date_key + interval '30' day) >= date'2020-10-01'
        and d.registration_date_key <= date'2020-10-31'


        GROUP BY 1

        ) fo

-- Deliveries NF
 LEFT JOIN --2sec
    (
        SELECT distinct courier_gk, d.registration_date_key,
        registration_date_key + interval '30' day promo,
        count(distinct delivery_gk) deliveries,
        count(distinct journey_gk) journeys_nf


        FROM model_delivery.dwh_fact_deliveries_v md
        left join "emilia_gettdwh"."dwh_dim_drivers_v" d
                    on d.driver_gk = md.courier_gk

        WHERE
        date(created_at) between d.registration_date_key
        and (registration_date_key + interval '30' day)

        and d.fleet_gk in (200014202,200016265,200016266,
                    200016267,200016359,200016361)
        and (registration_date_key + interval '30' day) >= date'2020-10-01'
        and d.registration_date_key <= date'2020-10-31'
        and delivery_status_id = 4

        GROUP BY 1,2,3

    ) md ON md.courier_gk  = fo.driver_gk

group by 1,2,3;



select distinct journeys,
count(distinct driver_gk) drivers,
sum(TR) TR

from
(
    select
    distinct d.driver_gk,
    count(case when fo.date_key between d.registration_date_key
            and d.promo_period then fo.order_gk end) journeys,
    (sum(customer_total_cost) - sum(driver_total_cost_inc_vat))
        + (sum(driver_total_commission_exc_vat)*(-1)) TR

    from
    (select distinct driver_gk, fleet_gk,
            registration_date_key, registration_date_key + interval '30' day promo_period

        from "emilia_gettdwh"."dwh_dim_drivers_v"
        where registration_date_key >= date'2020-08-01'
    ) as d

    left join "emilia_gettdwh"."dwh_fact_orders_v" fo on d.driver_gk = fo.driver_gk
        and lob_key in (5,6) and country_key = 2 and order_status_key in (7,4)
        and ordering_corporate_account_gk <> 20004730
        and fo.date_key <= date'2020-10-31'


    where d.fleet_gk in (200014202,200016265,200016266,
                         200016267,200016359,200016361)

    and d.promo_period >= date'2020-10-01'
    and registration_date_key <= date'2020-10-31'
    group by 1
)
group by 1



select count(distinct driver_gk) drivers,
count(distinct case when ftp_date_key = date'1900-01-01' then driver_gk  end ) no_ride

from emilia_gettdwh.dwh_dim_drivers_v
where fleet_gk in (200014202,200016265,200016266,
                    200016267,200016359,200016361)
and registration_date_key between date'2020-10-01' and date'2020-10-31'


-- TODO why numbers differ from riding drivers W47
select d.*, fl.vendor_name
from
emilia_gettdwh.dwh_dim_drivers_v d
left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
where driver_gk in (2000349767,	2000417653,	2000219684,	2000543405,	2000603124,
2000639342,	2000648387,	2000512326,	2000620207,	2000645769,
2000651668,	2000498075,	2000605775,	2000688514,	2000556355,
2000641405,	2000500156,	2000610638,	2000614957,	2000616644,	2000698977)




