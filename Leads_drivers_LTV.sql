
--from (
--1. Get the list of courier driver_gk who registered since 2020-07-01
------- detect external sources - workle, Plan-net
with leads as (
    --select count( distinct driver_gk) from (
    select
    distinct d.driver_gk,
            d.phone phone_number,
            d.driver_name registration_name,
            prog.courier_details, prog.request_id, prog.city, -- info about leads from workle, website, etc

            (case when d.driver_gk = prog.driver_gk then prog.source end) external_source,
            d.fleet_gk,

            (case when d.ftp_date_key between cast(ref."start" as date) and cast(ref."end" as date)
            then True else False end) is_reff,
            d.fleet_gk in (200014202,200016265,200016266,200016267,200016359,200016361) is_agent,

            d.registration_date_key, d.ftp_date_key,
            max(case when d.driver_gk = prog.driver_gk then date(lead_date) end) as "lead_date"

        from emilia_gettdwh.dwh_dim_drivers_v d
            -- to filter by fleet name selecting only couriers
            left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
            -- reff - to learn original fleet
            left join "sheets"."default".ru_fleet_promo ref on fl.vendor_gk = cast(ref.fleet_gk as bigint)

            -- external sources: workle, website etc. It's taken from GoogleSheet filled by Valera
            left join (
                    select distinct d.phone phone_number, "name",
                    d.driver_name registration_name,
                    d.fleet_gk, driver_gk,
                    vendor_name,
                    courier_details, request_id, leads.city,
                    "source",
                    max(date(lead_date)) as lead_date

                    -- google sheet
                    from sheets."default".delivery_courier_leads_new leads
                    -- get info about drivers by their phones
                    LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d
                        ON substring(d.phone, -10) = leads.phone_number
                            and d.phone not in ('89999999999', '8', '')
                            and country_key = 2
                    left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk

                    where "source" <> 'source' --filter the bug that occured because of union of tables in google sheet
                    and phone_2 <> 'phone_2'
                    and phone_number not in ('8', '', '9999999999', ' ', '3333333333', '2222222222') -- dummy phones
                    and phone_number is not null
                    and cast(lead_date as date) >= date'2020-07-01'

                    group by 1,2,3,4,5,6,7,8,9,10
                    ) prog on prog.driver_gk = d.driver_gk


            where 1=1
            -- this string is logically correct but decrease number of active couriers among agents
            --and substring (d.phone, -10) not in ('', '3333333333', '2222222222')
            and d.phone is not null
            and d.driver_gk <> 2000683923 -- some old bug
            and fl.vendor_name like '%courier%'
            and d.country_key = 2
            and d.ftp_date_key >= date'2020-07-01'
            group by 1,2,3,4,5,6,7,8,9,10,11,12
           --) where is_reff = True
            )

--2. Get activity information about drivers on the date_key level - TR, deliveries
------ determinate agents and reff drivers (on the level of the day as they can change fleets)
select
-- driver info + date key
distinct l.driver_gk,
l.registration_name,
l.phone_number,
fo.date_key,
tp.timecategory,
tp.period,
tp.subperiod,
        --which fleet was that day + fox bug with fleet -1 for orders
(case when fo.fleet_gk = -1 then l.fleet_gk else fo.fleet_gk end) fleet_gk,
fl.vendor_name,

-- raw agents & reff leads, not considering whether they've done ftr or not
l.is_reff is_reff_original,
l.is_agent is_agent_original,

--determination of the source by factual activity
(case when fo.fleet_gk = -1 then l.fleet_gk else fo.fleet_gk end) = cast(ref.fleet_gk AS bigint) is_reff,
(case when fo.fleet_gk = -1 then l.fleet_gk else fo.fleet_gk end) in (200014202,200016265,200016266,200016267,200016359,200016361) is_agent,
l.registration_date_key, -- date of registration in our system
l.lead_date, -- for external source
l.external_source,
l.courier_details, l.request_id, l.city, --external source information
l.registration_date_key >= l.lead_date in_our_system_first,

-- activity
fo.region_name,
fo.TR,
fo.orders deliveries_OF,
md.deliveries deliveries_NF,
md.journeys journeys_NF,
l.ftp_date_key, -- from dim drivers

count(fo.date_key) over(partition by l.driver_gk) LT,
min(fo.date_key) over(partition by l.driver_gk) FTR,
max(fo.date_key) over(partition by l.driver_gk) last_date,

-- source lable
(case when (l.is_agent = True and l.external_source is null) or (l.registration_date_key <= l.lead_date and l.is_agent = True ) then 'Agent'
        when (l.is_reff = True and l.external_source is null) or (l.registration_date_key <= l.lead_date and l.is_reff = True) then 'Reff'
        when l.external_source is not null then l.external_source
        else 'Fleet' end) source_lable,

(case when l.is_agent = True then 'Agent'
        when l.is_reff = True then 'Reff'
        when l.external_source is not null then l.external_source
        else 'Fleet' end) source_lable2


from leads l

-- Deliveries OF, region name, TR, last_date, FTR
left join --14 sec
  (
        --select count(distinct driver_gk) from (
        select
        distinct driver_gk,
        date_key,
        region_name,
        fo.fleet_gk,
        --finance
        (sum(customer_total_cost) - sum(driver_total_cost_inc_vat))
        + (sum(driver_total_commission_exc_vat)*(-1)) TR,
        --count(distinct case when order_status_key = 7 then date_key end) LT_days,
        -- ftr on any platform
        --min(date_key) ftr,

        -- orders only on OF
        count(distinct case when ct.class_family <> 'Premium'
         and ordering_corporate_account_gk <> 20004730 then order_gk end) orders

        from emilia_gettdwh.dwh_fact_orders_v fo
        left join emilia_gettdwh.dwh_dim_class_types_v AS ct
            ON ct.class_type_key = fo.class_type_key
        left join emilia_gettdwh.dwh_dim_locations_v loc on
            fo.origin_location_key = loc.location_key and loc.country_id = 2

        where fo.lob_key in (5,6)
        and date_key >= date'2020-11-01'
        and order_status_key = 7
        and fo.country_key = 2
        
--         ---TODO Check
--         and driver_gk in (2000837187,2000837478,2000842055,2000843852,2000844651)
--         and date_key >= date'2020-10-01'

        --and fo.fleet_gk in (200014202,200016265,200016266,200016267,200016359,200016361)
        group by 1,2,3,4
        --)

    ) fo on fo.driver_gk = l.driver_gk

-- Deliveries NF
left join --2sec
    (
        select
        distinct courier_gk,
        date(created_at) date_key,
        count(distinct delivery_gk) deliveries,
        count(distinct journey_gk) journeys

        from model_delivery.dwh_fact_deliveries_v

        where
        date(created_at) >= date'2020-11-01'
        and delivery_status_id = 4

        group by 1,2

    ) md on md.courier_gk  = fo.driver_gk and md.date_key = fo.date_key

-- timecategory
LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.hour_key = 0 and tp.date_key = fo.date_key
       and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
       and tp.timecategory is not null


-- reff
left join "sheets"."default".ru_fleet_promo ref on fo.fleet_gk = cast(ref.fleet_gk as bigint)
-- fleet names
left join emilia_gettdwh.dwh_dim_vendors_v fl on fo.fleet_gk = fl.vendor_gk
;




--group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18;


-- TODO check number of agents with dim_drivers/Fo. - DONE = 841 with strange phone numbers and
--  834 without, raw number of agent leads = 1444

-- TODO check number of reff = 3154 leads, 1942 working
--Agent original = 756 who registered in oct, and 403 who worked















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




