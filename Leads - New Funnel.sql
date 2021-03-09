-- PASHA V FUNNEL  |
--                 V
select
l.*,
deliv.city_name,

sum(case when deliv.date_key
                    between l.registration_date_key and l.registration_date_key + interval '14' day
                then deliv.deliv end) deliveries_14days,
sum(deliv.deliv) deliveries_total,
sum(deliv.jorn) journeys_total,
sum(case when deliv.date_key
                    between l.registration_date_key and l.registration_date_key + interval '14' day
                then deliv.jorn end) journeys_14days,
count(distinct deliv.date_key) work_days_total,
count(distinct case when  deliv.date_key
                    between l.registration_date_key and l.registration_date_key + interval '14' day
                then deliv.deliv end) wdays_14days


from analyst.delivery_leads l
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
        and date_key >= date'2020-11-01'
        --and order_status_key = 7
        and fo.country_key = 2
        and driver_gk = 2000613275

        group by 1,2,3

    ) deliv on l.driver_gk = deliv.driver_gk

group by 1,2,3,4,5,6,7,8,9,10,11;


-- Funnel dossier
select *
from
(
select funnel.* ,
tp_a.subperiod2 activation_week,
tp_ftr.subperiod2 ftr_week
from
(
    (select
            distinct
            leads.phone_number lead_phone,
            "name" lead_name,
            leads.city lead_city,
            (case when "source" like '%web%' then 'Plan-net'
                    when "source" like '%workle%' then 'Workle' end) external_source,
            max(date(lead_date)) over (partition by leads.phone_number) lead_date,
            null driver_gk,
            null source,
            null phone,
            null registration_name,
            d.frozen_comment, d.is_frozen,
            null fleet_gk,
            null fleet_name,
            null registration_date_key,
            null reFTR,
            null FTR,
            null city_name,
            null deliv_total,
            null work_days,
            null is_wau

                                -- google sheet
            from sheets."default".delivery_courier_leads_new leads
                                -- get info about drivers by their phones
            LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d
            ON substring(d.phone, -10) = leads.phone_number
                and d.phone not in ('89999999999', '8', '')
                and country_key = 2

            where "source" <> 'source' --filter the bug that occured because of union of tables in google sheet
            and phone_2 <> 'phone_2'
            and phone_number not in ('8', '', '9999999999', ' ', '3333333333', '2222222222') -- dummy phones
            and phone_number is not null
            and cast(lead_date as date) >= date'2020-07-01'
            and d.driver_gk is null
    )

    union

    (
        select

        case when l.external_source is not null then l.phone_number end lead_phone,
        l.registration_name lead_name,
        null lead_city,
        l.external_source,
        l.external_source_lead_date lead_date,
        l.driver_gk,
        l.source,
        l.phone_number phone,
        l.registration_name,
        d.frozen_comment, d.is_frozen,
        l.fleet_gk,
        fl.vendor_name,
        l.registration_date_key,
        l.reftr is not null reFTR,
        (case when date(l.reftr) is not null then date(l.reftr) else l.first_ftr end) FTR,
        fo.city_name,
        (case when fo.of_deliv is null then 0 else fo.of_deliv end) +
        (case when fo.nf_deliv is null then 0 else fo.nf_deliv end) deliv_total,
        fo.work_days,
        fo.last_ride between current_date - interval '7' day and current_date is_wau


        from analyst.delivery_leads l
        left join emilia_gettdwh.dwh_dim_drivers_v d on l.driver_gk = d.driver_gk
        left join emilia_gettdwh.dwh_dim_vendors_v fl on fl.vendor_gk = l.fleet_gk
                  and fl.country_key = 2
        left join
                (
                    select
                    driver_gk, loc.city_name,
                    count(distinct case when ct.class_family <> 'Premium'
                             and ordering_corporate_account_gk <> 20004730 then order_gk end) of_deliv,
                    count(distinct case when ordering_corporate_account_gk = 20004730 then order_gk end) nf_deliv,
                    count(distinct date_key) work_days,
                    max(date_key) last_ride

                    from emilia_gettdwh.dwh_fact_orders_v fo
                    left join emilia_gettdwh.dwh_dim_locations_v loc on
                                fo.origin_location_key = loc.location_key and loc.country_id = 2
                    left join emilia_gettdwh.dwh_dim_class_types_v AS ct
                                ON ct.class_type_key = fo.class_type_key

                    where fo.country_key = 2 and fo.lob_key in (5,6)
                    and fo.date_key >= date'2020-07-1'
                    and order_status_key = 7

                    group by 1,2
                ) fo on l.driver_gk = fo.driver_gk
        )
) funnel
LEFT JOIN  emilia_gettdwh.periods_v AS tp_ftr ON tp_ftr.hour_key = 0 and tp_ftr.date_key = funnel.FTR
       and tp_ftr.timecategory = '3.Weeks'
       and tp_ftr.timecategory is not null
LEFT JOIN  emilia_gettdwh.periods_v AS tp_a ON tp_a.hour_key = 0 and tp_a.date_key = funnel.registration_date_key
       and tp_a.timecategory = '3.Weeks'
       and tp_a.timecategory is not null
)
where activation_week is null
and driver_gk is not null


select count(distinct driver_gk)
from analyst.delivery_leads
where first_ftr = date'2020-12-20'

count(distinct driver_gk)








-- MINI CRM
-- NO FTR in 3 days after registration date

select *

select
driver_gk,
substring (cast(driver_gk as varchar), 5) id,
registration_date_key,
phone_number phone,
"source",
vendor_name,
gt.name registration_name,
case when first_ftr is null then 'NO FTR' end status

from analyst.delivery_leads l
left join (select cast(id as varchar) id, name from "gt-ru".gettaxi_ru_production.drivers) gt
on  concat('2000', gt.id) = cast(l.driver_gk as varchar)
left join emilia_gettdwh.dwh_dim_vendors_v fl on l.fleet_gk = fl.vendor_gk

where registration_date_key = current_date - interval '1' day
and first_ftr is null
;

select
case when ftp_date_key is null then date'1900-01-01' else ftp_date_key end ftr

from  emilia_gettdwh.dwh_dim_drivers_v
where source_id = 918442
--tp_date_key = date'1900-1-1'
and country_key = 2
limit 2;

--- Agents Nth ride
with t as (
with orders as (
    with leads as (
        -- agents
        -- there is no information about CITY - future step, to take from FO
            select
            distinct substring(d.phone ,-10) phone_number, d.driver_name as "name",
            '0' as courier_details, '0' as request_id, 'Nan' as city,
            'agent' as "source", d.registration_date_key as "lead_date",
            cast(max(ftr.date_key) as date) ftp_date_key

            from emilia_gettdwh.dwh_dim_drivers_v d
            left join bp_ba.sm_ftr_reftr_drivers ftr on ftr.driver_gk = d.driver_gk

            where d.fleet_gk in (200014202,200016265,200016266,200016267,200016359,200016361)

            group by 1,2,3,4,5,6,7

        )

    (select d.driver_gk, "source",
    (case when fo.date_key >= dcln.ftp_date_key then fo.date_key end) date_key, lead_date,
    dcln.ftp_date_key, count(fo.order_gk) orders

    FROM leads dcln -- "dcln" is ald short name for sheet delivery_courier_leads_new
    LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON substring(d.phone, -10) = dcln.phone_number
                and dcln.phone_number <> '8'
                and country_key = 2

    left join emilia_gettdwh.dwh_fact_orders_v fo on fo.driver_gk = d.driver_gk
        and fo.country_key = 2 and fo.date_key >= date '2020-07-01' and lob_key in (5,6)
        and fo.ordering_corporate_account_gk <> 20004730

        where fo.lob_key in (5,6)
        and fo.order_status_key = 7
        group by 1,2,3,4,5)
    )

    (select driver_gk, date_key, "source", lead_date, ftp_date_key,
        sum(orders) over(partition by driver_gk order by date_key asc) Nth_ride
    from orders)
)
(select driver_gk, source, ftp_date_key, date_key, Nth_ride,
case when Nth_ride >= 5 and Nth_ride < 10 then date_diff('day', ftp_date_key, date_key) end days_5th_ride,
case when Nth_ride >= 10 and Nth_ride < 25 then date_diff('day', ftp_date_key, date_key) end days_10th_ride,
case when Nth_ride >= 25 and Nth_ride < 35 then date_diff('day', ftp_date_key, date_key) end days_25th_ride,
case when Nth_ride >= 35 then date_diff('day', ftp_date_key, date_key) end days_25th_ride

from t
where ftp_date_key>= date'2020-07-01');






-- CHURN
select
case when source_lable2 like '%web%' then 'Plan-net'
when source_lable2 like '%workle%' then 'workle' else source_lable2 end source,
count(distinct driver_gk) activated,
count(distinct case when churn = 'churn' then driver_gk end) churned,
(count(distinct case when churn = 'churn' then driver_gk end)*1.00 / count(distinct driver_gk)*1.00) * 100 churn_perc,
count(distinct case when churn = 'churn' and deliveries_30days / wdays_30days is null then driver_gk end) drivers_0,
count(distinct case when churn = 'churn' and deliveries_30days / wdays_30days between 1 and 3 then driver_gk end) drivers_1_3,
count(distinct case when churn = 'churn' and deliveries_30days / wdays_30days between 4 and 5 then driver_gk end) drivers_4_5,
count(distinct case when churn = 'churn' and deliveries_30days / wdays_30days between 6 and 10 then driver_gk end) drivers_6_10,
count(distinct case when churn = 'churn' and deliveries_30days / wdays_30days between 11 and 15 then driver_gk end) drivers_11_15,
count(distinct case when churn = 'churn' and deliveries_30days / wdays_30days >=16 then driver_gk end) drivers_16more

from (

with churn as
(
SELECT
'churn' churn,
ltp_date_key + interval '30' day AS churn_date,
ltp_date_key,
 driver_gk

FROM "emilia_gettdwh"."dwh_dim_drivers_v" dd
LEFT JOIN "emilia_gettdwh"."dwh_dim_vendors_v" v ON v.vendor_gk = dd.fleet_gk

WHERE vendor_name like '%courier%'
and dd.country_key = 2
and "ltp_date_key" between date '2020-01-01' and current_date - interval '30' day
--and registration_date_key >= date'2020-06-01'
)

, leads as
(
select
    distinct d.driver_gk,
            --d.phone phone_number,
            --d.driver_name registration_name,
            --prog.courier_details, prog.request_id, prog.city, -- info about leads from workle, website, etc

            (case when d.driver_gk = prog.driver_gk then prog.source end) external_source,
            d.fleet_gk,
            --fl.vendor_name,
            (case when d.driver_gk = ref.driver_gk then True else False end) is_reff,
            d.fleet_gk in (200014202,200016265,200016266,200016267,200016359,200016361) is_agent,
            d.fleet_gk = 200017083 is_scouts,

            d.registration_date_key, d.ftp_date_key,
            max(case when d.driver_gk = prog.driver_gk then date(lead_date) end) as "lead_date"

        from emilia_gettdwh.dwh_dim_drivers_v d
            -- to filter by fleet name selecting only couriers
            left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
            -- reff
            left join
            (
                select
                driver_gk, ftp_date_key between cast("start" as date) and cast("end" as date)
                from emilia_gettdwh.dwh_dim_drivers_v d
                left join "sheets"."default".ru_fleet_promo ref on cast(ref.fleet_gk as integer) = d.fleet_gk
                where 1=1
                -- select drivers who were led by reff
                and ftp_date_key between cast("start" as date) and cast("end" as date)
                and d.country_key = 2
            ) ref on ref.driver_gk = d.driver_gk

            -- external sources: workle, website etc. It's taken from GoogleSheet filled by Valera
            left join
            (
                    select distinct d.phone phone_number, "name",
                    d.driver_name registration_name,
                    d.fleet_gk, driver_gk,
                    vendor_name,
                    courier_details, request_id, --leads.city,
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

                    group by 1,2,3,4,5,6,7,8,9
            ) prog on prog.driver_gk = d.driver_gk


            where 1=1
            -- this string is logically correct but decrease number of active couriers among agents
            --and substring (d.phone, -10) not in ('', '3333333333', '2222222222')
            and d.phone is not null
            and d.driver_gk <> 2000683923 -- some old bug
            and fl.vendor_name like '%courier%'
            and d.country_key = 2
            --and d.ftp_date_key >= date'2020-07-01'
            group by 1,2,3,4,5,6,7,8

)

(
select
l.*, churn,
(case when l.is_agent = True then 'Gorizont'
        when l.is_reff = True then 'Reff'
        when l.is_scouts = True then 'Scouts'
        when l.external_source is not null then l.external_source
        else 'Fleet' end) source_lable2,


sum(case when deliv.date_key
                    between churn.ltp_date_key - interval '30' day and ltp_date_key
                then deliv.deliv end) deliveries_30days,

count(distinct case when  deliv.date_key
                    between churn.ltp_date_key - interval '30' day and ltp_date_key
                then deliv.deliv end) wdays_30days


from leads l
left join churn  on l.driver_gk = churn.driver_gk
left join --14 sec
(
select fo.date_key, fo.driver_gk,
orders + (case when deliveries is not null then deliveries else 0 end) deliv

from
  (
        --select count(distinct driver_gk) from (
        select
        distinct driver_gk,
        date_key,
        city_name,

        -- orders only on OF
        count(distinct case when ct.class_family <> 'Premium'
         and ordering_corporate_account_gk <> 20004730 then order_gk end) orders

        from emilia_gettdwh.dwh_fact_orders_v fo
        left join emilia_gettdwh.dwh_dim_class_types_v AS ct
            ON ct.class_type_key = fo.class_type_key
        left join emilia_gettdwh.dwh_dim_locations_v loc on
            fo.origin_location_key = loc.location_key and loc.country_id = 2

        where fo.lob_key in (5,6)
        and date_key >= date'2020-7-1'
        and order_status_key = 7
        and fo.country_key = 2

        group by 1,2,3
        --)

    ) fo

-- Deliveries NF
left join --2sec
    (
        select
        distinct courier_gk,
        date(scheduled_at) date_key,
        count(distinct delivery_gk) deliveries,
        count(distinct journey_gk) journeys

        from model_delivery.dwh_fact_deliveries_v

        where date(scheduled_at) >= date'2020-7-1'
        and delivery_status_id = 4
        and country_symbol = 'RU'

        group by 1,2

    ) md on md.courier_gk  = fo.driver_gk and md.date_key = fo.date_key

) deliv on l.driver_gk = deliv.driver_gk

group by 1,2,3,4,5,6,7,8,9,10
)
)
where registration_date_key >= date'2020-09-1'
group by 1;



-- churn DNMK activity groups
select
distinct case when  deliveries_30days / wdays_30days >=10 then driver_gk end driver,
registration_date_key, driver_name, phone,
deliveries_30days / wdays_30days orders_per_day,
ltp_date_key day_last_ride,
churn_week
-- count(distinct case when deliveries_30days / wdays_30days is null then driver_gk end) drivers_0,
-- count(distinct case when  deliveries_30days / wdays_30days between 1 and 3 then driver_gk end) drivers_1_3,
-- count(distinct case when  deliveries_30days / wdays_30days between 4 and 5 then driver_gk end) drivers_4_5,
-- count(distinct case when deliveries_30days / wdays_30days between 6 and 10 then driver_gk end) drivers_6_10,
-- count(distinct case when deliveries_30days / wdays_30days between 11 and 15 then driver_gk end) drivers_11_15,
-- count(distinct case when  deliveries_30days / wdays_30days >=16 then driver_gk end) drivers_16more

from
(
SELECT
distinct
dd.driver_gk, dd.phone, dd.driver_name,
ftp_date_key, ltp_date_key,
ltp_date_key + interval '30' day churn_date,
week(ltp_date_key + interval '30' day) AS churn_week,
registration_date_key,
sum(case when deliv.date_key
                    between ltp_date_key - interval '30' day and ltp_date_key
                then deliv.deliv end) deliveries_30days,

count(distinct case when  deliv.date_key
                    between ltp_date_key - interval '30' day and ltp_date_key
                then deliv.deliv end) wdays_30days



FROM "emilia_gettdwh"."dwh_dim_drivers_v" dd
LEFT JOIN "emilia_gettdwh"."dwh_dim_vendors_v" v ON v.vendor_gk = dd.fleet_gk

left join --14 sec
(
select fo.date_key, fo.driver_gk,
orders + (case when deliveries is not null then deliveries else 0 end) deliv

from
  (
        --select count(distinct driver_gk) from (
        select
        distinct driver_gk,
        date_key,
        city_name,

        -- orders only on OF
        count(distinct case when ct.class_family <> 'Premium'
         and ordering_corporate_account_gk <> 20004730 then order_gk end) orders

        from emilia_gettdwh.dwh_fact_orders_v fo
        left join emilia_gettdwh.dwh_dim_class_types_v AS ct
            ON ct.class_type_key = fo.class_type_key
        left join emilia_gettdwh.dwh_dim_locations_v loc on
            fo.origin_location_key = loc.location_key and loc.country_id = 2

        where fo.lob_key in (5,6)
        and date_key >= date'2020-7-1'
        and order_status_key = 7
        and fo.country_key = 2
        and driver_gk <> 200013

        group by 1,2,3
        --)

    ) fo

-- Deliveries NF
left join --2sec
    (
        select
        distinct courier_gk,
        date(scheduled_at) date_key,
        count(distinct delivery_gk) deliveries,
        count(distinct journey_gk) journeys

        from model_delivery.dwh_fact_deliveries_v

        where date(scheduled_at) >= date'2020-7-1'
        and delivery_status_id = 4
        and country_symbol = 'RU'
        and courier_gk <> 200013

        group by 1,2

    ) md on md.courier_gk  = fo.driver_gk and md.date_key = fo.date_key

) deliv on dd.driver_gk = deliv.driver_gk



WHERE vendor_name like '%courier%'
and dd.country_key = 2
and "ltp_date_key" between date '2020-01-01' and current_date - interval '30' day
and ftp_date_key >= date'2020-01-01'

group by 1,2,3,4,5,6,7,8

)
where churn_week in (46,47,48,49)
and deliveries_30days / wdays_30days >=10
--group by 1




