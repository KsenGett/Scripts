with leads as (
        (select
        distinct leads.phone_number lead_phone,
        "name" lead_name,
        leads.city lead_city,
        "source",
        d.driver_gk,
        d.phone,
        d.driver_name registration_name,
        d.fleet_gk,
        fl.vendor_name,
        fl.vendor_name like '%courier%' is_courier,
        d.registration_date_key,
        concat('W', cast(week(d.registration_date_key) as varchar)) week_cohort,
        max(case when ride_type = 'ReFTRD' then rftr.date_key end) reFTR,
        max(date(lead_date)) as lead_date,
        max(rftr.date_key) ftr_date
                            -- google sheet
        from sheets."default".delivery_courier_leads_new leads
                            -- get info about drivers by their phones
        LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d
        ON substring(d.phone, -10) = leads.phone_number
            and d.phone not in ('89999999999', '8', '')
            and country_key = 2
        left join bp_ba.sm_ftr_reftr_drivers rftr on d.driver_gk = rftr.driver_gk
        left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
        and fl.country_symbol = 'RU'

        where "source" <> 'source' --filter the bug that occured because of union of tables in google sheet
        and phone_2 <> 'phone_2'
        and phone_number not in ('8', '', '9999999999', ' ', '3333333333', '2222222222') -- dummy phones
        and phone_number is not null
        and cast(lead_date as date) >= date'2020-07-01'

        group by 1,2,3,4,5,6,7,8,9,10,11,12
        )

    union

        (select
                null as lead_phone,
                null as lead_name,
                 null as lead_city,
                (case when d.fleet_gk in (200014202,200016265,200016266,200016267,200016359,200016361) then 'Agent'
                        when d.fleet_gk = cast(ref.fleet_gk AS bigint) then 'Reff'
                        else 'Fleet' end)  source,
                d.driver_gk,
                d.phone,
                d.driver_name registration_name,
                d.fleet_gk,
                fl.vendor_name,
                fl.vendor_name like '%courier%' is_courier,
                d.registration_date_key,
                concat('W', cast(week(d.registration_date_key) as varchar)) week_cohort,
                max(case when ride_type = 'ReFTRD' then rftr.date_key end) reFTR,
                null as lead_date,
                max(rftr.date_key) ftr_date

            from emilia_gettdwh.dwh_dim_drivers_v d
                -- to filter by fleet name selecting only couriers
                left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
                -- reff - to learn original fleet
                left join "sheets"."default".ru_fleet_promo ref on fl.vendor_gk = cast(ref.fleet_gk as bigint)
                -- FTR
                left join bp_ba.sm_ftr_reftr_drivers rftr on d.driver_gk = rftr.driver_gk
                -- to exclude external sources
                left join
                --select count(distinct driver_gk) from
                    (
                        select
                        distinct  d.driver_gk

                        -- google sheet
                        from sheets."default".delivery_courier_leads_new leads
                        -- get info about drivers by their phones
                        JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d
                            ON substring(d.phone, -10) = leads.phone_number
                                and d.phone not in ('89999999999', '8', '')
                                and country_key = 2
                        -- to exclude reff
                        left join  "sheets"."default".ru_fleet_promo ref on d.fleet_gk = cast(ref.fleet_gk as bigint)

                        where "source" <> 'source' --filter the bug that occured because of union of tables in google sheet
                        and phone_2 <> 'phone_2'
                        and phone_number not in ('8', '', '9999999999', ' ', '3333333333', '2222222222') -- dummy phones
                        and phone_number is not null
                        and cast(lead_date as date) >= date'2020-07-01'
                        -- exclude agents
                        and d.fleet_gk not in (200014202,200016265,200016266,200016267,200016359,200016361)
                        -- exclude reff
                        and ref.fleet_gk is null
                        ) prog on prog.driver_gk = d.driver_gk

                where 1=1
                and d.phone is not null
                and d.driver_gk <> 2000683923 -- some old bug
                and fl.vendor_name like '%courier%'
                and d.country_key = 2
                --and d.ftp_date_key >= date'2020-07-01'
                -- exclude external sources
                and prog.driver_gk is null
                group by 1,2,3,4,5,6,7,8,9,10,11,12
            )
)
select l.*,
deliv.city_name,

sum(case when deliv.date_key
                    between l.registration_date_key and l.registration_date_key + interval '14' day
                then deliv.deliv end) deliveries_14days,
sum(deliv.deliv) deliveries_total,
count(distinct deliv.date_key) work_days_total,
count(distinct case when  deliv.date_key
                    between l.registration_date_key and l.registration_date_key + interval '14' day
                then deliv.deliv end) wdays_14days


from leads l

left join --14 sec
(
select fo.city_name, fo.date_key, fo.driver_gk,
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
        date(created_at) date_key,
        count(distinct delivery_gk) deliveries,
        count(distinct journey_gk) journeys

        from model_delivery.dwh_fact_deliveries_v

        where date(created_at) >= date'2020-7-1'
        and delivery_status_id = 4
        and country_symbol = 'RU'

        group by 1,2

    ) md on md.courier_gk  = fo.driver_gk and md.date_key = fo.date_key
) deliv on l.driver_gk = deliv.driver_gk


group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16;



select * from bp_ba.sm_ftr_reftr_drivers limit 10;




-- Conversion (to activated, ftr, 5 rides, N rides, work days, percentage of completion)
-- of week leads cohorts by sources
-- check CR and Work days

-- additional
-- Count number of leads for a period (count driver_gk over registration_date_key)
-- Track DNMK of FTR, reFTR

    with leads as (
        (select
        distinct leads.phone_number lead_phone,
        "name" lead_name,
        leads.city lead_city,
        "source",
        d.driver_gk,
        d.phone,
        d.driver_name registration_name,
        d.fleet_gk,
        d.registration_date_key,
        concat('W', cast(week(d.registration_date_key) as varchar)) week_cohort,
        (case when ride_type = 'ReFTRD' then rftr.date_key end) reFTR,
        max(date(lead_date)) as lead_date,
        max(rftr.date_key) ftr_date
                            -- google sheet
        from sheets."default".delivery_courier_leads_new leads
                            -- get info about drivers by their phones
        LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d
        ON substring(d.phone, -10) = leads.phone_number
            and d.phone not in ('89999999999', '8', '')
            and country_key = 2
        left join bp_ba.sm_ftr_reftr_drivers rftr on d.driver_gk = rftr.driver_gk

        where "source" <> 'source' --filter the bug that occured because of union of tables in google sheet
        and phone_2 <> 'phone_2'
        and phone_number not in ('8', '', '9999999999', ' ', '3333333333', '2222222222') -- dummy phones
        and phone_number is not null
        and cast(lead_date as date) >= date'2020-07-01'

        group by 1,2,3,4,5,6,7,8,9,10,11
        )

    union

        (select
                null as lead_phone,
                null as lead_name,
                 null as lead_city,
                (case when d.fleet_gk in (200014202,200016265,200016266,200016267,200016359,200016361) then 'Agent'
                        when d.fleet_gk = cast(ref.fleet_gk AS bigint) then 'Reff'
                        else 'Fleet' end)  source,
                d.driver_gk,
                d.phone,
                d.driver_name registration_name,
                d.fleet_gk,
                d.registration_date_key,
                concat('W', cast(week(d.registration_date_key) as varchar)) week_cohort,
                (case when ride_type = 'ReFTRD' then rftr.date_key end) reFTR,
                null as lead_date,
                max(rftr.date_key) ftr_date

            from emilia_gettdwh.dwh_dim_drivers_v d
                -- to filter by fleet name selecting only couriers
                left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
                -- reff - to learn original fleet
                left join "sheets"."default".ru_fleet_promo ref on fl.vendor_gk = cast(ref.fleet_gk as bigint)
                -- FTR
                left join bp_ba.sm_ftr_reftr_drivers rftr on d.driver_gk = rftr.driver_gk
                -- to exclude external sources
                left join
                --select count(distinct driver_gk) from
                    (
                        select
                        distinct  d.driver_gk

                        -- google sheet
                        from sheets."default".delivery_courier_leads_new leads
                        -- get info about drivers by their phones
                        JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d
                            ON substring(d.phone, -10) = leads.phone_number
                                and d.phone not in ('89999999999', '8', '')
                                and country_key = 2
                        -- to exclude reff
                        left join  "sheets"."default".ru_fleet_promo ref on d.fleet_gk = cast(ref.fleet_gk as bigint)

                        where "source" <> 'source' --filter the bug that occured because of union of tables in google sheet
                        and phone_2 <> 'phone_2'
                        and phone_number not in ('8', '', '9999999999', ' ', '3333333333', '2222222222') -- dummy phones
                        and phone_number is not null
                        and cast(lead_date as date) >= date'2020-07-01'
                        -- exclude agents
                        and d.fleet_gk not in (200014202,200016265,200016266,200016267,200016359,200016361)
                        -- exclude reff
                        and ref.fleet_gk is null
                        ) prog on prog.driver_gk = d.driver_gk

                where 1=1
                and d.phone is not null
                and d.driver_gk <> 2000683923 -- some old bug
                and fl.vendor_name like '%courier%'
                and d.country_key = 2
                and d.registration_date_key >= date'2020-07-01'
                -- exclude external sources
                and prog.driver_gk is null
                group by 1,2,3,4,5,6,7,8,9,10,11
            )
)
select l.*,
loc.city_name,

count(distinct case when fo.date_key
                    between l.registration_date_key and l.registration_date_key + interval '7' day
                then fo.order_gk end) journeys_7days,
count(distinct case when order_status_key = 7 and fo.date_key
                    between l.registration_date_key and l.registration_date_key + interval '7' day
                then fo.order_gk end) journeys_7days_compl,

count(distinct case when fo.date_key
                    between l.registration_date_key and l.registration_date_key + interval '14' day
                then fo.order_gk end) journeys_14days,
count(distinct case when order_status_key = 7 and fo.date_key
                    between l.registration_date_key and l.registration_date_key + interval '14' day
                then fo.order_gk end) journeys_14days_compl,

count(distinct date_key) work_days_totall,
count(distinct case when fo.date_key
                    between l.registration_date_key and l.registration_date_key + interval '14' day
                then fo.date_key end) wdays_14days


from leads l
left join emilia_gettdwh.dwh_fact_orders_v fo on l.driver_gk = fo.driver_gk
    and fo.country_key = 2 and lob_key in (5,6)
    and ordering_corporate_account_gk <> 20004730
left join emilia_gettdwh.dwh_dim_locations_v loc on
            fo.origin_location_key = loc.location_key and loc.country_id = 2

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14;





select
d.fleet_gk, fl.vendor_name,
ref.city fleet_city,
d.driver_gk,
Documents_signed,
Target,
ftp_date_key,
"registration_date_key",
"registration_date_key" between date(Documents_signed) and date(Documents_signed) + interval '14' day within_2weeks_afterRegistration,
"ftp_date_key" between date(Documents_signed) and date(Documents_signed) + interval '14' day within_2weeks_afterFTR,
"ftp_date_key",
sum(case when deliv.date_key between "ftp_date_key" AND "ftp_date_key" + interval '14' day then deliv.deliv end) deliv_14days_after_ftr

from emilia_gettdwh.dwh_dim_drivers_v d
                -- to filter by fleet name selecting only couriers
left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
                -- reff - to learn original fleet
join "sheets"."default".ru_fleet_promo ref on fl.vendor_gk = cast(ref.fleet_gk as bigint)
left join --deliv
(
    select fo.date_key, fo.driver_gk, fo.fleet_gk,
    orders + (case when deliveries is not null then deliveries else 0 end) deliv

    from
      (
            --select count(distinct driver_gk) from (
            select
            distinct driver_gk,
            date_key,
            city_name,
            fleet_gk,

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

            group by 1,2,3,4
            --)

        ) fo

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

) deliv on d.driver_gk = deliv.driver_gk

where d.ftp_date_key >= date '2020-09-28'

group by 1,2,3,4,5,6,7,8,9,10
;

SELECT d."fleet_gk",
         taxi_station_id,
         vendor_name AS fleet_name,
         city AS fleet_city,
         Documents_signed,
         Target,
         ftp_date_key,
         driver_gk,
         driver_name,
         "registration_date_key",
         "ftp_date_key",
         "ltp_date_key",
         "number_of_days_online",
         number_of_rides AS orders_num
          FROM "emilia_gettdwh"."dwh_dim_drivers_v" d
         inner JOIN sheets."default".ru_fleet_promo p ON d."fleet_gk" = cast(p.fleet_gk AS integer)
         and ftp_date_key >= date '2020-09-28';



-- MINI CRM
-- NO FTR in 3 days after registration date

select *

from(

with leads as (
        (
        select
        distinct leads.phone_number lead_phone,
        leads."name" lead_name,
        leads.city lead_city,
        "source",
        d.driver_gk, d.source_id id,
        d.phone,
        gt.name registration_name,
        d.fleet_gk,
        d.registration_date_key,
        concat('W', cast(week(d.registration_date_key) as varchar)) week_cohort,
        (case when ride_type = 'ReFTRD' then rftr.date_key end) reFTR,
        max(date(lead_date)) as lead_date,
        max(rftr.date_key) ftr_date
                            -- google sheet
        from sheets."default".delivery_courier_leads_new leads
                            -- get info about drivers by their phones
        LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d
        ON substring(d.phone, -10) = leads.phone_number
            and d.phone not in ('89999999999', '8', '')
            and country_key = 2
        left join bp_ba.sm_ftr_reftr_drivers rftr on d.driver_gk = rftr.driver_gk
        -- full name
        left join (select id, name from "gt-ru".gettaxi_ru_production.drivers) gt on  d.source_id = gt.id

        where "source" <> 'source' --filter the bug that occured because of union of tables in google sheet
        and phone_2 <> 'phone_2'
        and phone_number not in ('8', '', '9999999999', ' ', '3333333333', '2222222222') -- dummy phones
        and phone_number is not null
        and cast(lead_date as date) >= date'2020-07-01'

        group by 1,2,3,4,5,6,7,8,9,10,11,12
        )

    union

        (select
                null as lead_phone,
                null as lead_name,
                 null as lead_city,
                (case when d.fleet_gk in (200014202,200016265,200016266,200016267,200016359,200016361) then 'Agent'
                        when d.fleet_gk = cast(ref.fleet_gk AS bigint) then 'Reff'
                        else 'Fleet' end)  source,
                d.driver_gk, d.source_id id,
                d.phone,
                gt.name registration_name,
                d.fleet_gk,
                d.registration_date_key,
                concat('W', cast(week(d.registration_date_key) as varchar)) week_cohort,
                (case when ride_type = 'ReFTRD' then rftr.date_key end) reFTR,
                null as lead_date,
                max(rftr.date_key) ftr_date

            from emilia_gettdwh.dwh_dim_drivers_v d
                -- to filter by fleet name selecting only couriers
                left join  emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
                -- reff - to learn original fleet
                left join "sheets"."default".ru_fleet_promo ref on fl.vendor_gk = cast(ref.fleet_gk as bigint)
                -- FTR
                left join bp_ba.sm_ftr_reftr_drivers rftr on d.driver_gk = rftr.driver_gk
                -- full name
                left join (select id, name from "gt-ru".gettaxi_ru_production.drivers) gt on  d.source_id = gt.id
                -- to exclude external sources
                left join
                --select count(distinct driver_gk) from
                    (
                        select
                        distinct  d.driver_gk

                        -- google sheet
                        from sheets."default".delivery_courier_leads_new leads
                        -- get info about drivers by their phones
                        JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d
                            ON substring(d.phone, -10) = leads.phone_number
                                and d.phone not in ('89999999999', '8', '')
                                and country_key = 2
                        -- to exclude reff
                        left join  "sheets"."default".ru_fleet_promo ref on d.fleet_gk = cast(ref.fleet_gk as bigint)

                        where "source" <> 'source' --filter the bug that occured because of union of tables in google sheet
                        and phone_2 <> 'phone_2'
                        and phone_number not in ('8', '', '9999999999', ' ', '3333333333', '2222222222') -- dummy phones
                        and phone_number is not null
                        and cast(lead_date as date) >= date'2020-07-01'
                        -- exclude agents
                        and d.fleet_gk not in (200014202,200016265,200016266,200016267,200016359,200016361)
                        -- exclude reff
                        and ref.fleet_gk is null
                        ) prog on prog.driver_gk = d.driver_gk

                where 1=1
                and d.phone is not null
                and d.driver_gk <> 2000683923 -- some old bug
                and fl.vendor_name like '%courier%'
                and d.country_key = 2
                and d.registration_date_key >= date'2020-07-01'
                -- exclude external sources
                and prog.driver_gk is null
                group by 1,2,3,4,5,6,7,8,9,10,11,12
            )
)
select l.*,

case when
registration_date_key = current_date - interval '3' day
and ftr_date is null then 'NO FTR'
when lead_date = current_date - interval '3' day
and registration_date_key is null then 'NOT Registered' end status


from leads l
)
where status is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
;

select * from "gt-ru".gettaxi_ru_production.drivers limit 20;

desc bp_ba.sm_ftr_reftr_drivers

select driver_gk, max(ftp_date_key) from bp_ba.sm_ftr_reftr_drivers
where ftp_date_key is not null
group by 1

select driver_gk, max(ftp_date_key) ftr

from bp_ba.sm_ftr_reftr_drivers

where driver_gk in (2000287460,2000394260,2000697993)

group by 1;



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
