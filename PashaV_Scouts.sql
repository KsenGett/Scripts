-- Scouts - agents, their payment is based on number of JOURNEYS within 30 days after FTR (reFTR included)
with leads as
        (
        select
                'Scouts' source,
                d.driver_gk,
                d.phone,
                d.driver_name registration_name,
                d.fleet_gk,
                fl.vendor_name,
                d.registration_date_key,
                (case when is_frozen = 1 then 'Заблокирован' else 'Активен' end) status,
                (case when d.ltp_date_key <> date'1900-01-01' then d.ltp_date_key end) last_ride_date,
                max(case when ride_type = 'ReFTRD' then rftr.date_key end) reFTR,
                max(rftr.date_key) ftr_date

            from emilia_gettdwh.dwh_dim_drivers_v d
                -- to filter by fleet name selecting only couriers
                left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
                -- FTR
                left join bp_ba.sm_ftr_reftr_drivers rftr on d.driver_gk = rftr.driver_gk


                where 1=1
                and d.phone is not null
                and d.driver_gk <> 2000683923 -- some old bug
                and d.country_key = 2
                and d.fleet_gk = 200017083 -- scouts
                --and d.registration_date_key >= date'2020-07-01'
                group by 1,2,3,4,5,6,7,8,9

        )
select l.*,
date(l.ftr_date) + interval '30' day stop_promo_date,
deliv.city_name,
sum(case when deliv.date_key
                    between date(l.ftr_date) and date(l.ftr_date) + interval '30' day
                then deliv.jorn end) journeys_30days,
sum(case when deliv.date_key > date(l.ftr_date) + interval '30' day
                then deliv.jorn end) journeys_after_promo,
sum(deliv.jorn) journeys_total,
min(case when Nth_ride >= 5 then date_key end) date_5th_jorn,
count(distinct deliv.date_key) work_days_total,

sum(case when deliv.date_key
                    between date(l.ftr_date) and date(l.ftr_date) + interval '30' day
                then deliv.deliv end) deliveries_30days,
sum(case when deliv.date_key > date(l.ftr_date) + interval '30' day
                then deliv.deliv end) deliveries_after_promo,
sum(deliv.deliv) deliveries_total


from leads l

left join --14 sec
(
select fo.city_name, fo.date_key, fo.driver_gk,
orders + (case when deliveries is not null then deliveries else 0 end) deliv,
orders + (case when journeys is not null then journeys else 0 end) jorn,
sum(orders + (case when journeys is not null then journeys else 0 end) )
over(partition by fo.driver_gk order by fo.date_key asc) Nth_ride


from
  (
        --select count(distinct driver_gk) from (
        select
        distinct fo.driver_gk,
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
        left join emilia_gettdwh.dwh_dim_drivers_v d on fo.driver_gk = d.driver_gk
            and d.fleet_gk = 200017083

        where fo.lob_key in (5,6)
        and date_key >= date'2020-7-1'
        and order_status_key = 7
        and fo.country_key = 2
        and d.fleet_gk = 200017083 -- scouts only

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

        from model_delivery.dwh_fact_deliveries_v fd
        left join emilia_gettdwh.dwh_dim_drivers_v d on fd.courier_gk = d.driver_gk
            and d.fleet_gk = 200017083

        where date(scheduled_at) >= date'2020-7-1'
        and delivery_status_id = 4
        and fd.country_symbol = 'RU'
        and d.fleet_gk = 200017083 -- scouts only

        group by 1,2

    ) md on md.courier_gk  = fo.driver_gk and md.date_key = fo.date_key
) deliv on l.driver_gk = deliv.driver_gk

group by 1,2,3,4,5,6,7,8,9,10,11,12,13;


-- Reff - their payment is based on number of DELIVERIES within 14 days after FTR (no reFTR included)
with leads as
        (
        select
                'Reff' source,
                d.driver_gk,
                d.phone,
                d.driver_name registration_name,
                d.fleet_gk,
                fl.vendor_name,
                d.registration_date_key,
                (case when is_frozen = 1 then 'Заблокирован' else 'Активен' end) status,
                (case when d.ltp_date_key <> date'1900-01-01' then d.ltp_date_key end) last_ride_date,
                ftp_date_key ftr_date,
                programme

            from emilia_gettdwh.dwh_dim_drivers_v d
                -- to filter by fleet name selecting only couriers
                left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
                -- reff
                left join "sheets"."default".ru_fleet_promo ref
                on cast(ref.fleet_gk as integer) = d.fleet_gk


                where 1=1
                and d.phone is not null
                and d.country_key = 2
                and ftp_date_key between cast("start" as date) and cast("end" as date)

        )
select l.*,
date(l.ftr_date) + interval '14' day stop_promo_date,
deliv.city_name,
sum(case when deliv.date_key
                    between date(l.ftr_date) and date(l.ftr_date) + interval '14' day
                then deliv.jorn end) journeys_14days,
sum(case when deliv.date_key > date(l.ftr_date) + interval '14' day
                then deliv.jorn end) journeys_after_promo,
sum(deliv.jorn) journeys_total,
count(distinct deliv.date_key) work_days_total,

sum(case when deliv.date_key
                    between date(l.ftr_date) and date(l.ftr_date) + interval '14' day
                then deliv.deliv end) deliveries_14days,
sum(case when deliv.date_key > date(l.ftr_date) + interval '14' day
                then deliv.deliv end) deliveries_after_promo,
sum(deliv.deliv) deliveries_total


from leads l

left join --14 sec
(
select fo.city_name, fo.date_key, fo.driver_gk,
orders + (case when deliveries is not null then deliveries else 0 end) deliv,
orders + (case when journeys is not null then journeys else 0 end) jorn

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

group by 1,2,3,4,5,6,7,8,9,10,11,12,13;

-- Департамент  успеха - agents, their payment is based on number of JOURNEYS within 30 days after FTR (reFTR included)
with leads as
        (
        select
                'D_Uspeha' source,
                d.driver_gk,
                d.phone,
                d.driver_name registration_name,
                d.fleet_gk,
                fl.vendor_name,
                d.registration_date_key,
                (case when is_frozen = 1 then 'Заблокирован' else 'Активен' end) status,
                (case when d.ltp_date_key <> date'1900-01-01' then d.ltp_date_key end) last_ride_date,
                max(case when ride_type = 'ReFTRD' then rftr.date_key end) reFTR,
                max(rftr.date_key) ftr_date

            from emilia_gettdwh.dwh_dim_drivers_v d
                -- to filter by fleet name selecting only couriers
                left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
                -- FTR
                left join bp_ba.sm_ftr_reftr_drivers rftr on d.driver_gk = rftr.driver_gk


                where 1=1
                and d.phone is not null
                and d.driver_gk <> 2000683923 -- some old bug
                and d.country_key = 2
                and d.fleet_gk = 200017111
                --and d.registration_date_key >= date'2020-07-01'
                group by 1,2,3,4,5,6,7,8,9

        )
select l.*,
date(l.ftr_date) + interval '30' day stop_promo_date,
deliv.city_name,
sum(case when deliv.date_key
                    between date(l.ftr_date) and date(l.ftr_date) + interval '30' day
                then deliv.jorn end) journeys_30days,
sum(case when deliv.date_key > date(l.ftr_date) + interval '30' day
                then deliv.jorn end) journeys_after_promo,
sum(deliv.jorn) journeys_total,
count(distinct deliv.date_key) work_days_total,

sum(case when deliv.date_key
                    between date(l.ftr_date) and date(l.ftr_date) + interval '30' day
                then deliv.deliv end) deliveries_30days,
sum(case when deliv.date_key > date(l.ftr_date) + interval '30' day
                then deliv.deliv end) deliveries_after_promo,
sum(deliv.deliv) deliveries_total


from leads l

left join --14 sec
(
select fo.city_name, fo.date_key, fo.driver_gk,
orders + (case when deliveries is not null then deliveries else 0 end) deliv,
orders + (case when journeys is not null then journeys else 0 end) jorn

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

group by 1,2,3,4,5,6,7,8,9,10,11,12,13;
