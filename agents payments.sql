
with new_drivers as
            (
            select
                ftr.driver_gk,
                dd.phone as driver_phone,
                dd.fleet_gk as ftr_fleet,
                fl.vendor_name ftr_fleet_name,
                substring(fl.vendor_name, 1, position('/' in fl.vendor_name)-1) as ftr_fleet_name_short, --delete supply type
                cast(ftr.date_key as date) as ftr_date,
                ftr.ride_type,
                date_add('month', 1, cast(ftr.date_key as date)) ftr_date_plus_1_month

            from bp_ba.sm_ftr_reftr_drivers ftr
            left join emilia_gettdwh.dwh_dim_drivers_v dd on ftr.driver_gk = dd.driver_gk
            left join emilia_gettdwh.dwh_dim_vendors_v fl on ftr.fleet_gk = fl.vendor_gk and fl.country_key = 2

            where true
                --and dd.fleet_gk in (200016266)
                and cast(ftr.date_key as date) >= date '2021-3-1'
                and ftr.driver_gk in ()

            )
    , stat_first_month as
        (
          select
          cast(coalesce(substring(cast(p.driver_gk as varchar),5),'0') as varchar) driver_id,
          p.driver_gk,
          fnr.driver_phone,
          fnr.ftr_fleet, fnr.ftr_fleet_name_short, fnr.ftr_fleet_name,
          fnr.ride_type,
          fnr.ftr_date,
          fnr.ftr_date_plus_1_month,
          sum(p.jorn) as rides_1st_month,
                    sum (case when p.date_key
                  between date_add('day', -1, fnr.ftr_date) and date_add('day', 6, fnr.ftr_date) then jorn else null end) as rides_7_days,
          sum (case when p.date_key
                  between date_add('day', -1, fnr.ftr_date) and date_add('day', 14, fnr.ftr_date) then jorn else null end) as rides_14_days,
          sum (case when p.date_key
                  between date_add('day', -1, fnr.ftr_date) and date_add('day', 20, fnr.ftr_date) then jorn else null end) as rides_20_days,
          sum (case when p.date_key
                  between date_add('day', -1, fnr.ftr_date) and date_add('day', 30, fnr.ftr_date) then jorn else null end) as rides_30_days

          from (
          select fo.city_name, fo.date_key, fo.driver_gk,
            coalesce(orders,0) + coalesce(deliveries,0) deliv,
            coalesce(orders,0) + coalesce(journeys,0) jorn

          from
              (
                    --select count(distinct driver_gk) from (
                    select
                    fo.driver_gk,
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

                    where fo.lob_key in (5,6)
                    and date_key >= date'2020-10-1'
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

                    from model_delivery.dwh_fact_deliveries_v fd
                    left join emilia_gettdwh.dwh_dim_drivers_v d on fd.courier_gk = d.driver_gk

                    where date(scheduled_at) >= date'2020-10-1'
                    and delivery_status_id = 4
                    and fd.country_symbol = 'RU'

                    group by 1,2

                ) md on md.courier_gk  = fo.driver_gk and md.date_key = fo.date_key
            ) p

        left join new_drivers fnr on fnr.driver_gk  = p.driver_gk
                        and p.date_key
                        between date_add('day', -1, fnr.ftr_date) and fnr.ftr_date_plus_1_month

        group by 1,2,3,4,5,6,7,8,9

        )
, rides as (
    select
    st.*,
    --case when week (cast (p.scheduled_at + interval '3' hour  as date)) in (1,2,3,4,5,6)
        --then week (cast (p.scheduled_at + interval '3' hour  as date)) + 53
        --else week (cast (p.scheduled_at + interval '3' hour  as date)) end as weeks,
    year (p.date_key) as years,
    week (p.date_key) as week,
    sum (deliv) as rides_this_week

    from (
          select fo.city_name, fo.date_key, fo.driver_gk,
            coalesce(orders,0) + coalesce(deliveries,0) deliv,
            coalesce(orders,0) + coalesce(journeys,0) jorn

          from
              (
                    --select count(distinct driver_gk) from (
                    select
                    fo.driver_gk,
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

                    where fo.lob_key in (5,6)
                    and date_key >= date'2020-10-1'
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

                    from model_delivery.dwh_fact_deliveries_v fd
                    left join emilia_gettdwh.dwh_dim_drivers_v d on fd.courier_gk = d.driver_gk

                    where date(scheduled_at) >= date'2020-10-1'
                    and delivery_status_id = 4
                    and fd.country_symbol = 'RU'

                    group by 1,2

                ) md on md.courier_gk  = fo.driver_gk and md.date_key = fo.date_key
            ) p

    join stat_first_month st on p.driver_gk = st.driver_gk
          and p.date_key between date_add('day', -1, st.ftr_date) and st.ftr_date_plus_1_month

    where true
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
        )
(select
    r.*,
    row_number () over (partition by r.driver_id order by r.driver_id, r.week) week_num,
    sum (r.rides_this_week) over (partition by r.ftr_fleet, r.driver_id,
    r.ftr_date order by week asc rows between unbounded preceding and current row) as cumsum

from rides r
where true
order by r.driver_id, r.week
)


-- old script with fixed fleet

        with new_drivers as
            (
            select
                ftr.driver_gk,
                dd.phone as driver_phone,
                dd.fleet_gk as ftr_fleet,
                fl.vendor_name ftr_fleet_name,
                substring(fl.vendor_name, 1, position('/' in fl.vendor_name)-1) as ftr_fleet_name_short, --delete supply type
                cast(ftr.date_key as date) as ftr_date,
                ftr.ride_type,
                date_add('month', 1, cast(ftr.date_key as date)) ftr_date_plus_1_month

            from bp_ba.sm_ftr_reftr_drivers ftr
            left join emilia_gettdwh.dwh_dim_drivers_v dd on ftr.driver_gk = dd.driver_gk
            left join emilia_gettdwh.dwh_dim_vendors_v fl on dd.fleet_gk = fl.vendor_gk and fl.country_key = 2

            where true
                and dd.fleet_gk in ({fleets})
                and cast(ftr.date_key as date) >= date '{date_start}'
            )
    , stat_first_month as
        (
          select
          p.driver_id,
          fnr.driver_phone,
          fnr.ftr_fleet, fnr.ftr_fleet_name_short, fnr.ftr_fleet_name,
          fnr.ride_type,
          fnr.ftr_date,
          fnr.ftr_date_plus_1_month,
          count (*) as rides_1st_month,
                    count (case when cast (p.scheduled_at + interval '3' hour  as date)
                  between date_add('day', -1, fnr.ftr_date) and date_add('day', 6, fnr.ftr_date) then 1 else null end) as rides_7_days,
          count (case when cast (p.scheduled_at + interval '3' hour  as date)
                  between date_add('day', -1, fnr.ftr_date) and date_add('day', 14, fnr.ftr_date) then 1 else null end) as rides_14_days,
          count (case when cast (p.scheduled_at + interval '3' hour  as date)
                  between date_add('day', -1, fnr.ftr_date) and date_add('day', 20, fnr.ftr_date) then 1 else null end) as rides_20_days,
          count (case when cast (p.scheduled_at + interval '3' hour  as date)
                  between date_add('day', -1, fnr.ftr_date) and date_add('day', 30, fnr.ftr_date) then 1 else null end) as rides_30_days

          from gettaxi_ru_production.orders p
          join new_drivers fnr on fnr.driver_gk  = CAST ('2000' || cast (p.driver_id as varchar) AS BIGINT)
                        and cast (p.scheduled_at + interval '3' hour  as date)
                        between date_add('day', -1, fnr.ftr_date) and fnr.ftr_date_plus_1_month
            --join emilia_gettdwh.dwh_dim_class_types_v dct on cast ((SUBSTRING(cast (dct.class_type_key as varchar), 5)) as integer)= p.division_id
          where true
            and p.status_id = 7
            and (p.company_id != 4730 or p.company_id is null)
            group by 1,2,3,4,5,6,7,8
        )
, rides as (
    select
    st.*,
    --case when week (cast (p.scheduled_at + interval '3' hour  as date)) in (1,2,3,4,5,6)
        --then week (cast (p.scheduled_at + interval '3' hour  as date)) + 53
        --else week (cast (p.scheduled_at + interval '3' hour  as date)) end as weeks,
    year (cast (p.scheduled_at + interval '3' hour  as date)) as years,
    week (cast (p.scheduled_at + interval '3' hour  as date)) as week,
    count (*) as rides_this_week
    from gettaxi_ru_production.orders p
    join stat_first_month st on p.driver_id = st.driver_id
          and cast (p.scheduled_at + interval '3' hour  as date) between date_add('day', -1, st.ftr_date) and st.ftr_date_plus_1_month
    where true
        and p.status_id = 7
        and (p.company_id != 4730 or p.company_id is null)
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
        )
(select
    r.*,
    row_number () over (partition by r.driver_id order by r.driver_id, r.week) week_num,
    sum (r.rides_this_week) over (partition by r.ftr_fleet, r.driver_id,
    r.ftr_date order by week asc rows between unbounded preceding and current row) as cumsum

from rides r
where true
order by r.driver_id, r.week
)


 select
                    fo.driver_gk,
                    date_key,
                    city_name,
        week(date_key) week,

                    -- orders only on OF
--                     count(distinct case when ct.class_family <> 'Premium'
--                      and ordering_corporate_account_gk <> 20004730 then order_gk end) orders_of,
--         count(distinct case when ordering_corporate_account_gk <> 20004730 then  order_gk end) jorn,
           count (case when ordering_corporate_account_gk <> 20004730 then  order_gk end)
               over (partition by fo.driver_gk
                order by week(date_key) asc rows between unbounded preceding and current row) as cumsum

                    from emilia_gettdwh.dwh_fact_orders_v fo
                    left join emilia_gettdwh.dwh_dim_class_types_v AS ct
                        ON ct.class_type_key = fo.class_type_key
                    left join emilia_gettdwh.dwh_dim_locations_v loc on
                        fo.origin_location_key = loc.location_key and loc.country_id = 2
                    left join emilia_gettdwh.dwh_dim_drivers_v d on fo.driver_gk = d.driver_gk

                    where fo.lob_key in (5,6)
                    and date_key >= date'2020-10-1'
                    and order_status_key = 7
                    and fo.country_key = 2
                    and fo.driver_gk = 20001057065

                    --group by 1,2,3,5

select driver_gk, fleet_gk
from emilia_gettdwh.dwh_dim_drivers_v
where driver_gk = 20001092864

select
          p.driver_id,
          count(case when p.status_id = 7 then 1 end) as rides_compl,
          count(*) all_

          from gettaxi_ru_production.orders p

          where true
            and cast(p.scheduled_at + interval '3' hour  as date) between date'2021-3-20' and date'2021-4-20'
            --and p.status_id = 7
            and (p.company_id != 4730 or p.company_id is null)
and driver_id = 1063491
            group by 1
;

