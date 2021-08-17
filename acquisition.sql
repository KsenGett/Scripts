-- "DATA" sheet in Acquisition google
select distinct
        driver_id, ride_type, ftr_date, ftr_cohort,
        ftr_fleet_name,

       -- stat by days
       order_date, order_month,
        (case when row_number_date = 1 then paid_deliveries end) paid_deliveries,
       (case when row_number_date = 1 then days_from_month_end end) days_from_month_end,
        (case when row_number_date = 1 then  days_from_month_start end) days_from_month_start,

       (case when row_number_date = 1 then DAU end) DAU, -- DAU per day and ftr_cohort

       -- CAC -- to save doubled payment information only once by join on month
       (case when row_number_payment = 1 and payment_month=order_month then payment_month end) payment_month,
       programm,
       (case when row_number_payment = 1 and payment_month=order_month then payoff end) payoff

from(

    select *,
           -- count DAU with ignore of doubles
           count(case when row_number_date = 1 then driver_id end) over (partition by order_date, ftr_cohort) DAU
    from
    (
    select
           da.*,
           ord.date_key order_date,
           date_format(ord.date_key, '%Y-%m') AS order_month,
           paid_deliveries,
           -- to calculate avg dau by filter by month order dates (like avg DAU for last 14 month days or for first 10 month days)
           days_from_month_end, days_from_month_start,
           -- to count doubles and calculate dau only for 1st row. doubles are due to payment info. one driver may have two in two months
           row_number() over (partition by da.driver_id,ord.date_key) row_number_date,

           -- to save payment only once
          row_number() over (partition by da.driver_id, da.payment_month,
          date_format(ord.date_key, '%Y-%m') order by ord.date_key) row_number_payment

    from ( -- DRIVERS
        select cast(driver_id as bigint) driver_id,
           ride_type,
           ftr_date,
           date_format( date(ftr_date), '%Y-%m') AS ftr_cohort,
           programm,
           ftr_fleet_name,
     --            week stat_week,
     --            payment_week,
           payment_month,
           sum(cast(payoff as integer)) payoff

            from sheets.default.payed_drivers_acquisition da
            LEFT JOIN (
                                    select subperiod stat_week,
                                           min(date_key) stat_week_day,
                                           concat('W',cast(week(min(date_key)+ interval '7' day) as varchar)) payment_week,
                                           date_format( min(date_key)+interval '7' day, '%Y-%m') payment_month
                                    from data_vis.periods_v
                                    where timecategory IN ( '3.Weeks')
                                      and hour_key=0 and period= '2021'
                                    group by 1
                        ) tp on tp.stat_week = concat('W',da.week)
            where true
                  --and cast(payoff as integer)>=0
                  --and driver_id = '1038070'
            group by 1,2,3,4,5,6,7--,8,9
            order by driver_id
            ) da
    left join
            (select distinct
              dd.driver_gk,
              dd.source_id,
              fo.date_key,
              tp.week, tp.month,
              coalesce(fo.orders, 0) + coalesce(md.deliveries,0) paid_deliveries,
                   -- to calculate avg dau by filter by month order dates (like avg DAU for last 14 month days or for first 10 month days)
             days_from_month_end, days_from_month_start

            from emilia_gettdwh.dwh_dim_drivers_v dd
            join sheets.default.payed_drivers_acquisition da on dd.source_id = cast(da.driver_id as bigint)

            left join
                (
                    select distinct driver_gk,
                                    fo.date_key,
                                    count(distinct case when ct.class_family <> 'Premium' and ordering_corporate_account_gk <> 20004730 then order_gk end) orders,
                        count(distinct case when ordering_corporate_account_gk = 20004730 then order_gk end) orders_nf
                    from emilia_gettdwh.dwh_fact_orders_v fo
                    left join emilia_gettdwh.dwh_dim_class_types_v ct on fo.class_type_key=ct.class_type_key

                    where fo.lob_key in (5, 6)
                      and date_key >= date '2021-04-01'
                      --and ordering_corporate_account_gk <> 20004730
                      and customer_total_cost>0
                      and fo.country_key = 2
                      --and ct.class_family <> 'Premium'

                    group by 1, 2
                ) fo on fo.driver_gk = dd.driver_gk

            left join --2sec
                    (
                        select distinct courier_gk,
                                        date_key,
                                        count(distinct delivery_gk) deliveries,
                                        count(distinct journey_gk)  journeys

                        from model_delivery.dwh_fact_deliveries_v d

                        where
                            date (d.created_at) >= date'2021-04-01'
                            and delivery_status_id = 4
                            and d.country_symbol= 'RU'
                            and total_customer_amount_exc_vat > 0
                            group by 1,2
                    ) md on md.courier_gk = fo.driver_gk and fo.date_key = md.date_key

            left join
                        (
                            select *,
                        date_diff('day',date_key,last_month_date) days_from_month_end,
                        date_diff('day',first_month_date,date_key) days_from_month_start
                        from(
                                select
                                subperiod week,
                                date_format(date_key, '%Y-%m') "month",
                                date_key,
                                max(date_key) over (partition by date_format(date_key, '%Y-%m')) last_month_date,
                                min(date_key) over (partition by date_format(date_key, '%Y-%m')) first_month_date


                                from data_vis.periods_v
                                 where timecategory IN ( '3.Weeks')
                                and hour_key=0 and period= '2021'
                        )) tp on tp.date_key = fo.date_key

        where true --and month(fo.date_key) = 6--da.driver_id = '102806'
            ) ord on ord.source_id = da.driver_id

    where date(da.ftr_date) >= date'2021-04-01'
    --and ord.date_key = date'2021-05-01'
    --and da.driver_id in (102806,1382167)
    and date_format(ord.date_key, '%Y-%m') >= date_format( date(ftr_date), '%Y-%m')
    order by da.driver_id desc
    )
)
-- delete double rows with nulls
where true
and (row_number_date = 1 or (row_number_payment = 1 and payment_month=order_month))
order by driver_id, order_date;




-- 2. For agents payments (Moscow only) -> "Sheet 1" in Acquisition google
select
driver_id,  ftr_fleet, ftr_date ,sum(rides_this_week)
from
(
---- for payment
with new_drivers as
            (
            select
                ftr.driver_gk, dd.source_id driver_id,
                dd.phone as driver_phone,
                dd.fleet_gk as ftr_fleet,
                fl.vendor_name ftr_fleet_name,
                substring(fl.vendor_name, 1, position('/' in fl.vendor_name)-1) as ftr_fleet_name_short, --delete supply type
                cast(ftr.date_key as date) as ftr_date,
                ftr.ride_type,
                date_add('month', 1, cast(ftr.date_key as date)) ftr_date_plus_1_month

            from analyst.reftr_delivery ftr
            left join emilia_gettdwh.dwh_dim_drivers_v dd on ftr.driver_gk = dd.driver_gk and dd.country_key = 2
            left join emilia_gettdwh.dwh_dim_vendors_v fl on dd.fleet_gk = fl.vendor_gk and fl.country_key = 2

            where true
          and dd.is_frozen <> 1
                and dd.fleet_gk in (200016265, 200016266, 200016267, 200014202, 200016359,
                                    200014203, 200017740, 200017741, 200016697, 200015607,
                                    20004202, 200017204, 200047113,200017698)
              and dd.source_id in (1084086,	1093740,	1122566,	1137464,	1143443,	1085366,	1098348,	1074958,	1084689,	1090385,	1133728,	1111727,	1111518,	1081806,	1090008,	1089195,	1119572,	1091377,	1115338,	1120963,	1087102,	1091577,	1102721,	1095057,	1069143,	1087943,	1097327,	1097420,	1088334,	1085988,	1125224,	1146338,	1089394,	1123500)
              and dd.country_key = 2
              and cast(ftr.date_key as date) >= date '2021-04-01'
)
    , stat_first_month as
        (
          select
          p.driver_gk,
          fnr.driver_id,
          fnr.driver_phone,
          fnr.ftr_fleet, fnr.ftr_fleet_name_short, fnr.ftr_fleet_name,
          fnr.ride_type,
          fnr.ftr_date,
          fnr.ftr_date_plus_1_month,
          count (distinct order_gk) as rides_1st_month,
                    count (case when p.date_key
                      between fnr.ftr_date and date_add('day', 6, fnr.ftr_date) then p.order_gk else null end) as rides_7_days,
          count (case when p.date_key
                      between fnr.ftr_date and date_add('day', 14, fnr.ftr_date) then p.order_gk else null end) as rides_14_days,
          count (case when p.date_key
                      between fnr.ftr_date and date_add('day', 20, fnr.ftr_date) then p.order_gk else null end) as rides_20_days,
          count (case when p.date_key
                      between fnr.ftr_date and date_add('day', 30, fnr.ftr_date) then p.order_gk else null end) as rides_30_days

          from emilia_gettdwh.dwh_fact_orders_v p
          join new_drivers fnr on fnr.driver_gk  = p.driver_gk
                        and p.date_key
                        between fnr.ftr_date and fnr.ftr_date_plus_1_month
            where true
            and p.order_status_key = 7
            and p.country_key = 2
              and dd.source_id in (1084086,	1093740,	1122566,	1137464,	1143443,	1085366,	1098348,	1074958,	1084689,	1090385,	1133728,	1111727,	1111518,	1081806,	1090008,	1089195,	1119572,	1091377,	1115338,	1120963,	1087102,	1091577,	1102721,	1095057,	1069143,	1087943,	1097327,	1097420,	1088334,	1085988,	1125224,	1146338,	1089394,	1123500)

            and p.date_key >= date '2021-04-01'
            and (p.ordering_corporate_account_gk <> 20004730 or p.ordering_corporate_account_gk is null or p.ordering_corporate_account_gk = -1)
            group by 1,2,3,4,5,6,7,8,9
            )


, rides as (
    select
    st.*,
    year (p.date_key) as years,
    week (p.date_key) as week,
    count (distinct order_gk) as rides_this_week

    from emilia_gettdwh.dwh_fact_orders_v p
    join stat_first_month st on p.driver_gk = st.driver_gk
          and p.date_key between date_add('day', -1, st.ftr_date) and st.ftr_date_plus_1_month
    where true
        and p.order_status_key = 7
            and p.country_key = 2
            and p.date_key >= date '2021-04-01'
            and (p.ordering_corporate_account_gk <> 20004730 or p.ordering_corporate_account_gk is null or p.ordering_corporate_account_gk = -1)
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
        )
(select
    r.*,
    week - week(ftr_date) + 1 week_num,
    sum (r.rides_this_week) over (partition by r.ftr_fleet, r.driver_id,
    r.ftr_date order by week asc rows between unbounded preceding and current row) as cumsum

from rides r
where true
order by r.driver_id, r.week
)
)

group by 1,2,3


-- 3. Only DAU script - NOT shown in Acquisition google
    select
           da.ride_type,
           ftr_cohort,
           programm,
           ord.date_key order_date,
           date_format(ord.date_key, '%Y-%m') AS order_month,
           days_from_month_end, days_from_month_start,
           count(distinct driver_id) DAU

    from ( -- DRIVERS
        select cast(driver_id as bigint) driver_id,
           ride_type,
           ftr_date,
           date_format( date(ftr_date), '%Y-%m') AS ftr_cohort,
           programm

            from sheets.default.payed_drivers_acquisition da
            where true
            order by driver_id
            ) da
    left join -- working days
            (select distinct
              dd.driver_gk,
              dd.source_id,
              fo.date_key,
              tp.week, tp.month,
            days_from_month_start,days_from_month_end

            from emilia_gettdwh.dwh_dim_drivers_v dd
            join sheets.default.payed_drivers_acquisition da on dd.source_id = cast(da.driver_id as bigint)

            left join
                (
                    select distinct driver_gk,
                                    fo.date_key

                    from emilia_gettdwh.dwh_fact_orders_v fo
                    left join emilia_gettdwh.dwh_dim_class_types_v ct on fo.class_type_key=ct.class_type_key

                    where fo.lob_key in (5, 6)
                      and date_key >= date '2021-04-01'
                      and order_status_key = 7
                      and ordering_corporate_account_gk <> 20004730
                      and customer_total_cost>0
                      and fo.country_key = 2

                ) fo on fo.driver_gk = dd.driver_gk

            left join
                        (
                            select *,
                        date_diff('day',date_key,last_month_date) days_from_month_end,
                        date_diff('day',first_month_date,date_key) days_from_month_start
                        from(
                                select
                                subperiod week,
                                date_format(date_key, '%Y-%m') "month",
                                date_key,
                                max(date_key) over (partition by date_format(date_key, '%Y-%m')) last_month_date,
                                min(date_key) over (partition by date_format(date_key, '%Y-%m')) first_month_date


                                from data_vis.periods_v
                                 where timecategory IN ( '3.Weeks')
                                and hour_key=0 and period= '2021'
                        )) tp on tp.date_key = fo.date_key

        where true --and month(fo.date_key) = 6--da.driver_id = '102806'

    ) ord on ord.source_id = da.driver_id

    where date(da.ftr_date) >= date'2021-04-01'
    and date_format(ord.date_key, '%Y-%m') >= date_format( date(ftr_date), '%Y-%m')

group by 1,2,3,4,5,6,7;



-- 4. Manual extract of FTRs by programmes -> "DB_jul" in Acquisition google
select
*, case when programme like '%MO%' and rides_first_30days_mo > 0 then programme
when programme not like '%MO%' then programme
else 'failed criteria' end corrected_programme

from (
    select ftr.date_key ftr_date,
    d.fleet_gk,
    ftr.driver_gk,
    ride_type,
    -- Programme name - correct after filters
    case when (d.fleet_gk in (200016265,
    200016266,
    200016267,
    200016359,
    200017741) and ftr.date_key between date'2021-07-01' and date'2021-07-31') then 'After 29'
    when (d.fleet_gk in (
    200023811,
    200023819,
    200037169,
    200037181) and ftr.date_key between date'2021-07-01' and date'2021-07-18') then 'April MO'
    when (d.fleet_gk in (200014202,
    200015607) and ftr.date_key between date'2021-07-01' and date'2021-07-31') then 'June MSK'
    when (d.fleet_gk in (200017204,
    200017698,
    200047113) and ftr.date_key between date'2021-07-01' and date'2021-07-31') then 'June SPBb'
    when (d.fleet_gk in (200023811,
    200023819,
    200037169,
    200037181) and ftr.date_key between date'2021-07-19' and date'2021-07-31'
    ) then 'July MO' end programme,

    -- Completed Rides
    count(distinct case when fo.date_key between ftr.date_key and ftr.date_key + interval '1' month
    then fo.order_gk end) rides_first_30days,
    count(distinct case when fo.date_key between ftr.date_key and ftr.date_key + interval '1' month
    and area_desc is not null
    then fo.order_gk end) rides_first_30days_mo

    from analyst.reftr_delivery ftr
    left join emilia_gettdwh.dwh_fact_orders_v fo on ftr.driver_gk = fo.driver_gk
    and fo.date_key >= date'2021-4-1' and lob_key in (5,6)
    and country_key = 2 and order_status_key = 7 and ordering_corporate_account_gk <> 20004730

    left join "emilia_gettdwh"."dwh_dim_areas_v" areas  on ST_Contains(ST_GeometryFromText(areas.borders)
    , ST_Point(fo.origin_longitude, fo.origin_latitude)) and "area_desc" like '%Moscow regions deliver%'

    join emilia_gettdwh.dwh_dim_drivers_v d on ftr.driver_gk = d.driver_gk
    and is_frozen <> 1
    where true
    and ftr.is_courier = 1
    and ((d.fleet_gk in (200016265,
    200016266,
    200016267,
    200016359,
    200017741) and ftr.date_key between date'2021-07-01' and date'2021-07-31') -- After 29
    or (d.fleet_gk in (
    200023811,
    200023819,
    200037169,
    200037181) and ftr.date_key between date'2021-07-01' and date'2021-07-18') --april mo
    or (d.fleet_gk in (200014202,
    200015607) and ftr.date_key between date'2021-07-01' and date'2021-07-31') -- june msk
    or ((d.fleet_gk in (200017204,
    200017698
    ) and ftr.date_key between date'2021-07-01' and date'2021-07-31') or
    d.fleet_gk in (200047113) and ftr.date_key between date'2021-07-12' and date'2021-07-31') -- june spb
    or (d.fleet_gk in (200023811,
    200023819,
    200037169,
    200037181) and ftr.date_key between date'2021-07-19' and date'2021-07-31'
)
)

and d.phone <> '8'
group by 1,2,3,4,5)


--5. Manual optimized
select
ftr_date,
fleet_gk,
driver_gk,
ride_type,
programme,
rides_first_30days_mo, rides_first_30days,

case when programme like '%MO%'
then (case when rides_first_30days_mo >= cast(step_four_orders as integer) then cast(step_four_payment as integer)
when rides_first_30days_mo >= cast(step_three_orders as integer)  then cast(step_three_payment as integer)
when rides_first_30days_mo >= cast(step_two_orders as integer)  then cast(step_two_payment as integer)
when rides_first_30days_mo >= cast(step_one_orders as integer) then cast(step_one_payment as integer) end)
when programme not like '%MO%'
then (case when
    rides_first_30days >= cast(step_four_orders as integer)  then cast(step_four_payment as integer)
when rides_first_30days >= cast(step_three_orders as integer)  then cast(step_three_payment as integer)
when rides_first_30days >= cast(step_two_orders as integer)  then cast(step_two_payment as integer)
when rides_first_30days >= cast(step_one_orders as integer)  then cast(step_one_payment as integer)
    else 0 end)
end payment

from
(

    select
      distinct
      ftr.date_key ftr_date,
    d.fleet_gk,
    ftr.driver_gk,
    ride_type,
           pr.Name ,
-- programme_desc
step_one_orders,	step_one_payment,
step_two_orders,	step_two_payment,	step_three_orders,
step_three_payment,
step_four_orders,
step_four_payment,
    (case when pr.mo_only = '1' and area_desc is not null then pr.Name
        when pr.mo_only = '0' then pr.Name  end) programme,


    -- Completed Rides
    count(distinct case when fo.date_key between ftr.date_key and ftr.date_key + interval '1' month
    then fo.order_gk end) rides_first_30days,
    count(distinct case when fo.date_key between ftr.date_key and ftr.date_key + interval '1' month
    and area_desc is not null
    then fo.order_gk end) rides_first_30days_mo

    from analyst.reftr_delivery ftr
    left join emilia_gettdwh.dwh_fact_orders_v fo on ftr.driver_gk = fo.driver_gk
    and fo.date_key >= date'2021-4-1' and lob_key in (5,6)
    and country_key = 2 and order_status_key = 7 and ordering_corporate_account_gk <> 20004730

    left join "emilia_gettdwh"."dwh_dim_areas_v" areas  on ST_Contains(ST_GeometryFromText(areas.borders)
    , ST_Point(fo.origin_longitude, fo.origin_latitude)) and "area_desc" like '%Moscow regions deliver%'

    join emilia_gettdwh.dwh_dim_drivers_v d on ftr.driver_gk = d.driver_gk
    and is_frozen <> 1

    join sheets.default.payed_drivers_programmes pr
        on cast(pr.fleet_gk as bigint) = d.fleet_gk
                    and ftr.date_key between date(pr.date_start)
                    and coalesce(date(date_end), current_date)

    where true
    and ftr.is_courier = 1
    and d.phone <> '8'

    -- set up lead date
    and ftr.date_key >= date'2021-07-01'

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
where programme is not null
order by driver_gk