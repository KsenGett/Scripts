with main as (

select
distinct fo.driver_gk, dd.phone, dd.driver_name,

(case when fo.origin_location_key = 246 then 'Saint Petersburg Region'
    when fo.origin_location_key = 245 then 'Moscow Region'
    when fo.origin_location_key = 354 then 'Privolzhsky FD - Kazan' end) city,
count(distinct case when tp.timecategory = '4.Months' and order_gk is not null then subperiod2 end) number_months,
count(distinct case when tp.timecategory = '3.Weeks' and order_gk is not null then subperiod2 end) number_weeks,
count(distinct case when tp.timecategory = '2.Dates' and order_gk is not null then subperiod2 end) number_days,
avg(orders) orders_day,
max(dpd.date_key) last_day


from emilia_gettdwh.dwh_fact_orders_v fo
left join
        (
        select
        fo.driver_gk, date_key, count(order_gk) orders
        from emilia_gettdwh.dwh_fact_orders_v fo
        left join emilia_gettdwh.dwh_dim_drivers_v dd on fo.driver_gk = dd.driver_gk
        where 1=1
        and fo.date_key >= date'2020-06-01'
                and fo.origin_location_key in (245,246, 354)
                and fo.lob_key in (5,6)
                and fo.order_status_key = 7
                and dd.is_frozen<>1
        group by 1,2
        ) dpd on dpd.driver_gk = fo.driver_gk

LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.hour_key = 0 and tp.date_key = fo.date_key
       and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
left join emilia_gettdwh.dwh_dim_drivers_v dd on fo.driver_gk = dd.driver_gk

where fo.date_key > date'2019-05-01'
and dd.is_frozen<>1
and fo.lob_key in (5,6)
and fo.order_status_key = 7
and fo.origin_location_key in (245,246,354)
group by 1,2,3,4)

select main.* ,
case when last_day = date'2020-10-16' then 'one_week_chern'
when last_day = date'2020-10-09' then 'two_weeks_chern' end chern_period
from main
where last_day in (date'2020-10-16',date'2020-10-09');




select *
from emilia_gettdwh.dwh_dim_locations_v








