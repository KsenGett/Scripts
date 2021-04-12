select * from emilia_gettdwh.dwh_dim_drivers_v
where last_week_number_of_rides not in (-1, 0) limit 10;

-- TODO add supply type, GH,
--  companies info (rank by impact to the company, rank probability to get order),
--  clean companies from 'Premium' (bad number of orders, distances), IML(driver cost)
--  NF correction - join fact deliveries on driver_gk (check count driver gk in fact orders and fd)



select distinct fo.driver_gk,
day_of_week(order_create_datetime) week_day,
(case when hour_key between 0 and 2 then '1.0-3'
    when hour_key between 3 and 5 then '2.3-6'
    when hour_key between 6 and 8 then '3.6-9'
    when hour_key between 9 and 11 then '4.9-12'
    when hour_key between 12 and 14 then '5.12-15'
    when hour_key between 15 and 17 then '6.15-18'
    when hour_key between 18 and 20 then '7.18-21'
    when hour_key between 21 and 23 then '8.21-00'
end) hour_bean, driver_life.weeks,

dr_gh.gh,
count(order_gk) orders,
sum(driver_total_cost) driver_total_cost,
sum(est_distance) est_distance_total

from emilia_gettdwh.dwh_fact_orders_v fo
left join (
        select distinct driver_gk, array_agg(distinct subperiod order by subperiod) weeks

        from emilia_gettdwh.dwh_fact_orders_v fo
        LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.date_key = fo.date_key
        and fo.hour_key = tp.hour_key
        and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')

        where lob_key in (5,6)
        and subperiod like 'W%'
        and country_key = 2
        and order_status_key = 7
        and fo.date_key >= date'2020-06-01'
        group by 1) as driver_life on fo.driver_gk = driver_life.driver_gk
left join (
            SELECT fdh.driver_gk,
            day_of_week(date_key) week_day,
            avg(CASE
                                  when fdh.driver_status_key IN (2, 4, 5, 6) --Free, IN Routing, Busy, Busy IN Ride
                                          THEN fdh.minutes_in_status / 60.0
                                  ELSE 0 end) AS gh
            FROM emilia_gettdwh.dwh_fact_drivers_hourly_v fdh
             WHERE   1 = 1
                        and fdh.date_key >= date'2020-06-01'
                        and fdh.country_key = 2
                        GROUP BY 1,2
            ) as dr_gh on dr_gh.driver_gk = fo.driver_gk and dr_gh.week_day = day_of_week(fo.date_key)

where lob_key in (5,6)
and country_key = 2
and order_status_key = 7
and fo.date_key >= date'2020-06-01'
group by 1,2,3,4,5;




select distinct driver_gk,
(case when
    accounts.name_internal is null then
    ca.corporate_account_name
    when ordering_corporate_account_gk = -1 then 'c2c'
    else accounts.name_internal end) company_name,

 count(distinct order_gk) driver_compl_orders,
sum(count(distinct order_gk)) over(partition by driver_gk) drivers_ordes,
sum(count(distinct order_gk)) over(partition by
                                        (case when
                                            accounts.name_internal is null then
                                            ca.corporate_account_name
                                            when ordering_corporate_account_gk = -1 then 'c2c'
                                            else accounts.name_internal end)
                                        ) company_orders


from emilia_gettdwh.dwh_fact_orders_v fo
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
    ON ca.corporate_account_gk = fo.ordering_corporate_account_gk

LEFT JOIN sheets."default".delivery_corp_accounts_20191203 AS accounts
        ON cast(accounts.company_gk AS bigint)=fo.ordering_corporate_account_gk

where lob_key in (5,6)
and fo.country_key = 2
and order_status_key = 7
and date_key >= date'2020-06-01'
and fo.ordering_corporate_account_gk not in (20004730,200017459)
group by 1,2;

--company bucket grocery(Pya, per, vv), electronics (mVIdeo), retail , corp (others)-delete
--regions (offer driver location) , AR vs est distance


with GH as (

            SELECT fdh.driver_gk,
            registration_date_key,
            fdh.date_key, location_key,
            driver_status_key,
            min_latitude, max_latitude, min_longitude, max_longitude,
            sum(minutes_in_status)/ 60.00 AS gh

            FROM emilia_gettdwh.dwh_fact_drivers_hourly_v fdh
            left join emilia_gettdwh.dwh_dim_drivers_v d on fdh.driver_gk = d.driver_gk

             WHERE   1 = 1
                        and fdh.date_key >= date'2020-09-01'
                        and fdh.country_key = 2
                        and is_courier = 1
                        and driver_status_key IN (2, 4, 5, 6) --Free, IN Routing, Busy, Busy IN Ride
                        and location_key in (245,246)
            group by 1,2,3,4,5,6,7,8,9

            ),

fof as ( --26 sec

    select fof.driver_gk, fof.date_key, vendor_name,
    destination_order_location_key,
    count(distinct fo.order_gk) orders,
    count(Case when driver_response_key = 1
                then offer_gk end) accepted_offers,
    count(Case when driver_response_key = 1 or is_received = 1
                then offer_gk end) received_offers,
    count(Case when driver_response_key = 2 and is_received = 1 and is_withdrawned = 1
                then offer_gk else NULL end)  ignored_and_withdrawn

    from emilia_gettdwh.dwh_fact_offers_v  fof
    LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
        ON ct.class_type_key = fof.class_type_key

    left join emilia_gettdwh.dwh_fact_orders_v fo on fof.order_gk = fo.order_gk
        and fo.country_key =2 and fo.lob_key in (5,6) and fo.order_status_key = 7
        and fo.date_key >= date'2020-09-01'

    left join emilia_gettdwh.dwh_dim_drivers_v d on fof.driver_gk = d.driver_gk
    left join  emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
                and fl.country_symbol = 'ru'

    where fof.country_key = 2
    and ct.lob_key IN (5,6)
    and fof.date_key >= date'2020-09-01'
    and destination_order_location_key in (245, 246)

    group by 1,2,3,4
    )
, churn as (

    select driver_gk, origin_location_key, 'churned' churn, last_date
    from (
        select driver_gk,
        origin_location_key,
        max(date_key) last_date

        from emilia_gettdwh.dwh_fact_orders_v
        where lob_key in (5,6)
        and origin_location_key in (245,246)
        and country_key = 2
        and order_status_key = 7
        and date_key between current_date - interval '70' day and current_date
        group by 1,2
        )
    where last_date between current_date - interval '60' day and current_date
)

(
select *, accepted_offers*1.00 / nullif((received_offers - ignored_and_withdrawn), 0)*100.0 AR

from GH
left join fof on GH.driver_gk = fof.driver_gk and GH.date_key = fof.date_key
and fof.destination_order_location_key  = GH.location_key
left join churn ch on GH.driver_gk = ch.driver_gk and GH.location_key = ch.origin_location_key
    and GH.date_key = ch.last_date
)


select *
from emilia_gettdwh.dwh_fact_drivers_hourly_v
where driver_gk = 2000829348 --1.00 st 2
and date_key = date'2020-10-28';




--fleet 200014151

with GH as (

            SELECT fdh.driver_gk,
            registration_date_key,
            fdh.date_key, location_key,
            d.fleet_gk, fl.vendor_name,
            driver_status_key,
            min_latitude, max_latitude, min_longitude, max_longitude,
            sum(minutes_in_status)/ 60.00 AS gh

            FROM emilia_gettdwh.dwh_fact_drivers_hourly_v fdh
            left join emilia_gettdwh.dwh_dim_drivers_v d on fdh.driver_gk = d.driver_gk
            left join emilia_gettdwh.dwh_dim_vendors_v fl on fl.vendor_gk = d.fleet_gk

             WHERE   1 = 1
                        and fdh.date_key >= date'2020-09-01'
                        and fdh.country_key = 2
                        and is_courier = 1
                        and driver_status_key IN (2, 4, 5, 6) --Free, IN Routing, Busy, Busy IN Ride
                        and location_key in (245,246)
            group by 1,2,3,4,5,6,7,8,9

            ),

fof as ( --26 sec

    select fof.driver_gk, fof.date_key, vendor_name,
    destination_order_location_key,
    count(distinct fo.order_gk) orders,
    count(Case when driver_response_key = 1
                then offer_gk end) accepted_offers,
    count(Case when driver_response_key = 1 or is_received = 1
                then offer_gk end) received_offers,
    count(Case when driver_response_key = 2 and is_received = 1 and is_withdrawned = 1
                then offer_gk else NULL end)  ignored_and_withdrawn

    from emilia_gettdwh.dwh_fact_offers_v  fof
    LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
        ON ct.class_type_key = fof.class_type_key

    left join emilia_gettdwh.dwh_fact_orders_v fo on fof.order_gk = fo.order_gk
        and fo.country_key =2 and fo.lob_key in (5,6) and fo.order_status_key = 7
        and fo.date_key >= date'2020-09-01'

    left join emilia_gettdwh.dwh_dim_drivers_v d on fof.driver_gk = d.driver_gk
    left join  emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
                and fl.country_symbol = 'ru'

    where fof.country_key = 2
    and ct.lob_key IN (5,6)
    and fof.date_key >= date'2020-09-01'
    and destination_order_location_key in (245, 246)

    group by 1,2,3,4
    )
, churn as (

    select driver_gk, origin_location_key, 'churned' churn, last_date
    from (
        select driver_gk,
        origin_location_key,
        max(date_key) last_date

        from emilia_gettdwh.dwh_fact_orders_v
        where lob_key in (5,6)
        and origin_location_key in (245,246)
        and country_key = 2
        and order_status_key = 7
        and date_key between current_date - interval '70' day and current_date
        group by 1,2
        )
    where last_date between current_date - interval '30' day and current_date
)

(
select *, accepted_offers*1.00 / nullif((received_offers - ignored_and_withdrawn), 0)*100.0 AR

from GH
left join fof on GH.driver_gk = fof.driver_gk and GH.date_key = fof.date_key
and fof.destination_order_location_key  = GH.location_key
left join churn ch on GH.driver_gk = ch.driver_gk and GH.location_key = ch.origin_location_key
    and GH.date_key = ch.last_date
)

-- work days
