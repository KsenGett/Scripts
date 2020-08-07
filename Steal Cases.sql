--test
--with all_ as (
with aggregate_table as (
        with --5-7 sec
            fleet_rubbers as (
                select distinct fo.fleet_gk, fl.vendor_name,
                (CASE when fl.vendor_name like '%courier car%' THEN 'PHV'
                when fl.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                when fl.vendor_name like '%courier scooter%' THEN 'scooter'
                when fl.vendor_name is null THEN NULL
                ELSE 'taxi'
                end) AS supply_type, loc.region_name

                from sheets."default".delivery_steal_cases_actual steal
                --fo
                join emilia_gettdwh.dwh_fact_orders_v fo
                on steal.order_id = cast(fo.sourceid as varchar)
                and fo.date_key between date'2020-03-06' and current_date
                --fleet
                left join emilia_gettdwh.dwh_dim_vendors_v fl on fo.fleet_gk = fl.vendor_gk
                and fo.country_key = 2
                left join emilia_gettdwh.dwh_dim_locations_v loc
                on fo.origin_location_key = loc.location_key
                and fo.country_key = 2
                and fo.date_key between date'2020-03-06' and current_date
                and steal.order_id = cast(fo.sourceid as varchar)
                )

        --completed orders per date by vendor
        select
        flr.fleet_gk, flr.vendor_name, flr.supply_type, flr.region_name,
        fo.date_key,
        cast(0 AS integer) hour_key,
        count(case when fo.lob_key = 5 then fo.order_gk end) orders_b2b_7_per_day,
        count(case when fo.lob_key = 6 then fo.order_gk end) orders_b2c_7_per_day

        from fleet_rubbers flr
        left join emilia_gettdwh.dwh_fact_orders_v fo on
        flr.fleet_gk = fo.fleet_gk
            --filters
        WHERE fo.date_key between date'2020-03-04' and current_date
        and fo.country_key = 2
        and fo.lob_key IN (5,6)
        and fo.order_status_key = 7
        group by 1,2,3,4,5
    )
, steal_orders_info AS (
            SELECT  --orders
                fo.ordering_corporate_account_gk, fo.date_key, cast(0 AS integer) hour_key,
                fo.fleet_gk fleet_id,
                fl.vendor_name fleet,
                (CASE when fl.vendor_name like '%courier car%' THEN 'PHV'
                        when fl.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                        when fl.vendor_name like '%courier scooter%' THEN 'scooter'
                        when fl.vendor_name is null THEN NULL
                        ELSE 'taxi'
                   end) AS supply_type,
                dr.driver_name, dr.phone driver_phone_number,
                order_id, try_cast(parcel_value_Rub AS integer) order_cost,
                lower(status) status, loc.region_name,
                fo.Is_Future_Order_Key Is_Future_Order


                FROM sheets."default".delivery_steal_cases_actual steal
                --fact ORDER
                LEFT JOIN emilia_gettdwh.dwh_fact_orders_v fo
                ON cast(steal.order_id as bigint) = fo.sourceid
                and fo.date_key between date'2020-03-06' and current_date
                --company names
                LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v ca
                ON fo.ordering_corporate_account_gk = ca.corporate_account_gk
                and fo.date_key between date'2020-03-06' and current_date
                --fleet
                LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v fl ON fo.fleet_gk = fl.vendor_gk
                --drivers
                LEFT JOIN emilia_gettdwh.dwh_dim_drivers_v dr ON fo.driver_gk = dr.driver_gk
                and fo.country_key = 2 and fo.date_key between date'2020-03-06' and current_date
                --region
                left join emilia_gettdwh.dwh_dim_locations_v loc
                on fo.origin_location_key = loc.location_key
                and fo.country_key = 2
                and fo.date_key between date'2020-03-06' and current_date
                and steal.order_id = cast(fo.sourceid as varchar)
                )

SELECT   agt.fleet_gk, agt.vendor_name fleet_name, agt.supply_type, agt.region_name,agt.date_key,
tp.timecategory, tp.subperiod, tp.period, tp.subperiod2 AS time_period,
agt.orders_b2b_7_per_day, agt.orders_b2c_7_per_day,
soi.order_id, soi.status,soi.driver_name, soi.supply_type, soi.driver_phone_number, soi.order_cost
--spd.order_cost
FROM aggregate_table agt
            --steal date
LEFT JOIN steal_orders_info soi
ON soi.fleet_id = agt.fleet_gk and soi.date_key = agt.date_key
and soi.supply_type = agt.supply_type and soi.region_name = agt.region_name
            --time
LEFT JOIN  emilia_gettdwh.periods_v tp
ON agt.hour_key = tp.hour_key and tp.date_key = agt.date_key
and tp.timecategory IN ( '2.Dates', '3.Weeks', '4.Months', '5.Quarters')
WHERE tp.timecategory is not null

--) select count(distinct order_id), count(distinct fleet_gk) from all_


--test
with all__ as (
with aggregate_table AS (
            with robbed_companies AS --18 companies
                    (
                    SELECT fo.ordering_corporate_account_gk, fo.date_key, fo.origin_location_key,
                    (CASE when fl.vendor_name like '%courier car%' THEN 'PHV'
                    when fl.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                    when fl.vendor_name like '%courier scooter%' THEN 'scooter'
                    when fl.vendor_name is null THEN NULL
                    ELSE 'taxi' end) AS supply_type
                    --order_id

                        FROM sheets."default".delivery_steal_cases_actual steal
                        --fo
                        LEFT JOIN emilia_gettdwh.dwh_fact_orders_v fo
                        ON steal.order_id = cast(fo.sourceid AS varchar)
                        and fo.date_key between date'2020-03-06' and current_date
                        and fo.country_key = 2

                        LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v fl ON fo.fleet_gk = fl.vendor_gk
                        )

                --completed orders per date IN company
                SELECT --04-05 -1 taxi 750
                fo.ordering_corporate_account_gk,
                (CASE when fo.ordering_corporate_account_gk = cast(internal.company_gk AS integer)
                    THEN internal.name_internal ELSE
                        (case when fo.ordering_corporate_account_gk = -1 then 'C2C'
                        else ca.corporate_account_name end)end) company_name,

                 (CASE when fo.lob_key = 6 THEN '3. C2C' ELSE
                    (CASE when fo.ordering_corporate_account_gk IN
                    (200024062, 20007748, 200020229, 200010176, 200010174, 200010175,
                    200010173, 200024020, 200024019, 200024022, 200024021, 200021777,
                    200022170, 20009449, 20004469, 200023153, 200012721, 200022121,
                    200022024, 200019250, 200024495, 200024403, 200022256, 200023661,
                    200025094, 200025199, 200024424, 200025082, 200025083,200025081,
                    20007916,20007918,20007915,20007917,200025160, 200025235, 200025241,
                    200025387, 200024152, 200025410)
                    THEN '1. eCommerce' ELSE '2. Corporate' end) end) AS Client_type,
                    loc.region_name,
                    fo.date_key, cast('0' as integer) hour_key,
                    (CASE when fl.vendor_name like '%courier car%' THEN 'PHV'
                    when fl.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                    when fl.vendor_name like '%courier scooter%' THEN 'scooter'
                    when fl.vendor_name is null THEN NULL
                    ELSE 'taxi' end) AS supply_type,
                    (case when fo.ordering_corporate_account_gk = cr.ordering_corporate_account_gk
                    and fo.origin_location_key = cr.origin_location_key and fo.date_key = cr.date_key
                    and supply_type = cr.supply_type
                    then 'Yes' else 'No' end) as robbed_company,

                    count(fo.order_gk) orders_7_per_day
                    --count(CASE when fo.is_future_order_key = 1 THEN fo.order_gk end) future_orders_number

                    FROM emilia_gettdwh.dwh_fact_orders_v fo
                    --robbed companies
                    LEFT JOIN robbed_companies cr ON
                    cr.ordering_corporate_account_gk = fo.ordering_corporate_account_gk
                    and fo.date_key = cr.date_key and fo.origin_location_key = cr.origin_location_key
                    --vendor
                    LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v fl ON fo.fleet_gk = fl.vendor_gk
                    -- company name
                    LEFT JOIN sheets."default".delivery_corp_accounts_20191203 internal ON
                    fo.ordering_corporate_account_gk = cast(internal.company_gk AS integer)
                    LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v ca
                    ON fo.ordering_corporate_account_gk = ca.corporate_account_gk
                    and fo.date_key between date'2020-03-06' and current_date

                    --region name
                    left join emilia_gettdwh.dwh_dim_locations_v loc
                    on fo.origin_location_key = loc.location_key

                    where fo.lob_key in (5,6)
                    and fo.date_key between date'2020-03-06' and current_date
                    and fo.country_key = 2
                    and fo.order_status_key = 7

                    group by 1,2,3,4,5,6,7,8
                )

            , steal_orders_info AS (
            SELECT  --orders
                fo.ordering_corporate_account_gk, fo.date_key, cast(0 AS integer) hour_key,
                fo.fleet_gk fleet_id,
                fl.vendor_name fleet,
                (CASE when fl.vendor_name like '%courier car%' THEN 'PHV'
                        when fl.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                        when fl.vendor_name like '%courier scooter%' THEN 'scooter'
                        when fl.vendor_name is null THEN NULL
                        ELSE 'taxi'
                   end) AS supply_type,
                dr.driver_name, dr.phone driver_phone_number,
                order_id, try_cast(parcel_value_Rub AS integer) order_cost,
                lower(status) status, loc.region_name,
                fo.Is_Future_Order_Key Is_Future_Order


                FROM sheets."default".delivery_steal_cases_actual steal
                --fact ORDER
                LEFT JOIN emilia_gettdwh.dwh_fact_orders_v fo
                ON cast(steal.order_id as bigint) = fo.sourceid
                and fo.date_key between date'2020-03-06' and current_date
                --company names
                LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v ca
                ON fo.ordering_corporate_account_gk = ca.corporate_account_gk
                and fo.date_key between date'2020-03-06' and current_date
                --fleet
                LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v fl ON fo.fleet_gk = fl.vendor_gk
                --drivers
                LEFT JOIN emilia_gettdwh.dwh_dim_drivers_v dr ON fo.driver_gk = dr.driver_gk
                and fo.country_key = 2
                --company internal name
                LEFT JOIN sheets."default".delivery_corp_accounts_20191203 internal ON
                fo.ordering_corporate_account_gk = cast(internal.company_gk AS integer)
                --region
                left join emilia_gettdwh.dwh_dim_locations_v loc
                on fo.origin_location_key = loc.location_key
                and fo.country_key = 2
                and fo.date_key between date'2020-03-06' and current_date
                and steal.order_id = cast(fo.sourceid as varchar)
                )

SELECT agt.ordering_corporate_account_gk company_gk,
agt.company_name company, agt.Client_type, agt.supply_type, agt.region_name,
agt.date_key, tp.timecategory, tp.subperiod, tp.period, tp.subperiod2 AS time_period,
agt.orders_7_per_day, agt.robbed_company,
soi.order_id, soi.status,soi.driver_name, soi.supply_type, soi.driver_phone_number, soi.order_cost,
soi.region_name
--spd.order_cost
FROM aggregate_table agt
            --steal date
LEFT JOIN steal_orders_info soi
ON soi.ordering_corporate_account_gk = agt.ordering_corporate_account_gk and soi.date_key = agt.date_key
and soi.supply_type = agt.supply_type and soi.region_name = agt.region_name

--time
LEFT JOIN  emilia_gettdwh.periods_v tp
ON agt.hour_key = tp.hour_key and tp.date_key = agt.date_key
and tp.timecategory IN ( '2.Dates', '3.Weeks', '4.Months', '5.Quarters')
where tp.timecategory is not null

) select count(distinct order_id), count(distinct company_gk) from all__


--Check Delivery Company performance  results
select count(distinct ordering_corporate_account_gk)
from emilia_gettdwh.dwh_fact_orders_v fo
where fo.lob_key in (5,6)
and fo.date_key between date'2020-03-06' and current_date
and fo.country_key = 2
and fo.order_status_key = 7

--- Orders details1
SELECT  --orders
                fo.ordering_corporate_account_gk, fo.date_key,
                (CASE when fo.ordering_corporate_account_gk = cast(internal.company_gk AS integer)
                    THEN internal.name_internal ELSE
                        (case when fo.ordering_corporate_account_gk = -1 then 'C2C'
                        else ca.corporate_account_name end)end) company_name,
                fo.fleet_gk fleet_id,
                fl.vendor_name fleet,
                (CASE when fl.vendor_name like '%courier car%' THEN 'PHV'
                        when fl.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                        when fl.vendor_name like '%courier scooter%' THEN 'scooter'
                        when fl.vendor_name is null THEN NULL
                        ELSE 'taxi'
                   end) AS supply_type,
                dr.driver_name, dr.phone driver_phone_number,
                order_id, try_cast(parcel_value_Rub AS integer) order_cost,
                lower(status) status, loc.region_name,
                fo.Is_Future_Order_Key Is_Future_Order


                FROM sheets."default".delivery_steal_cases_actual steal
                --fact ORDER
                LEFT JOIN emilia_gettdwh.dwh_fact_orders_v fo
                ON cast(steal.order_id as bigint) = fo.sourceid
                and fo.date_key between date'2020-03-06' and current_date
                --company names
                LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v ca
                ON fo.ordering_corporate_account_gk = ca.corporate_account_gk
                and fo.date_key between date'2020-03-06' and current_date
                --fleet
                LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v fl ON fo.fleet_gk = fl.vendor_gk
                --drivers
                LEFT JOIN emilia_gettdwh.dwh_dim_drivers_v dr ON fo.driver_gk = dr.driver_gk
                and fo.country_key = 2
                --company internal name
                LEFT JOIN sheets."default".delivery_corp_accounts_20191203 internal ON
                fo.ordering_corporate_account_gk = cast(internal.company_gk AS integer)
                --region
                left join emilia_gettdwh.dwh_dim_locations_v loc
                on fo.origin_location_key = loc.location_key
                and fo.country_key = 2
                and fo.date_key between date'2020-03-06' and current_date
                and steal.order_id = cast(fo.sourceid as varchar)



-- All delivery orders
select fo.ordering_corporate_account_gk,
(CASE when fo.ordering_corporate_account_gk = cast(internal.company_gk AS integer)
                    THEN internal.name_internal ELSE
                        (case when fo.ordering_corporate_account_gk = -1 then 'C2C'
                        else ca.corporate_account_name end)end) company_name,
loc.region_name,
fo.date_key, tp.timecategory, tp.subperiod, tp.period, tp.subperiod2 AS time_period,
count(order_gk) completed_orders

from emilia_gettdwh.dwh_fact_orders_v fo

LEFT JOIN sheets."default".delivery_corp_accounts_20191203 internal ON
fo.ordering_corporate_account_gk = cast(internal.company_gk AS integer)
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v ca
                ON fo.ordering_corporate_account_gk = ca.corporate_account_gk
                and fo.date_key between date'2020-03-06' and current_date
LEFT JOIN  emilia_gettdwh.periods_v tp
ON fo.hour_key = tp.hour_key and tp.date_key = fo.date_key
and tp.timecategory IN ( '2.Dates', '3.Weeks', '4.Months', '5.Quarters')

left join emilia_gettdwh.dwh_dim_locations_v loc
                on fo.origin_location_key = loc.location_key

where fo.lob_key in (5,6)
and fo.date_key between date'2020-03-06' and current_date
and tp.timecategory is not null
and fo.country_key = 2
and fo.order_status_key = 7

group by 1,2,3,4,5,6,7,8


---ALL FLEETS + Steal Details
with aggregate_table as (
        with --5-7 sec
            fleet_robbers as (
                select fo.fleet_gk
                /*, fl.vendor_name,
                (CASE when fl.vendor_name like '%courier car%' THEN 'PHV'
                when fl.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                when fl.vendor_name like '%courier scooter%' THEN 'scooter'
                when fl.vendor_name is null THEN NULL
                ELSE 'taxi'
                end) AS supply_type, fo.origin_location_kesy, fo.date_key*/
                from sheets."default".delivery_steal_cases_actual steal
                --fo
                join emilia_gettdwh.dwh_fact_orders_v fo
                on steal.order_id = cast(fo.sourceid as varchar)
                and fo.date_key between date'2020-03-06' and current_date
                /*--fleet
                left join emilia_gettdwh.dwh_dim_vendors_v fl on fo.fleet_gk = fl.vendor_gk
                and fo.country_key = 2
                and fo.date_key between date'2020-03-06' and current_date
                and steal.order_id = cast(fo.sourceid as varchar)*/
                )
        --completed orders per date by vendor
        select
        fo.fleet_gk, fl.vendor_name,
        (CASE when fl.vendor_name like '%courier car%' THEN 'PHV'
                when fl.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                when fl.vendor_name like '%courier scooter%' THEN 'scooter'
                when fl.vendor_name is null THEN NULL
                ELSE 'taxi'
                end) AS supply_type,
        fo.origin_location_key, loc.region_name,
        fo.date_key, cast(0 AS integer) hour_key,
        (case when fo.fleet_gk = flr.fleet_gk then 'Yes' else 'No' end) as robber_fleet,
        count(case when fo.lob_key = 5 then fo.order_gk end) orders_b2b_7_per_day,
        count(case when fo.lob_key = 6 then fo.order_gk end) orders_b2c_7_per_day

        from emilia_gettdwh.dwh_fact_orders_v fo
        --region
        left join emilia_gettdwh.dwh_dim_locations_v loc
                on fo.origin_location_key = loc.location_key
        --robbers fleets
        left join fleet_robbers flr on
        flr.fleet_gk = fo.fleet_gk
        -- fleet name
        left join emilia_gettdwh.dwh_dim_vendors_v fl on fo.fleet_gk = fl.vendor_gk
                and fo.country_key = 2
        --filters
        WHERE fo.date_key between date'2020-03-04' and current_date
        and fo.country_key = 2
        and fo.lob_key IN (5,6)
        and fo.order_status_key = 7
        group by 1,2,3,4,5,6,7,8
    )
, steal_orders_info AS (
            SELECT  --orders
                fo.ordering_corporate_account_gk, fo.date_key,
                cast(0 AS integer) hour_key,
                fo.fleet_gk fleet_id,
                fl.vendor_name fleet,
                (CASE when fl.vendor_name like '%courier car%' THEN 'PHV'
                        when fl.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                        when fl.vendor_name like '%courier scooter%' THEN 'scooter'
                        when fl.vendor_name is null THEN NULL
                        ELSE 'taxi'
                   end) AS supply_type,
                dr.driver_name, dr.phone driver_phone_number,
                order_id, try_cast(parcel_value_Rub AS integer) order_cost,
                lower(status) status, loc.region_name,
                fo.Is_Future_Order_Key Is_Future_Order


                FROM sheets."default".delivery_steal_cases_actual steal
                --fact ORDER
                LEFT JOIN emilia_gettdwh.dwh_fact_orders_v fo
                ON cast(steal.order_id as bigint) = fo.sourceid
                and fo.date_key between date'2020-03-06' and current_date
                --company names
                LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v ca
                ON fo.ordering_corporate_account_gk = ca.corporate_account_gk
                and fo.date_key between date'2020-03-06' and current_date
                --fleet
                LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v fl ON fo.fleet_gk = fl.vendor_gk
                --drivers
                LEFT JOIN emilia_gettdwh.dwh_dim_drivers_v dr ON fo.driver_gk = dr.driver_gk
                and fo.country_key = 2 and fo.date_key between date'2020-03-06' and current_date
                --region
                left join emilia_gettdwh.dwh_dim_locations_v loc
                on fo.origin_location_key = loc.location_key
                and fo.country_key = 2
                and fo.date_key between date'2020-03-06' and current_date
                and steal.order_id = cast(fo.sourceid as varchar)
                )

SELECT  agt.fleet_gk, agt.vendor_name fleet_name, agt.supply_type,
robber_fleet, agt.region_name, agt.date_key,
tp.timecategory, tp.subperiod, tp.period, tp.subperiod2 AS time_period,
agt.orders_b2b_7_per_day, agt.orders_b2c_7_per_day,
soi.order_id, soi.status, soi.driver_name, soi.supply_type, soi.driver_phone_number, soi.order_cost
--spd.order_cost
FROM aggregate_table agt
            --steal date
LEFT JOIN steal_orders_info soi
ON soi.fleet_id = agt.fleet_gk and soi.date_key = agt.date_key
and soi.supply_type = agt.supply_type and soi.region_name = agt.region_name
            --time
LEFT JOIN  emilia_gettdwh.periods_v tp
ON agt.hour_key = tp.hour_key and tp.date_key = agt.date_key
and tp.timecategory IN ( '2.Dates', '3.Weeks', '4.Months', '5.Quarters')
WHERE tp.timecategory is not null;