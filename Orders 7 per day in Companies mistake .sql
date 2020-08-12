with aggregate_table AS (
            with robbed_companies AS --18 companies
                    (
                    SELECT distinct fo.ordering_corporate_account_gk
                    /*fo.date_key, fo.origin_location_key,
                    (CASE when fl.vendor_name like '%courier car%' THEN 'PHV'
                    when fl.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                    when fl.vendor_name like '%courier scooter%' THEN 'scooter'
                    when fl.vendor_name is null THEN NULL
                    ELSE 'taxi' end) AS supply_type */
                    --order_id

                        FROM sheets."default".delivery_steal_cases_actual steal
                        --fo
                        LEFT JOIN emilia_gettdwh.dwh_fact_orders_v fo
                        ON steal.order_id = cast(fo.sourceid AS varchar)
                        and fo.date_key between date'2020-03-06' and current_date
                        and fo.country_key = 2

                        --LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v fl ON fo.fleet_gk = fl.vendor_gk
                        )

                --completed orders per date IN company
                SELECT
                fo.ordering_corporate_account_gk,
                (CASE when fo.ordering_corporate_account_gk = cast(internal.company_gk AS integer)
                    THEN internal.name_internal ELSE
                        (CASE when fo.ordering_corporate_account_gk = -1 THEN 'C2C'
                        ELSE ca.corporate_account_name end)end) company_name,

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
                    fo.date_key, cast('0' AS integer) hour_key,
                    (CASE when fl.vendor_name like '%courier car%' THEN 'PHV'
                    when fl.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                    when fl.vendor_name like '%courier scooter%' THEN 'scooter'
                    when fl.vendor_name is null THEN NULL
                    ELSE 'taxi' end) AS supply_type,

                    (CASE when fo.ordering_corporate_account_gk = cr.ordering_corporate_account_gk
                    THEN 'Yes' ELSE 'No' end) AS robbed_company,

                    count(fo.order_gk) orders_7_per_day
                    --count(CASE when fo.is_future_order_key = 1 THEN fo.order_gk end) future_orders_number

                    FROM emilia_gettdwh.dwh_fact_orders_v fo
                    --robbed companies
                    LEFT JOIN robbed_companies cr ON
                    cr.ordering_corporate_account_gk = fo.ordering_corporate_account_gk
                    --and fo.date_key = cr.date_key and fo.origin_location_key = cr.origin_location_key
                    --vendor
                    LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v fl ON fo.fleet_gk = fl.vendor_gk
                    -- company name
                    LEFT JOIN sheets."default".delivery_corp_accounts_20191203 internal ON
                    fo.ordering_corporate_account_gk = cast(internal.company_gk AS integer)
                    LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v ca
                    ON fo.ordering_corporate_account_gk = ca.corporate_account_gk
                    and fo.date_key between date'2020-03-06' and current_date

                    --region name
                    LEFT JOIN emilia_gettdwh.dwh_dim_locations_v loc
                    ON fo.origin_location_key = loc.location_key

                    WHERE fo.lob_key IN (5,6)
                    and fo.date_key between date'2020-03-06' and current_date
                    and fo.country_key = 2
                    and fo.order_status_key = 7

                    GROUP BY 1,2,3,4,5,6,7,8

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
                ON cast(steal.order_id AS bigint) = fo.sourceid
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
                LEFT JOIN emilia_gettdwh.dwh_dim_locations_v loc
                ON fo.origin_location_key = loc.location_key
                and fo.country_key = 2
                and fo.date_key between date'2020-03-06' and current_date
                and steal.order_id = cast(fo.sourceid AS varchar)
                )
SELECT agt.ordering_corporate_account_gk company_gk,
agt.company_name company, agt.Client_type, agt.supply_type, agt.region_name,
agt.date_key, tp.timecategory, tp.subperiod, tp.period, tp.subperiod2 AS time_period,
agt.orders_7_per_day, agt.robbed_company,
soi.order_id, soi.status,soi.driver_name, soi.supply_type supply_type2, soi.driver_phone_number, soi.order_cost,
soi.region_name region_name2
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
WHERE tp.timecategory is not null


select count(order_gk)
--2020-04-17 pyaterocka in my sql 5457; in dash 5457 in this request 5457

from emilia_gettdwh.dwh_fact_orders_v fo
WHERE fo.date_key = date'2020-04-17'
and fo.lob_key IN (5,6)
and fo.order_status_key = 7
and ordering_corporate_account_gk = 200022121