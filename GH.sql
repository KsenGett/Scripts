with t1 AS (
    with AR AS (
            SELECT
             date_key,
             fof.driver_gk,
             SUM(  CASE WHEN fof.Driver_Response_Key=1 and fof.Delivered_Datetime IS NOT NULL THEN 1 ELSE 0 END) AS numerator,
                        (SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL THEN 1 ELSE 0 END) - SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL AND fof.Is_Withdrawned=1 AND fof.Driver_Response_Key<>1 THEN 1 ELSE 0 END) )  AS denominator
            FROM emilia_gettdwh.dwh_fact_offers_v fof
             LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v cl ON cl.class_type_key = fof.class_type_key
            WHERE lob_key IN (5,6)
            and fof.country_key = 2
            and date_key >= date'2020-01-01'
            GROUP BY 1,2
            ), -- AR excludes manually assigned orders (fof.Delivered_Datetime IS NULL and fof.Driver_Response_Key=1)

    GH AS ( SELECT fdh.driver_gk,
            date_key,
            sum(CASE
                                  when fdh.driver_status_key IN (2, 4, 5, 6) --Free, IN Routing, Busy, Busy IN Ride
                                          THEN fdh.minutes_in_status
                                  ELSE 0 end) / 60.0 AS gh
            FROM emilia_gettdwh.dwh_fact_drivers_hourly_v fdh
             WHERE   1 = 1
                        and fdh.date_key >= date'2020-01-01'
                        and fdh.country_key = 2
                        GROUP BY 1,2),

    rides AS (
                SELECT fo.driver_gk, fo.date_key,
                l.city_name,
                (CASE when v.vendor_name like '%courier car%' THEN 'PHV'
                                  when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                                  when v.vendor_name like '%courier scooter%' THEN 'scooter'
                                  when  v.vendor_name like '%courier trike%' THEN 'e-bike'
                                  when v.vendor_name is null THEN NULL
                                  ELSE 'taxi' end) AS supply_type,
                 count( distinct order_gk) AS deliveries,
                 count(distinct CASE when ordering_corporate_account_gk not IN (200023153, 200025199, 200023861) THEN order_gk end) AS not_VV_deliveries,
                 sum( CASE when ordering_corporate_account_gk not IN (200023153, 200025199, 200023861) THEN driver_total_cost end ) +
                 sum( CASE when ordering_corporate_account_gk not IN (200023153, 200025199, 200023861) THEN driver_total_commission_inc_vat end ) AS driver_daily_earnings_not_VV,
                 sum(driver_total_cost) + sum(driver_total_commission_inc_vat) AS driver_daily_earnings --incl fleet commission
                  FROM "emilia_gettdwh"."dwh_fact_orders_v" fo
                  LEFT JOIN "emilia_gettdwh"."dwh_dim_class_types_v" ct ON ct.class_type_key = fo.class_type_key
                 LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON d.driver_gk = fo."driver_gk"
                 LEFT JOIN "emilia_gettdwh"."dwh_dim_vendors_v" v ON v.vendor_gk = d.fleet_gk
                 LEFT JOIN "emilia_gettdwh"."dwh_dim_locations_v" l ON l.location_key = fo.origin_location_key
                WHERE fo.lob_key IN (5,6)
                and fo.country_key = 2
                and  "order_status_key" = 7 -- completed_only
                and fo.date_key >= date '2020-01-01'
                --and "origin_location_key" = 245 --MSK only
                and (ct.class_family <> 'Premium' or (ct.class_family = 'Premium' and ct.class_type_desc like '%ondemand%')) -- exclude long routes
                --and vendor_name like '%courier car%' --PHV only
                and ordering_corporate_account_gk <> 20004730 -- exclude DROP off orders (dummy)
                GROUP BY 1,2,3,4)

            SELECT r.driver_gk,
            r.date_key,
            r.city_name,
            r.supply_type,
            CASE when deliveries < 5 THEN '0-5'
            when deliveries between 5 and 6 THEN '5-6'
            when deliveries between 7 and 9 THEN '7-9'
            when deliveries >= 10 THEN '10+' end AS order_group_desc,
            CASE when deliveries < 5 THEN 1
            when deliveries between 5 and 6 THEN 2
            when deliveries between 7 and 9 THEN 3
            when deliveries >= 10 THEN 4 end AS order_group_key,
            sum(numerator) AS Accepted_offers,
            sum(denominator)AS received_offers,
            sum(gh) AS GH_ttl,
            sum(deliveries) AS TTL_deliveries,
            sum(driver_daily_earnings) AS TTL_driver_earnings,
            sum(not_VV_deliveries) AS TTL_deliveries_not_VV,
            sum(driver_daily_earnings_not_VV) AS TTL_driver_earnings_not_VV
           FROM rides r
            LEFT JOIN ar ON ar.driver_gk = r.driver_gk and ar.date_key = r.date_key
            LEFT  JOIN gh ON gh.driver_gk = r.driver_gk and gh.date_key = r.date_key
            GROUP BY 1,2,3,4,5,6
)

            (SELECT t1.date_key,
            driver_gk,
            city_name,
            supply_type,
            tp.timecategory,
            tp.subperiod,
            tp.period,
            date_format (t1.date_key, '%W') AS weekday,
            tp.subperiod2 AS time_period,
             order_group_desc,
            order_group_key,
            sum (Accepted_offers) AS Accepted_offers,
            sum( received_offers) AS received_offers,
            sum(GH_ttl) AS GH_ttl,
            sum( TTL_deliveries) AS TTL_deliveries,
            sum(TTL_driver_earnings) AS TTL_driver_earnings,
            sum(TTL_deliveries_not_VV) AS TTL_deliveries_not_VV,
            sum( TTL_driver_earnings_not_VV) AS TTL_driver_earnings_not_VV
            FROM t1
             LEFT JOIN  (SELECT distinct "timecategory", "subperiod", "subperiod2", "period", "date_key" FROM emilia_gettdwh.periods_v
             WHERE timecategory IN ('2.Dates', '3.Weeks', '4.Months', '5.Quarters')) tp ON tp.date_key = t1.date_key
       GROUP BY 1,2,3,4,5,6,7,8,9,10,11)