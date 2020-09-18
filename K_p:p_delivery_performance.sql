SELECT f.date_key AS dates,
       tp.timecategory,
       tp.period,
       tp.subperiod,
       (CASE WHEN (timecategory = '2.Dates') THEN date_diff('day', f.date_key, current_date)
WHEN ((timecategory = '3.Weeks') AND (day_of_week(f.date_key) = 0)) THEN (date_diff('week', f.date_key, current_date) + 1)
WHEN (timecategory = '3.Weeks') THEN max(date_diff('week', f.date_key, current_date)) OVER (PARTITION BY subperiod2)
WHEN (timecategory = '4.Months') THEN max(date_diff('month', f.date_key, current_date)) OVER (PARTITION BY subperiod2)
WHEN (timecategory = '5.Quarters') THEN max(date_diff('quarter', f.date_key, current_date)) OVER (PARTITION BY subperiod2)
WHEN (timecategory = '6.Years') THEN max(date_diff('year', f.date_key, current_date)) OVER (PARTITION BY subperiod2) ELSE null END) AS period_diff,
       tp.subperiod2 AS time_period,
       f.country_symbol,
       f.lob_key,
       ct.lob_desc,
       l.city_name,
(CASE when ct.lob_key = 6 THEN '3. C2C'
   when am.name like '%Delivery%' or ca.account_manager_gk IN( 100079, 100096, 100090, 100073, 100088)
   THEN '1. eCommerce' ELSE '2. Corporate' end ) AS Client_type,
 (CASE when f.country_key = 1 THEN NULL ELSE accounts.name_internal end) AS RU_company_name_united,
ct.class_type_desc AS class_type,
 count (distinct CASE when f.order_status_key = 7 THEN f.series_original_order_gk ELSE null end) AS completed_orders,
 count (distinct CASE when (f.order_status_key = 7 or (f.order_status_key = 4 and driver_total_cost > 0)) THEN f.order_gk ELSE null end) AS completed_and_cancelled_orders,
 count (distinct f.order_gk) AS gross_orders,
 count(distinct f.series_original_order_gk) AS net_orders
FROM emilia_gettdwh.dwh_fact_orders_v AS f
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v ca ON ca.corporate_account_gk = f.ordering_corporate_account_gk
LEFT JOIN "emilia_gettdwh"."dwh_dim_account_managers_v" am ON am."account_manager_gk" = ca."account_manager_gk"
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = f.class_type_key
LEFT JOIN sheets."default".delivery_corp_accounts_20191203 AS accounts ON cast(accounts.company_gk AS bigint)=f.ordering_corporate_account_gk
LEFT JOIN "emilia_gettdwh"."dwh_dim_locations_v" l ON l.location_key = f.destination_location_key
LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.date_key = f.date_key
and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
WHERE f.date_key between date '2020-01-01' and current_date - interval '1' day
and f.country_key IN (1,2)
and f.lob_key IN (5,6)
and ct.class_family not IN ('Premium')
and ct.class_group not like 'Test'
GROUP BY 1,2,3,4,6,7,8,9,10,11,12,13