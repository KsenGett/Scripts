-- p2p deliveries cube Delivery Performance
with main as (
with p2p_fo AS (
SELECT fo.date_key AS dates,
       date_format(fo.order_datetime, '%Y-%m') AS months,
       fo.hour_key,
       loc.city_name AS city,
       ct.lob_desc AS lob_desc,
       ct.class_type_desc AS class_type,
       fo.gt_order_gk AS order_gk,
       (CASE when order_status_key = 7 THEN 'Completed' ELSE 'Cancelled ON Arrival' end) AS order_status,
       fo.fleet_gk AS fleet_gk,
       fo.is_went_to_cc_key AS went_to_cc,
       v.vendor_name AS fleet_name,
       (CASE when v.vendor_name like '%courier car%' THEN 'PHV'
            when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
            when v.vendor_name like '%courier scooter%' THEN 'scooter'
            ELSE 'taxi'
       end) AS supply_type,
       fo.ordering_corporate_account_gk AS company_gk,
       ca.corporate_account_name AS company_name,
       am.name AS account_manager,
       (CASE when ct.class_type_desc like '%c2c%' THEN '3. C2C' ELSE
       (CASE when am.name like '%Delivery%' THEN '1. eCommerce' ELSE '2. Corporate' end) end) AS Client_type,
      fo.customer_total_cost_inc_vat AS customer_total_cost_inc_vat,
      fo.customer_total_cost AS customer_total_cost ,
      fo.driver_total_cost_inc_vat AS driver_total_cost_inc_vat ,
      fo.driver_total_cost AS driver_total_cost ,
      fo.driver_total_commission_exc_vat AS driver_total_commission_exc_vat,
     (CASE
            WHEN lob_desc = 'Deliveries - B2B'
            THEN customer_total_cost - driver_total_cost_inc_vat
            ELSE (CASE
                      WHEN customer_total_cost_inc_vat - driver_total_cost_inc_vat >0
                      THEN round((customer_total_cost_inc_vat - driver_total_cost_inc_vat)/1.2,2)
                      ELSE customer_total_cost_inc_vat - driver_total_cost_inc_vat
                  END)
        END) AS buy_sell
FROM emilia_gettdwh.dwh_fact_orders_v AS fo
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
  ON ct.class_type_key = fo.class_type_key
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
  ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
LEFT JOIN "emilia_gettdwh"."dwh_dim_account_managers_v" am ON am."account_manager_gk" = ca."account_manager_gk"
LEFT JOIN emilia_gettdwh.dwh_dim_locations_v AS loc
  ON loc.location_key = fo.origin_location_key
LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON d.driver_gk = fo.driver_gk
LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v AS v ON v.vendor_gk = d.fleet_gk
WHERE fo.country_key=2
  AND ct.lob_key IN (5,6)
  AND ct.class_group NOT LIKE 'Test'
  and ct.class_family not IN ('Premium')
  and (fo.order_status_key = 7 or (fo.order_status_key=4 and fo.driver_total_cost>0))
  AND fo.date_key BETWEEN date'2019-01-01' AND (CURRENT_DATE - interval '1' day)
  )

SELECT dates,
       months,
       p2p_fo.hour_key,
       tp.timecategory,
       tp.subperiod,
       tp.period,
       tp.subperiod2 AS time_period,
       city,
       lob_desc,
       class_type,
       client_type,
       p2p_fo.company_gk,
       company_name,
       account_manager,
       supply_type,
       order_status,
       accounts.name_internal AS company_name_united,
       went_to_cc,
       count(distinct order_gk) AS deliveries,
       sum(customer_total_cost_inc_vat) AS customer_total_cost_inc_vat,
       sum(customer_total_cost) AS customer_total_cost,
       sum(driver_total_cost_inc_vat) AS driver_total_cost_inc_vat,
       sum(driver_total_cost) AS driver_total_cost,
       sum(buy_sell) AS buy_sell,
       sum(driver_total_commission_exc_vat)*(-1) AS driver_total_commission_exc_vat
       FROM p2p_fo
       LEFT JOIN sheets."default".delivery_corp_accounts_20191203 AS accounts ON cast(accounts.company_gk AS bigint)=p2p_fo.company_gk
       LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.hour_key = p2p_fo.hour_key and tp.date_key = p2p_fo.dates
       and tp.timecategory IN ('1.Hours', '2.Dates', '3.Weeks', '4.Months', '5.Quarters', '7.Std Hours')
       WHERE tp.timecategory is not null
       GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

)
(
select client_type,
subperiod, sum(buy_sell) but_sell, sum(driver_total_commission_exc_vat) driver_total_comission,
sum(customer_total_cost) GMV_wo_vat,
sum(buy_sell)+sum(driver_total_commission_exc_vat) TR
from main
where subperiod in ('W37', 'W38','W39','W36')
group by 1,2

)