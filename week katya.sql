/*
Owner - Ekaterina Nesterova
Cube Name - Delivery Performance - UPD
ID - A049E02A11EB08E358E50080EFC56510
*/

select time_period,
sum(paid_deliveries)

from (

with all_orders AS (
SELECT fo.date_key AS dates,
       fo.hour_key,
       loc.city_name AS city,
       ct.lob_desc AS lob_desc,
       ct.class_type_desc AS class_type,
       fo.gt_order_gk AS order_gk,
       -- driver_gk = 200013 i a CC user, all his orders should be marked AS cancelled
       CASE when fo.driver_gk = 200013 THEN 4 ELSE order_status_key end AS order_status_key,
       order_cancellation_stage_key,
       fo.is_went_to_cc_key AS went_to_cc,
       (CASE when v.vendor_name like '%courier car%' THEN 'PHV'
            when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
            when v.vendor_name like '%courier scooter%' THEN 'scooter'
            ELSE 'taxi'
       end) AS supply_type,
       (CASE when ct.class_family = 'Premium' THEN 'NF' ELSE 'OF' end) AS platform,
       fo.ordering_corporate_account_gk AS company_gk,
       ca.corporate_account_name AS company_name,
       am.name AS account_manager,
       del_ftp_date_key,
       (CASE when ct.class_type_desc like '%c2c%' THEN 3 ELSE
       (CASE when am.name like '%Delivery%' THEN 1 ELSE 2 end) end) AS Client_type_key,
 (CASE when ct.class_type_desc like '%c2c%' THEN 'C2C' ELSE
       (CASE when am.name like '%Delivery%' THEN 'eCommerce' ELSE 'Corporate' end) end) AS Client_type_desc,
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
        END) AS buy_sell,
sum(CASE when ct.class_family IN ('Premium') and ct.class_type_desc not like '%ondemand%' then jo.completed_deliveries
when ct.class_family IN ('Premium') and ct.class_type_desc like '%ondemand%' THEN jo.picked_up_deliveries ELSE 0 end) AS paid_deliveries_NF,
sum(CASE when ct.class_family IN ('Premium') THEN jo.completed_deliveries ELSE 0 end) AS completed_deliveries_NF,
sum(CASE when ct.class_family IN ('Premium') THEN jo.picked_up_deliveries ELSE 0 end) AS picked_up_deliveries_NF,
sum(CASE when ct.class_family IN ('Premium') THEN jo.gross_deliveries ELSE 0 end) AS gross_deliveries_NF
FROM "emilia_gettdwh"."dwh_fact_orders_v" fo
LEFT JOIN (
SELECT  j.legacy_order_id ,
-- supplier_id = 13 - CC user that is used to cancel orders, all his orders should be marked AS cancelled
count(CASE when d.status = 'completed' and j.supplier_id <> 13 THEN d.id end) AS completed_deliveries,
count(CASE when d.status IN ('completed','not_delivered') and j.supplier_id <> 13 THEN d.id end) AS picked_up_deliveries,
count(d.id) AS gross_deliveries
FROM delivery.public.journeys AS j
LEFT JOIN delivery.public.deliveries AS d ON d.journey_id=j.id
WHERE 1=1
and d.status IN ('completed','not_delivered', 'rejected', 'cancelled')
and j.env='RU'
and d.display_identifier <> 'Returns'
and d.company_id <> '1999'
GROUP BY 1
) AS jo ON jo.legacy_order_id=fo.sourceid
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
  ON ct.class_type_key = fo.class_type_key
LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
  ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
LEFT JOIN  emilia_gettdwh.dwh_dim_account_managers_v am ON am."account_manager_gk" = ca."account_manager_gk"
LEFT JOIN emilia_gettdwh.dwh_dim_locations_v AS loc
  ON loc.location_key = fo.origin_location_key
LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON d.driver_gk = fo.driver_gk
LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v AS v ON v.vendor_gk = d.fleet_gk
WHERE fo.country_key=2
  AND ct.lob_key IN (5,6)
  AND ct.class_group NOT LIKE 'Test'
  and ordering_corporate_account_gk not IN ( 20004730, 200017459, 20001999) --dummy delivery user and test company
  AND fo.date_key >= date'2021-02-01'
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
  )

SELECT dates,
       tp.timecategory,
       tp.subperiod,
       tp.period,
       tp.subperiod2 AS time_period,
       city,
       all_orders.platform,
       lob_desc,
       class_type,
       client_type_key,
       client_type_desc,
       all_orders.company_gk,
       company_name,
       account_manager,
       del_ftp_date_key,
       supply_type,
       order_status_key,
       CASE when lob_desc = 'Deliveries - B2C' THEN 'C2C'
       when lob_desc = 'Deliveries - B2B' and name_internal is null THEN 'others' ELSE name_internal end AS company_name_united,
       CASE when lob_desc = 'Deliveries - B2C' THEN 'C2C'
       when lob_desc = 'Deliveries - B2B' and segment is null THEN 'other' ELSE segment end AS Segment,
       went_to_cc,
      sum(CASE when platform = 'OF'  and (order_status_key = 7 or (order_status_key=4 and "order_cancellation_stage_key" = 3)) THEN 1
         when platform = 'NF' THEN paid_deliveries_NF end) AS paid_deliveries,
       sum(CASE when platform = 'OF' and (order_status_key = 7 or (order_status_key=4 and "order_cancellation_stage_key" = 3)) THEN 1
         when platform = 'NF' THEN picked_up_deliveries_NF end) AS picked_up_deliveries,
           sum(CASE when platform = 'OF' THEN 1
         when platform = 'NF' THEN gross_deliveries_NF end) AS gross_deliveries,
       sum(customer_total_cost_inc_vat) AS customer_total_cost_inc_vat,
       sum(customer_total_cost) AS customer_total_cost,
       sum(driver_total_cost_inc_vat) AS driver_total_cost_inc_vat,
       sum(driver_total_cost) AS driver_total_cost,
       sum(buy_sell) AS buy_sell,
       sum(driver_total_commission_exc_vat)*(-1) AS driver_total_commission_exc_vat
       FROM all_orders
       LEFT JOIN sheets."default".delivery_corp_accounts_20191203 AS accounts ON cast(accounts.company_gk AS bigint)=all_orders.company_gk
       LEFT JOIN data_vis.periods_v AS tp ON tp.date_key = all_orders.dates and tp.hour_key = all_orders.hour_key
        WHERE timecategory IN ('2.Dates', '3.Weeks', '4.Months', '5.Quarters', '7.Std Hours')
        and subperiod2 like '%W09%'
       GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

)
where time_period like '%W09%'
group by 1;