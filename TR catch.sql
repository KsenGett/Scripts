/*
Owner - Kozlova Kseniia
Cube Name - Margine Track
ID -
*/

    with all_orders as (

        SELECT fo.date_key,
               loc.city_name AS city,
               ct.lob_desc AS lob_desc,
               ct.class_type_desc AS class_type,
               fo.gt_order_gk AS order_gk,
               jo.journey_id,
               jo.is_jouney_aggregation, jo.number_of_deliveries,
               fo.origin_full_address,
               ct.class_family = 'Premium' and jo.order_gk is null hard_reset,
               fo.est_distance, fo.est_duration, fo.ride_distance_key ride_distance,
               CASE when fo.driver_gk = 200013 THEN 4 ELSE order_status_key end AS order_status_key,
               fo.order_cancellation_stage_key,
               cst.order_cancellation_stage_desc,
               (CASE when v.vendor_name like '%courier car%' THEN 'PHV'
                    when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                    when v.vendor_name like '%courier scooter%' THEN 'scooter'
                    ELSE 'taxi'
               end) AS supply_type,
               (CASE when ct.class_family = 'Premium' THEN 'NF' ELSE 'OF' end) AS platform,
               fo.ordering_corporate_account_gk AS company_gk,
               ca.corporate_account_name AS company_name,
               am.name AS account_manager,
        --        (CASE when ct.class_type_desc like '%c2c%' THEN 3 ELSE
        --        (CASE when am.name like '%Delivery%' THEN 1 ELSE 2 end) end) AS Client_type_key,

                (CASE when ct.class_type_desc like '%c2c%' THEN '3. C2C' ELSE
               (CASE when am.name like '%Delivery%' THEN '1. eCommerce' ELSE '2. Corporate' end) end) AS Client_type_desc,
              coalesce((case when ct.class_family = 'Premium' then total_customer_amount_exc_vat*1.2 else fo.customer_total_cost_inc_vat end),0) AS customer_total_cost_inc_vat,
             coalesce((case when ct.class_family = 'Premium' then total_customer_amount_exc_vat else fo.customer_total_cost end),0) AS customer_total_cost ,
              fo.driver_total_cost_inc_vat AS driver_total_cost_inc_vat ,
              fo.driver_total_cost AS driver_total_cost ,
              fo.driver_total_commission_exc_vat AS driver_total_commission_exc_vat,
             (CASE
                    WHEN lob_desc = 'Deliveries - B2B'
                    THEN
                    (case when ct.class_family = 'Premium'
                         then coalesce(total_customer_amount_exc_vat,0) - coalesce(driver_total_cost_inc_vat,0)
                         else coalesce(customer_total_cost,0) - coalesce(driver_total_cost_inc_vat,0) end)
                    ELSE (CASE
                              WHEN coalesce(customer_total_cost_inc_vat,0) - coalesce(driver_total_cost_inc_vat,0) >0
                              THEN round((coalesce(customer_total_cost_inc_vat,0) - coalesce(driver_total_cost_inc_vat,0))/1.2,2)
                              ELSE coalesce(customer_total_cost_inc_vat,0) - coalesce(driver_total_cost_inc_vat,0)
                          END)
                END) AS buy_sell,
        sum(CASE when ct.class_family IN ('Premium') and (ct.class_type_desc not like '%ondemand%' or fo.ordering_corporate_account_gk = 200025342) then jo.completed_deliveries
        when ct.class_family IN ('Premium') and ct.class_type_desc like '%ondemand%' and fo.ordering_corporate_account_gk <> 200025342 THEN jo.picked_up_deliveries ELSE 0 end) AS paid_deliveries_NF,

        sum(CASE when ct.class_family IN ('Premium') THEN jo.completed_deliveries ELSE 0 end) AS completed_deliveries_NF,
        sum(CASE when ct.class_family IN ('Premium') THEN jo.picked_up_deliveries ELSE 0 end) AS picked_up_deliveries_NF,
        sum(CASE when ct.class_family IN ('Premium') THEN jo.gross_deliveries ELSE 0 end) AS gross_deliveries_NF

        FROM "emilia_gettdwh"."dwh_fact_orders_v" fo
        LEFT JOIN (
                                SELECT
                                    j.order_gk, j.journey_id, j.is_jouney_aggregation, j.number_of_deliveries,
                                    j.total_customer_amount_exc_vat,
                                    COUNT(CASE WHEN d.delivery_status_id = 4 AND j.courier_gk <> 200013 THEN d.delivery_gk END) AS completed_deliveries, -- j.courier_gk = 200013 is a CC user that is used to cancel orders, all his orders should be marked AS cancelled
                                    COUNT(CASE WHEN d.delivery_status_id IN (4,7) AND j.courier_gk <> 200013 THEN d.delivery_gk END) AS picked_up_deliveries, -- j.courier_gk = 200013 is a CC user that is used to cancel orders, all his orders should be marked AS cancelled
                                    COUNT(d.delivery_gk) AS gross_deliveries
                                FROM model_delivery.dwh_fact_journeys_v AS j
                                LEFT JOIN model_delivery.dwh_fact_deliveries_v AS d ON d.journey_gk = j.journey_gk
                                WHERE 1 = 1
                                    AND j.country_symbol = 'RU'
                                    AND d.delivery_type_id <> 2 -- Returns
                                AND d.company_gk NOT IN (20001999) -- Test company
                                    AND j.date_key BETWEEN date'2019-01-01' AND CURRENT_DATE
                                GROUP BY 1,2,3,4,5
                    ) AS jo ON jo.order_gk=fo.order_gk

        -- class
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
          ON ct.class_type_key = fo.class_type_key
        -- company
        LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
          ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
        LEFT JOIN  emilia_gettdwh.dwh_dim_account_managers_v am ON am."account_manager_gk" = ca."account_manager_gk"
        -- city
        LEFT JOIN emilia_gettdwh.dwh_dim_locations_v AS loc
          ON loc.location_key = fo.origin_location_key
        -- supply type
        LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON d.driver_gk = fo.driver_gk
        LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v AS v ON v.vendor_gk = d.fleet_gk
        -- cancellation stage GT
        left join emilia_gettdwh.dwh_dim_order_cancellation_stages_v cst on fo.order_cancellation_stage_key = cst.order_cancellation_stage_key

        WHERE fo.country_key=2
          AND ct.lob_key IN (5,6)
          AND ct.class_group NOT LIKE 'Test'
          and ordering_corporate_account_gk not IN ( 20004730, 200017459, 20001999) --dummy delivery user and test company
          AND fo.date_key BETWEEN  (CURRENT_DATE - interval '7' day) and  (CURRENT_DATE - interval '1' day)
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23, 24,25,26,27,28
        ),

    fin as (
        select *,
      buy_sell - driver_total_commission_exc_vat as take_rate,
      (buy_sell - driver_total_commission_exc_vat) / nullif(customer_total_cost, 0) * 1.00 TR_perc,

        buy_sell - driver_total_commission_exc_vat < 0 negative_TR,
        ((buy_sell - driver_total_commission_exc_vat) / nullif(customer_total_cost, 0) * 1.00) > 100 over_100_TR_perc,

      avg(buy_sell - driver_total_commission_exc_vat) over (partition by company_gk, city, supply_type) as avg_take_rate_per_company_city_supply,
      avg(buy_sell - driver_total_commission_exc_vat) over (partition by class_type, city) as avg_take_rate_per_class_city_supply,
      avg((buy_sell - driver_total_commission_exc_vat) / nullif(customer_total_cost, 0) * 1.00) over (partition by company_gk, city, supply_type) avg_TRperc_per_company_city_supply,
    avg((buy_sell - driver_total_commission_exc_vat) / nullif(customer_total_cost, 0) * 1.00) over (partition by class_type, city) avg_TRperc_per_class_city_supply

      from all_orders
        order by take_rate asc
        )

(select *
from fin
--where take_rate < 0
)

select journey_id, order_gk, is_jouney_aggregation, number_of_deliveries, number_of_cancelled_deliveries, number_of_not_delivered_deliveries, number_of_parcels, number_of_completed_deliveries
from model_delivery.dwh_fact_journeys_v
where order_gk = 20001604232381
and country_symbol = 'RU'

SELECT *
from emilia_gettdwh.dwh_dim_order_cancellation_stages_v

