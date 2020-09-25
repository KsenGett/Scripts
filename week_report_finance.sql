with IML as (
    with t_total AS (
            with t_cost AS (
                with t_routes AS (
                    with t_orders AS (
                        SELECT d.id delivery_id, d.journey_id, d.status,
                         date(j.scheduled_at) AS date_key, j.supplier_id

                        FROM delivery.public.deliveries AS d
                        LEFT JOIN delivery.public.journeys AS j ON j.id = d.journey_id

                        WHERE d.env = 'RU'
                         and d.company_id = '24062'
                         and d.status IN ( 'completed' , 'not_delivered')
                         and date(j.scheduled_at) >= date'2020-05-01'
                        --ORDER BY delivery_id desc
                        ) -- orders

                    (SELECT
                        t1.journey_id, t1.date_key,
                        count (t1.delivery_id) AS deliveries,
                        sum(CASE when t1.status = 'completed' THEN 1 ELSE 0 end ) AS completed,
                        sum(CASE when t1.status = 'not_delivered' THEN 1 ELSE 0 end ) AS not_delivered,
                        sum(CASE when t1.status = 'completed' THEN 1 ELSE 0 end )*1.0/count (t1.delivery_id)*1.0 AS CR

                       FROM t_orders AS t1
                       GROUP BY 1,2)
                    ) -- routes

            (SELECT
                   t2.journey_id, t2.date_key,
                   t2.deliveries, t2.completed, t2.not_delivered, t2.CR,
                   (CASE when t2.CR > 0.7 THEN
                   (CASE when t2.completed <= 25 THEN 165
                         when t2.completed <= 30 THEN 150
                         ELSE 140  end)
                    ELSE
                     (CASE when t2.deliveries <=25 THEN 165
                           when t2.deliveries <=30 THEN 150
                           ELSE 140 end) end) AS price_per_stop
                FROM t_routes AS t2)
            ) --costs

             (SELECT
              t3.journey_id,
              t3.date_key,
              t3.deliveries,
              t3.completed,
              t3.CR,
              t3.completed * t3.price_per_stop AS total_price_wo_vat, --customer total cost
              t3.completed * t3.price_per_stop*1.2 AS total_price_w_vat
             FROM t_cost AS t3)
        ) --total

    (SELECT
    200024062 AS company_gk,
    t.date_key,
    sum (t.completed) AS completed_deliveries,
    sum(t.deliveries) AS deliveries,
    sum (t.total_price_wo_vat) AS total_price_wo_vat,
    sum (t.total_price_w_vat) AS total_price_w_vat
    FROM t_total AS t
    GROUP BY 1,2)

) -- iml finish

, p2p as (
    with p2p_fo AS ( --order leve
        SELECT fo.date_key,
           fo.hour_key,
           loc.city_name AS city,
           ct.lob_desc AS lob_desc,
           ct.class_type_desc AS class_type,
           fo.gt_order_gk AS order_gk, fo.sourceid, ct.class_family,
           (CASE when order_status_key = 7 THEN 'Completed' ELSE 'Cancelled ON Arrival' end) AS order_status,
           fo.ordering_corporate_account_gk AS company_gk,
           ca.corporate_account_name AS company_name,

           (CASE when ct.class_type_desc like '%c2c%' THEN '3. C2C' ELSE
                (CASE when am.name like '%Delivery%' THEN '1. eCommerce' ELSE '2. Corporate' end) end) AS Client_type,

          fo.customer_total_cost_inc_vat AS customer_total_cost_inc_vat,
          fo.customer_total_cost AS customer_total_cost ,
          fo.driver_total_cost_inc_vat AS driver_total_cost_inc_vat ,
          fo.driver_total_cost AS driver_total_cost ,
          fo.driver_total_commission_exc_vat AS driver_total_commission_exc_vat,

          (CASE when ct.class_family = 'Premium' THEN 'NF' ELSE 'OF' end) AS platform

    FROM emilia_gettdwh.dwh_fact_orders_v AS fo
    LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
      ON ct.class_type_key = fo.class_type_key
    LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
      ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
    LEFT JOIN "emilia_gettdwh"."dwh_dim_account_managers_v" am ON am."account_manager_gk" = ca."account_manager_gk"
    LEFT JOIN emilia_gettdwh.dwh_dim_locations_v AS loc
      ON loc.location_key = fo.origin_location_key

    WHERE fo.country_key=2
      AND ct.lob_key IN (5,6)
      AND ct.class_group NOT LIKE 'Test'
      --and ct.class_family not IN ('Premium')
      and (fo.order_status_key = 7 or (fo.order_status_key=4 and fo.driver_total_cost>0))
      AND fo.date_key >= date'2020-05-01'
      and fo.ordering_corporate_account_gk = 200025094;
  )
-- day level + IML
(SELECT p2p_fo.date_key, p2p_fo.platform,
       --p2p_fo.hour_key,
       --times
       tp.timecategory,
       tp.subperiod,
       tp.period,
       tp.subperiod2 AS time_period,

       city,
    lob_desc, class_type, client_type, p2p_fo.company_gk,
    company_name,
    order_status,
    accounts.name_internal AS company_name_united,

       count(distinct order_gk) AS gross_orders,
       sum(customer_total_cost_inc_vat) AS customer_total_cost_inc_vat,
       sum(customer_total_cost) AS customer_total_cost,
       sum(driver_total_cost_inc_vat) AS driver_total_cost_inc_vat,
       sum(driver_total_cost) AS driver_total_cost,
       sum(driver_total_commission_exc_vat)*(-1) AS driver_total_commission_exc_vat,
       sum(CASE when p2p_fo.class_family IN ('Premium') and p2p_fo.class_type not like '%ondemand% 'then jo.completed_deliveries
            when p2p_fo.class_family IN ('Premium') and p2p_fo.class_type like '%ondemand%' THEN jo.deliveries
         ELSE 0 end) AS deliveries_NF

       FROM p2p_fo
       LEFT JOIN sheets."default".delivery_corp_accounts_20191203 AS accounts ON cast(accounts.company_gk AS bigint)=p2p_fo.company_gk
       LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.hour_key = p2p_fo.hour_key and tp.date_key = p2p_fo.date_key
       and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
       -- to calculate number of deliveries in NF
       LEFT JOIN (
            SELECT j.legacy_order_id ,
            count(CASE when d.status = 'completed' THEN d.id end) AS completed_deliveries, count(d.id) AS deliveries

            FROM delivery.public.journeys AS j
            LEFT JOIN delivery.public.deliveries AS d ON d.journey_id=j.id

            WHERE 1=1 and (d.status = 'completed' or d.status='not_delivered') and j.env='RU'
            GROUP BY 1
            ) AS jo ON jo.legacy_order_id= p2p_fo.sourceid

       WHERE tp.timecategory is not null
       GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14))

(select p2p.date_key, p2p.platform,
       --times
p2p.timecategory, p2p.subperiod, p2p.period, p2p.time_period,
p2p.city, p2p.lob_desc, p2p.class_type, p2p.client_type, p2p.company_gk,
p2p.company_name, p2p.order_status, p2p.company_name_united,

p2p.gross_orders, p2p.deliveries_NF,
(case when p2p.company_gk = 200024062 then iml.total_price_w_vat else p2p.customer_total_cost_inc_vat end) as customer_total_cost_inc_vat,
(case when p2p.company_gk = 200024062 then iml.total_price_wo_vat else p2p.customer_total_cost end) as customer_total_cost,
driver_total_cost_inc_vat, driver_total_cost, driver_total_commission_exc_vat,

(CASE WHEN lob_desc = 'Deliveries - B2B'
    THEN
    -- iml correction
    (case when p2p.company_gk = 200024062 then iml.total_price_wo_vat else p2p.customer_total_cost end) - driver_total_cost_inc_vat
    ELSE (CASE
                --iml correction
        WHEN (case when p2p.company_gk = 200024062 then iml.total_price_w_vat else p2p.customer_total_cost_inc_vat end) - driver_total_cost_inc_vat >0
                --iml correction
        THEN round(((case when p2p.company_gk = 200024062 then iml.total_price_w_vat else p2p.customer_total_cost_inc_vat end) - driver_total_cost_inc_vat)/1.2,2)
                --iml correction
        ELSE (case when p2p.company_gk = 200024062 then iml.total_price_w_vat else p2p.customer_total_cost_inc_vat end) - driver_total_cost_inc_vat END)
    END) AS buy_sell

from p2p
left join IML on p2p.company_gk = iml.company_gk and iml.date_key = p2p.date_key);


--TODO ask about buy-sell
-- todo - check numbers in dynamic
-- todo ask about ata - why I have more orders and manually assigned
