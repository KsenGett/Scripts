---IML
    with iml as (
            with t_total AS (
                with t_cost AS (
                  with t_routes AS (
                   with t_orders AS (
                        SELECT
                         d.id AS delivery_id,
                         d.journey_id,
                         d.status,
                         date (j.scheduled_at) AS dates,
                         j.supplier_id
                        FROM delivery.public.deliveries AS d
                        LEFT JOIN delivery.public.journeys AS j ON j.id = d.journey_id --legacy_order_id
                        LEFT JOIN delivery.public.cancellation_infos AS c ON d.id = c.cancellable_id and c.cancellable_type = 'deliveries'
                        WHERE
                         d.env = 'RU'
                         and d.company_id = '24062'
                         and year(j.scheduled_at) > 2018
                         and d.status IN ( 'completed' , 'not_delivered')
                        ORDER BY delivery_id desc
                        )

                    (SELECT
                    t1.journey_id,
                    t1.dates,
                    count (t1.delivery_id) AS deliveries,
                    sum(CASE when t1.status = 'completed' THEN 1 ELSE 0 end ) AS completed,
                    sum(CASE when t1.status = 'not_delivered' THEN 1 ELSE 0 end ) AS not_delivered,
                    sum(CASE when t1.status = 'completed' THEN 1 ELSE 0 end )*1.0/count (t1.delivery_id)*1.0 AS CR
                    FROM t_orders AS t1
                    GROUP BY 1,2)
                    )
                (SELECT
                t2.journey_id,
                t2.dates,
                t2.deliveries,
                t2.completed,
                t2.not_delivered,
                t2.CR, 0 as hour_key,

                (CASE when t2.CR > 0.7 THEN
                (CASE when t2.completed <= 25 THEN 165
                             when t2.completed <= 30 THEN 150
                             ELSE 140  end)
                    ELSE
                         (CASE when t2.deliveries <=25 THEN 165
                               when t2.deliveries <=30 THEN 150
                               ELSE 140 end) end) AS price_per_stop
                FROM t_routes AS t2)

                )
            (SELECT
            t3.journey_id,
            t3.dates,
            t3.deliveries,
            t3.completed,
            t3.price_per_stop,
            t3.CR,
            t3.completed * t3.price_per_stop AS total_price_wo_vat, --customer total cost
            t3.completed * t3.price_per_stop*1.2 AS total_price_w_vat,
            tp.timecategory,
            tp.subperiod,
            tp.period,
            tp.subperiod2 AS time_period,
            '200024062' AS company_gk
                 FROM t_cost AS t3
                    LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.hour_key = t3.hour_key and tp.date_key = t3.dates
                    and tp.timecategory IN ('1.Hours', '2.Dates', '3.Weeks', '4.Months', '5.Quarters'))
                )
        (SELECT
        '200024062' AS company_gk,
        t.dates AS date,
        sum (t.completed) AS completed_deliveries,
        sum(t.deliveries) AS deliveries,
        sum (t.total_price_wo_vat) AS total_price_wo_vat,
        sum (t.total_price_w_vat) AS total_price_w_vat
        FROM t_total AS t
        GROUP BY 1,2));



---Delivery performance
    with iml as (
    --IML start
                with t_cost AS (
                  with t_routes AS (
                   with t_orders AS (
                        SELECT
                         d.id AS delivery_id,
                         d.journey_id,
                         jo.legacy_order_id,
                         d.status,
                         date (jo.scheduled_at) AS dates,
                         jo.supplier_id
                        FROM delivery.public.deliveries AS d
                        LEFT JOIN delivery.public.journeys AS jo ON jo.id = d.journey_id
                        LEFT JOIN delivery.public.cancellation_infos AS c ON d.id = c.cancellable_id
                            and c.cancellable_type = 'deliveries'

                        WHERE
                         d.env = 'RU'
                         and d.company_id = '24062'
                         and year(jo.scheduled_at) > 2018
                         and d.status IN ( 'completed' , 'not_delivered')
                        ORDER BY delivery_id desc
                        )

                    (SELECT
                    t1.journey_id,
                    t1.legacy_order_id,
                    t1.dates,
                    count (t1.delivery_id) AS deliveries,
                    sum(CASE when t1.status = 'completed' THEN 1 ELSE 0 end ) AS completed,
                    sum(CASE when t1.status = 'not_delivered' THEN 1 ELSE 0 end ) AS not_delivered,
                    sum(CASE when t1.status = 'completed' THEN 1 ELSE 0 end )*1.0/count (t1.delivery_id)*1.0 AS CR
                    FROM t_orders AS t1
                    GROUP BY 1,2,3)
                    )
                (SELECT
                t2.journey_id,
                t2.legacy_order_id,
                t2.dates,
                t2.deliveries,
                t2.completed,
                t2.not_delivered,
                t2.CR, 0 as hour_key,

                (CASE when t2.CR > 0.7 THEN
                (CASE when t2.completed <= 25 THEN 165
                             when t2.completed <= 30 THEN 150
                             ELSE 140  end)
                    ELSE
                         (CASE when t2.deliveries <=25 THEN 165
                               when t2.deliveries <=30 THEN 150
                               ELSE 140 end) end) AS price_per_stop
                FROM t_routes AS t2)

                )
            (SELECT
            '200024062' AS company_gk,
            t3.journey_id,
            t3.legacy_order_id, -- to attach to p2p
            t3.dates,
            t3.deliveries,
            t3.completed,
            t3.price_per_stop,
            t3.CR,
            t3.completed * t3.price_per_stop AS customer_total_cost_wo_vat,
            t3.completed * t3.price_per_stop*1.2 AS customer_total_cost_w_vat
                 FROM t_cost AS t3

    --IML finish
    )

with p2p_fo AS (
    (SELECT fo.date_key AS dates,
           fo.hour_key,
           loc.city_name AS city,
           ct.lob_desc AS lob_desc,
           ct.class_type_desc AS class_type,
           fo.gt_order_gk AS order_gk,
           (CASE when order_status_key = 7 THEN 'Completed' ELSE 'Cancelled ON Arrival' end) AS order_status,
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
           (CASE when ct.class_type_desc like '%c2c%' THEN 3 ELSE
           (CASE when am.name like '%Delivery%' THEN 1 ELSE 2 end) end) AS Client_type_key,
           (CASE when ct.class_type_desc like '%c2c%' THEN 'C2C' ELSE
           (CASE when am.name like '%Delivery%' THEN 'eCommerce' ELSE 'Corporate' end) end) AS Client_type_desc,
            fo.customer_total_cost_inc_vat AS customer_total_cost_inc_vat,

          case when fo.ordering_corporate_account_gk = 200024062 then iml.customer_total_cost_wo_vat
           else fo.customer_total_cost end AS customer_total_cost ,

          fo.driver_total_cost_inc_vat AS driver_total_cost_inc_vat ,
          fo.driver_total_cost AS driver_total_cost ,
          fo.driver_total_commission_exc_vat AS driver_total_commission_exc_vat,
         -- + IML correction
         (CASE
                WHEN lob_desc = 'Deliveries - B2B'
                THEN fo.customer_total_cost - driver_total_cost_inc_vat
                ELSE (CASE
                          WHEN fo.customer_total_cost_inc_vat - driver_total_cost_inc_vat > 0
                          THEN round((fo.customer_total_cost_inc_vat - driver_total_cost_inc_vat)/1.2,2)
                          ELSE fo.customer_total_cost_inc_vat end) - driver_total_cost_inc_vat
                      END) AS buy_sell,

        sum(CASE when ct.class_family IN ('Premium') and ct.class_type_desc not like '%ondemand% 'then jo.completed_deliveries
        when ct.class_family IN ('Premium') and ct.class_type_desc like '%ondemand%' THEN jo.deliveries
        ELSE 0 end) AS deliveries_NF

        FROM "emilia_gettdwh"."dwh_fact_orders_v" fo

        LEFT JOIN (
            SELECT
            j.legacy_order_id ,
            count(CASE when d.status = 'completed' THEN d.id end) AS completed_deliveries,
            count(d.id) AS deliveries

            FROM delivery.public.journeys AS j
            LEFT JOIN delivery.public.deliveries AS d ON d.journey_id=j.id

            WHERE 1=1
            and (d.status = 'completed' or d.status='not_delivered')
            and j.env='RU'
            GROUP BY 1
            ) AS jo ON jo.legacy_order_id=fo.sourceid
        --class
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
          ON ct.class_type_key = fo.class_type_key
        --corportae account
        LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
          ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
        LEFT JOIN "emilia_gettdwh"."dwh_dim_account_managers_v" am ON am."account_manager_gk" = ca."account_manager_gk"
        --locations
        LEFT JOIN emilia_gettdwh.dwh_dim_locations_v AS loc
          ON loc.location_key = fo.origin_location_key
        --drivers
        LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON d.driver_gk = fo.driver_gk
        --fleets
        LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v AS v ON v.vendor_gk = d.fleet_gk

        WHERE fo.country_key=2
          AND ct.lob_key IN (5,6)
          AND ct.class_group NOT LIKE 'Test'
          and ordering_corporate_account_gk not IN ( 20004730, 200017459) --dummy delivery user and test company
          --and ct.class_family not IN ('Premium')
          and (fo.order_status_key = 7 or (fo.order_status_key=4 and fo.driver_total_cost>0)) -- cancelled ON arrival
          AND fo.date_key BETWEEN date'2019-01-01' AND (CURRENT_DATE - interval '1' day)
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21)
  )

(SELECT dates,
       p2p_fo.hour_key,
       tp.timecategory,
       tp.subperiod,
       tp.period,
       tp.subperiod2 AS time_period,
       city,
       platform,
       lob_desc,
       class_type,
       client_type_key,
       client_type_desc,
       p2p_fo.company_gk,
       company_name,
       account_manager,
       supply_type,
       order_status,
       accounts.name_internal AS company_name_united,
       went_to_cc,
       sum(CASE when platform = 'OF' THEN 1
         when platform = 'NF' THEN deliveries_NF
         ELSE 0 end) AS deliveries,
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
       GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19);

select *
from emilia_gettdwh.dwh_fact_orders_v
where ordering_corporate_account_gk = 200024062
limit 2;





