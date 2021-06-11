/*
Owner - Kozlova Kseniia
Cube Name - Margine Track
ID - C27C11BE11EB978AF5DB0080EFD5EA07
*/

    with all_orders as (

        SELECT fo.date_key,
               loc.city_name AS city,
               --areas_pick.area_desc area_pick, areas_drop.area_desc area_drop,
               ct.lob_desc AS lob_desc,
               ct.class_type_desc AS class_type,
               fo.gt_order_gk AS order_gk, jo.journey_id,
               ct.class_family = 'Premium' and jo.order_gk is null hard_reset,
               --fo.est_distance, fo.est_duration, fo.ride_distance_key ride_distance,
               CASE when fo.driver_gk = 200013 THEN 4 ELSE order_status_key end AS order_status_key,
               order_cancellation_stage_desc,
               fo.origin_full_address,
               (CASE when v.vendor_name like '%courier car%' THEN 'PHV'
                    when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                    when v.vendor_name like '%courier scooter%' THEN 'scooter'
                    ELSE 'taxi'
               end) AS supply_type,
               (CASE when ct.class_family = 'Premium' THEN 'NF' ELSE 'OF' end) AS platform,
               fo.ordering_corporate_account_gk AS company_gk,
               coalesce(ca.company_name, ca_of.corporate_account_name) company_name,
               ca.is_new_pricing_service,
               am.name AS account_manager,
         (CASE when fo.lob_key = 6 THEN 3 ELSE
        (CASE when am.name like '%Delivery%' THEN 1 ELSE 2 end) end) AS Client_type_key,

                (CASE when fo.lob_key = 6 THEN 'C2C' ELSE
               (CASE when am.name like '%Delivery%' THEN 'eCommerce' ELSE 'Corporate' end) end) AS Client_type_desc,
              (case when ct.class_family = 'Premium' then total_customer_amount_exc_vat*1.2 else fo.customer_total_cost_inc_vat end) AS customer_total_cost_inc_vat,
             (case when ct.class_family = 'Premium' then total_customer_amount_exc_vat else fo.customer_total_cost end) AS customer_total_cost ,
              fo.driver_total_cost_inc_vat AS driver_total_cost_inc_vat ,
              fo.driver_total_cost AS driver_total_cost ,
              fo.driver_total_commission_exc_vat AS driver_total_commission_exc_vat,
             (CASE
                    WHEN lob_desc = 'Deliveries - B2B'
                    THEN
                    (case when ct.class_family = 'Premium'
                         then total_customer_amount_exc_vat - driver_total_cost_inc_vat
                         else customer_total_cost - driver_total_cost_inc_vat end)
                    ELSE (CASE
                              WHEN customer_total_cost_inc_vat - driver_total_cost_inc_vat >0
                              THEN round((customer_total_cost_inc_vat - driver_total_cost_inc_vat)/1.2,2)
                              ELSE customer_total_cost_inc_vat - driver_total_cost_inc_vat
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
                                    j.order_gk, j.journey_id,
                                    j.total_customer_amount_exc_vat,
                                    COUNT(CASE WHEN d.delivery_status_id = 4 AND j.courier_gk <> 200013 THEN d.delivery_gk END) AS completed_deliveries, -- j.courier_gk = 200013 is a CC user that is used to cancel orders, all his orders should be marked AS cancelled
                                    COUNT(CASE WHEN d.delivery_status_id IN (4,7) AND j.courier_gk <> 200013 THEN d.delivery_gk END) AS picked_up_deliveries, -- j.courier_gk = 200013 is a CC user that is used to cancel orders, all his orders should be marked AS cancelled
                                    COUNT(d.delivery_gk) AS gross_deliveries
                                FROM model_delivery.dwh_fact_journeys_v AS j
                                LEFT JOIN model_delivery.dwh_fact_deliveries_v AS d ON d.journey_gk = j.journey_gk
                                WHERE 1 = 1
                                    AND j.country_symbol = 'RU'
                                    AND d.delivery_type_id <> 2 -- Returns
                                AND ((d.company_gk NOT IN (20001999)) or (d.company_gk is null)) -- Test company
                                    AND j.date_key BETWEEN date'2021-02-01' AND CURRENT_DATE

                                GROUP BY 1,2,3
                    ) AS jo ON jo.order_gk=fo.order_gk

        -- class
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
          ON ct.class_type_key = fo.class_type_key
        -- company
        LEFT JOIN "model_delivery"."dwh_dim_delivery_companies_v" AS ca
          ON ca.company_gk = fo.ordering_corporate_account_gk
        LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca_of
                  ON ca_of.corporate_account_gk = fo.ordering_corporate_account_gk
        LEFT JOIN  emilia_gettdwh.dwh_dim_account_managers_v am ON am."account_manager_gk" = ca."account_manager_gk"
        -- city
        LEFT JOIN emilia_gettdwh.dwh_dim_locations_v AS loc
          ON loc.location_key = fo.origin_location_key
        -- supply type
        LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON d.driver_gk = fo.driver_gk
        LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v AS v ON v.vendor_gk = d.fleet_gk
        left join "emilia_gettdwh"."dwh_dim_order_cancellation_stages_v" cs on cs.order_cancellation_stage_key = fo.order_cancellation_stage_key
--         left join  "emilia_gettdwh"."dwh_dim_areas_v" areas_pick on ST_Contains(ST_GeometryFromText(areas_pick.borders),
--                         ST_Point(fo.origin_longitude, fo.origin_latitude))
--                         and areas_pick."area_gk" in (20001512,200086625,200086666,200086687) -- mkad
--          left join  "emilia_gettdwh"."dwh_dim_areas_v" areas_drop on ST_Contains(ST_GeometryFromText(areas_drop.borders),
--                         ST_Point(fo.dropoff_longitude, fo.dropoff_latitude))
--                         and areas_drop."area_gk" in (20001512,200086625,200086666,200086687) -- mkad


        WHERE fo.country_key=2
          AND ct.lob_key IN (5,6)
          --and journey_id = 2439384
          AND ct.class_group NOT LIKE 'Test'
          --and corporate_account_gk = 200025410
          and ordering_corporate_account_gk not IN ( 20004730, 200017459, 20001999) --dummy delivery user and test company
          --AND fo.date_key BETWEEN  (CURRENT_DATE - interval '60' day) and  (CURRENT_DATE - interval '1' day)
          AND fo.date_key >= date'2021-05-01'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24--,25,26,27,28,29
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
    --where company_gk = 20004744
where (company_gk in (200025576,200022121,200025540) or (company_gk is null))
    and journey_id =  2864363

)


SELECT fo.fleet_gk, fl.vendor_name, sum(fo.driver_total_commission_exc_vat) / sum(fo.driver_total_cost) as commission

From emilia_gettdwh.dwh_fact_orders_v fo
Left join emilia_gettdwh.dwh_dim_vendors_v fl on fl.vendor_gk = fo.fleet_gk

Where fo.lob_key in (5,6)
And fo.country_key = 2
And fo.date_key >= date '2021-04-30'
And fo.ordering_corporate_account_gk <> 20004730
and fl.vendor_name like '%courier%'

Group by 1,2;

select f.external_id, pa.active_from, p.id from "commission".public.plan_assignments pa
join "commission".public.fleets f on f.id = pa.assignee_id and pa.assignee_type = 'Fleet' and f.env = 'RU'
join "commission".public.plans p on p.id = pa.plan_id
where p."type" = 'Plan::FleetPlan'
and pa.active_to is null
and pa.env = 'RU'
and pa.assignee_type = 'Fleet'
and f.external_id in (14111)

-- Moscow Region

with new_drivers as
            (
            select
                ftr.driver_gk,
                dd.phone as driver_phone,
                dd.fleet_gk as ftr_fleet,
                fl.vendor_name ftr_fleet_name,
                substring(fl.vendor_name, 1, position('/' in fl.vendor_name)-1) as ftr_fleet_name_short, --delete supply type
                cast(ftr.date_key as date) as ftr_date,
                ftr.ride_type,
                date_add('month', 1, cast(ftr.date_key as date)) ftr_date_plus_1_month

            from temp.reftr_delivery ftr
            left join emilia_gettdwh.dwh_dim_drivers_v dd on ftr.driver_gk = dd.driver_gk and dd.country_key = 2
            left join emilia_gettdwh.dwh_dim_vendors_v fl on dd.fleet_gk = fl.vendor_gk and fl.country_key = 2

            where true
                and dd.fleet_gk in ({fleets_mo})
                and cast(ftr.date_key as date) >= date '{date_start}'
            )
    , stat_first_month as
        (
          select
          p.driver_id,
          fnr.driver_phone,
          fnr.ftr_fleet, fnr.ftr_fleet_name_short, fnr.ftr_fleet_name,
          fnr.ride_type,
          fnr.ftr_date,
          fnr.ftr_date_plus_1_month,
          count (*) as rides_1st_month,
                    count (case when cast (p.scheduled_at + interval '3' hour  as date)
                  between date_add('day', -1, fnr.ftr_date) and date_add('day', 6, fnr.ftr_date) then 1 else null end) as rides_7_days,
          count (case when cast (p.scheduled_at + interval '3' hour  as date)
                  between date_add('day', -1, fnr.ftr_date) and date_add('day', 14, fnr.ftr_date) then 1 else null end) as rides_14_days,
          count (case when cast (p.scheduled_at + interval '3' hour  as date)
                  between date_add('day', -1, fnr.ftr_date) and date_add('day', 20, fnr.ftr_date) then 1 else null end) as rides_20_days,
          count (case when cast (p.scheduled_at + interval '3' hour  as date)
                  between date_add('day', -1, fnr.ftr_date) and date_add('day', 30, fnr.ftr_date) then 1 else null end) as rides_30_days

          from gettaxi_ru_production.orders p
          join new_drivers fnr on fnr.driver_gk  = CAST ('2000' || cast (p.driver_id as varchar) AS BIGINT)
                        and cast (p.scheduled_at + interval '3' hour  as date)
                        between date_add('day', -1, fnr.ftr_date) and fnr.ftr_date_plus_1_month
            --join emilia_gettdwh.dwh_dim_class_types_v dct on cast ((SUBSTRING(cast (dct.class_type_key as varchar), 5)) as integer)= p.division_id
          join "emilia_gettdwh"."dwh_dim_areas_v" areas  on ST_Contains(ST_GeometryFromText(areas.borders)
            , ST_Point(p.origin_lon, p.origin_lat)) and "area_desc" like '%Moscow delivery acquisition%'

          where true
            and p.status_id = 7
            and (p.company_id != 4730 or p.company_id is null)
            group by 1,2,3,4,5,6,7,8
        )
, rides as (
    select
    st.*,
    --case when week (cast (p.scheduled_at + interval '3' hour  as date)) in (1,2,3,4,5,6)
        --then week (cast (p.scheduled_at + interval '3' hour  as date)) + 53
        --else week (cast (p.scheduled_at + interval '3' hour  as date)) end as weeks,
    year (cast (p.scheduled_at + interval '3' hour  as date)) as years,
    week (cast (p.scheduled_at + interval '3' hour  as date)) as week,
    count (*) as rides_this_week
    from gettaxi_ru_production.orders p
    join stat_first_month st on p.driver_id = st.driver_id
          and cast (p.scheduled_at + interval '3' hour  as date) between date_add('day', -1, st.ftr_date) and st.ftr_date_plus_1_month
    where true
        and p.status_id = 7
        and (p.company_id != 4730 or p.company_id is null)
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
        )
(select
    r.*,
    row_number () over (partition by r.driver_id order by r.driver_id, r.week) week_num,
    sum (r.rides_this_week) over (partition by r.ftr_fleet, r.driver_id,
    r.ftr_date order by week asc rows between unbounded preceding and current row) as cumsum

from rides r
where true
order by r.driver_id, r.week
);


--Bringo
    with all_orders as (

        SELECT fo.date_key,
               loc.city_name AS city,
               coalesce(areas_pick.area_desc,areas_pick_10.area_desc,areas_pick_20.area_desc, areas_pick_30.area_desc) area_pick,
                coalesce(areas_drop.area_desc,areas_drop_10.area_desc,areas_drop_20.area_desc, areas_drop_30.area_desc) area_drop,

               ct.lob_desc AS lob_desc,
               ct.class_type_desc AS class_type,
               fo.gt_order_gk AS order_gk, jo.journey_id,
               ct.class_family = 'Premium' and jo.order_gk is null hard_reset,
               fo.est_distance, fo.est_duration, fo.ride_distance_key ride_distance,
               CASE when fo.driver_gk = 200013 THEN 4 ELSE order_status_key end AS order_status_key,
               order_cancellation_stage_desc,
               fo.origin_full_address, fo.dest_full_address,
               (CASE when v.vendor_name like '%courier car%' THEN 'PHV'
                    when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                    when v.vendor_name like '%courier scooter%' THEN 'scooter'
                    ELSE 'taxi'
               end) AS supply_type,
               (CASE when ct.class_family = 'Premium' THEN 'NF' ELSE 'OF' end) AS platform,
               fo.ordering_corporate_account_gk AS company_gk,
               coalesce(ca.company_name, ca_of.corporate_account_name) company_name,
               ca.is_new_pricing_service,
               am.name AS account_manager,
         (CASE when fo.lob_key = 6 THEN 3 ELSE
        (CASE when am.name like '%Delivery%' THEN 1 ELSE 2 end) end) AS Client_type_key,

                (CASE when fo.lob_key = 6 THEN 'C2C' ELSE
               (CASE when am.name like '%Delivery%' THEN 'eCommerce' ELSE 'Corporate' end) end) AS Client_type_desc,
              (case when ct.class_family = 'Premium' then total_customer_amount_exc_vat*1.2 else fo.customer_total_cost_inc_vat end) AS customer_total_cost_inc_vat,
             (case when ct.class_family = 'Premium' then total_customer_amount_exc_vat else fo.customer_total_cost end) AS customer_total_cost ,
              fo.driver_total_cost_inc_vat AS driver_total_cost_inc_vat ,
              fo.driver_total_cost AS driver_total_cost ,
              fo.driver_total_commission_exc_vat AS driver_total_commission_exc_vat,
             (CASE
                    WHEN lob_desc = 'Deliveries - B2B'
                    THEN
                    (case when ct.class_family = 'Premium'
                         then total_customer_amount_exc_vat - driver_total_cost_inc_vat
                         else customer_total_cost - driver_total_cost_inc_vat end)
                    ELSE (CASE
                              WHEN customer_total_cost_inc_vat - driver_total_cost_inc_vat >0
                              THEN round((customer_total_cost_inc_vat - driver_total_cost_inc_vat)/1.2,2)
                              ELSE customer_total_cost_inc_vat - driver_total_cost_inc_vat
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
                                    j.order_gk, j.journey_id,
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
                                    AND j.date_key BETWEEN date'2021-02-01' AND CURRENT_DATE
                                GROUP BY 1,2,3
                    ) AS jo ON jo.order_gk=fo.order_gk

        -- class
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
          ON ct.class_type_key = fo.class_type_key
        -- company
        LEFT JOIN "model_delivery"."dwh_dim_delivery_companies_v" AS ca
          ON ca.company_gk = fo.ordering_corporate_account_gk
        LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca_of
                  ON ca_of.corporate_account_gk = fo.ordering_corporate_account_gk
        LEFT JOIN  emilia_gettdwh.dwh_dim_account_managers_v am ON am."account_manager_gk" = ca."account_manager_gk"
        -- city
        LEFT JOIN emilia_gettdwh.dwh_dim_locations_v AS loc
          ON loc.location_key = fo.origin_location_key
        -- supply type
        LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON d.driver_gk = fo.driver_gk
        LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v AS v ON v.vendor_gk = d.fleet_gk
        left join "emilia_gettdwh"."dwh_dim_order_cancellation_stages_v" cs on cs.order_cancellation_stage_key = fo.order_cancellation_stage_key

         left join  "emilia_gettdwh"."dwh_dim_areas_v" areas_pick on ST_Contains(ST_GeometryFromText(areas_pick.borders),
                        ST_Point(fo.origin_longitude, fo.origin_latitude))
                        and areas_pick."area_gk" = (20001512) -- mkad
            left join  "emilia_gettdwh"."dwh_dim_areas_v" areas_pick_10 on ST_Contains(ST_GeometryFromText(areas_pick_10.borders),
                        ST_Point(fo.origin_longitude, fo.origin_latitude))
                        and areas_pick_10."area_gk" = (200086625) -- mkad 10+
         left join  "emilia_gettdwh"."dwh_dim_areas_v" areas_pick_20 on ST_Contains(ST_GeometryFromText(areas_pick_20.borders),
                        ST_Point(fo.origin_longitude, fo.origin_latitude))
                        and areas_pick_20."area_gk" in (200086666) -- mkad 20+
        left join  "emilia_gettdwh"."dwh_dim_areas_v" areas_pick_30 on ST_Contains(ST_GeometryFromText(areas_pick_30.borders),
                        ST_Point(fo.origin_longitude, fo.origin_latitude))
                        and areas_pick_30."area_gk" in (200086687)

            left join  "emilia_gettdwh"."dwh_dim_areas_v" areas_drop on ST_Contains(ST_GeometryFromText(areas_drop.borders),
                        ST_Point(fo.dropoff_longitude, fo.dropoff_latitude))
                        and areas_drop."area_gk" = (20001512) -- mkad
             left join  "emilia_gettdwh"."dwh_dim_areas_v" areas_drop_10 on ST_Contains(ST_GeometryFromText(areas_drop_10.borders),
                        ST_Point(fo.dropoff_longitude, fo.dropoff_latitude))
                        and areas_drop_10."area_gk" = (200086625) -- mkad 10+
         left join  "emilia_gettdwh"."dwh_dim_areas_v" areas_drop_20 on ST_Contains(ST_GeometryFromText(areas_drop_20.borders),
                        ST_Point(fo.dropoff_longitude, fo.dropoff_latitude))
                        and areas_drop_20."area_gk" in (200086666) -- mkad 20+
        left join  "emilia_gettdwh"."dwh_dim_areas_v" areas_drop_30 on ST_Contains(ST_GeometryFromText(areas_drop_30.borders),
                        ST_Point(fo.dropoff_longitude, fo.dropoff_latitude))
                        and areas_drop_30."area_gk" in (200086687)


        WHERE fo.country_key=2
          AND ct.lob_key IN (5,6)
          --and journey_id = 2439384
          AND ct.class_group NOT LIKE 'Test'
          and corporate_account_gk = 20004744
          and ordering_corporate_account_gk not IN ( 20004730, 200017459, 20001999) --dummy delivery user and test company
          AND fo.date_key BETWEEN  (CURRENT_DATE - interval '60' day) and  (CURRENT_DATE - interval '1' day)
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
        ),

    fin as (
        select *,
      buy_sell - driver_total_commission_exc_vat as take_rate,
      (buy_sell - driver_total_commission_exc_vat) / nullif(customer_total_cost, 0) * 1.00 TR_perc
      from all_orders
        order by take_rate asc
        )

(select *
from fin
    where company_gk = 20004744
--where take_rate < 0
)
;


-- for margine research
with all_orders as (

        SELECT fo.date_key,
               loc.city_name AS city,
               areas_pick.area_desc area_pick, areas_drop.area_desc area_drop,
               ct.lob_desc AS lob_desc,
               ct.class_type_desc AS class_type,
               fo.gt_order_gk AS order_gk, jo.journey_id,
               ct.class_family = 'Premium' and jo.order_gk is null hard_reset,
               fo.est_distance, fo.est_duration, fo.ride_distance_key ride_distance,
               CASE when fo.driver_gk = 200013 THEN 4 ELSE order_status_key end AS order_status_key,
               order_cancellation_stage_desc,
               fo.origin_full_address,
               (CASE when v.vendor_name like '%courier car%' THEN 'PHV'
                    when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                    when v.vendor_name like '%courier scooter%' THEN 'scooter'
                    ELSE 'taxi'
               end) AS supply_type,
               (CASE when ct.class_family = 'Premium' THEN 'NF' ELSE 'OF' end) AS platform,
               fo.ordering_corporate_account_gk AS company_gk,
               coalesce(ca.company_name, ca_of.corporate_account_name) company_name,
               ca.is_new_pricing_service,
               am.name AS account_manager,
         (CASE when fo.lob_key = 6 THEN 3 ELSE
        (CASE when am.name like '%Delivery%' THEN 1 ELSE 2 end) end) AS Client_type_key,

                (CASE when fo.lob_key = 6 THEN 'C2C' ELSE
               (CASE when am.name like '%Delivery%' THEN 'eCommerce' ELSE 'Corporate' end) end) AS Client_type_desc,
              (case when ct.class_family = 'Premium' then total_customer_amount_exc_vat*1.2 else fo.customer_total_cost_inc_vat end) AS customer_total_cost_inc_vat,
             (case when ct.class_family = 'Premium' then total_customer_amount_exc_vat else fo.customer_total_cost end) AS customer_total_cost ,
              fo.driver_total_cost_inc_vat AS driver_total_cost_inc_vat ,
              fo.driver_total_cost AS driver_total_cost ,
              fo.driver_total_commission_exc_vat AS driver_total_commission_exc_vat,
             (CASE
                    WHEN lob_desc = 'Deliveries - B2B'
                    THEN
                    (case when ct.class_family = 'Premium'
                         then total_customer_amount_exc_vat - driver_total_cost_inc_vat
                         else customer_total_cost - driver_total_cost_inc_vat end)
                    ELSE (CASE
                              WHEN customer_total_cost_inc_vat - driver_total_cost_inc_vat >0
                              THEN round((customer_total_cost_inc_vat - driver_total_cost_inc_vat)/1.2,2)
                              ELSE customer_total_cost_inc_vat - driver_total_cost_inc_vat
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
                                    j.order_gk, j.journey_id,
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
                                    AND j.date_key BETWEEN date'2021-02-01' AND CURRENT_DATE
                                GROUP BY 1,2,3
                    ) AS jo ON jo.order_gk=fo.order_gk

        -- class
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
          ON ct.class_type_key = fo.class_type_key
        -- company
        LEFT JOIN "model_delivery"."dwh_dim_delivery_companies_v" AS ca
          ON ca.company_gk = fo.ordering_corporate_account_gk
        LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca_of
                  ON ca_of.corporate_account_gk = fo.ordering_corporate_account_gk
        LEFT JOIN  emilia_gettdwh.dwh_dim_account_managers_v am ON am."account_manager_gk" = ca."account_manager_gk"
        -- city
        LEFT JOIN emilia_gettdwh.dwh_dim_locations_v AS loc
          ON loc.location_key = fo.origin_location_key
        -- supply type
        LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON d.driver_gk = fo.driver_gk
        LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v AS v ON v.vendor_gk = d.fleet_gk
        left join "emilia_gettdwh"."dwh_dim_order_cancellation_stages_v" cs on cs.order_cancellation_stage_key = fo.order_cancellation_stage_key
        left join  "emilia_gettdwh"."dwh_dim_areas_v" areas_pick on ST_Contains(ST_GeometryFromText(areas_pick.borders),
                        ST_Point(fo.origin_longitude, fo.origin_latitude))
                        and areas_pick."area_gk" in (20001512,200086625,200086666,200086687) -- mkad
         left join  "emilia_gettdwh"."dwh_dim_areas_v" areas_drop on ST_Contains(ST_GeometryFromText(areas_drop.borders),
                        ST_Point(fo.dropoff_longitude, fo.dropoff_latitude))
                        and areas_drop."area_gk" in (20001512,200086625,200086666,200086687) -- mkad


        WHERE fo.country_key=2
          AND ct.lob_key IN (5,6)
          --and journey_id = 2439384
          AND ct.class_group NOT LIKE 'Test'
          --and corporate_account_gk = 200025410
          and ordering_corporate_account_gk not IN ( 20004730, 200017459, 20001999) --dummy delivery user and test company
          AND fo.date_key BETWEEN  (CURRENT_DATE - interval '60' day) and  (CURRENT_DATE - interval '1' day)
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29
        ),

    fin as (
        select *,
      buy_sell - driver_total_commission_exc_vat as take_rate,
      (buy_sell - driver_total_commission_exc_vat) / nullif(customer_total_cost, 0) * 1.00 TR_perc,

        case when buy_sell - driver_total_commission_exc_vat < 0 then  negative_TR


      from all_orders
        order by take_rate asc
        )

(select *
from fin
   -- where company_gk = 20004744
--where take_rate < 0
);



