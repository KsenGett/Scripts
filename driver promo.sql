--select count(distinct driver_gk)

--from
--(
with AR AS (
    SELECT
     date_key,
     driver_gk,
     SUM(  CASE WHEN fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) AS numerator,
                (SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL THEN 1 ELSE 0 END)
                - SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL AND fof.Is_Withdrawned=1
                AND fof.Driver_Response_Key<>1 THEN 1 ELSE 0 END) )  AS denominator
    FROM emilia_gettdwh.dwh_fact_offers_v fof
     LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v cl ON cl.class_type_key = fof.class_type_key
    WHERE lob_key IN (5,6)
    and date_key >= date'2020-08-01'
    GROUP BY 1,2
),

GH AS (
    SELECT driver_gk,
    date_key,
    sum(CASE
                                      when fdh.driver_status_key IN (2, 4, 5, 6)
                                              THEN fdh.minutes_in_status
                                      ELSE 0 end)/60.0 AS gh
    FROM emilia_gettdwh.dwh_fact_drivers_hourly_v fdh
     WHERE   1 = 1
                and fdh.date_key >= date'2020-08-01'
                and fdh.country_key = 2
                and driver_gk IN (SELECT cast(driver_gk AS bigint) FROM "sheets"."default".driver_promo_3500)
                GROUP BY 1,2
),

-- fleet_commission AS ( SELECT order_id,
-- cast(json_extract_scalar(metadata, '$.amount_inc_tax') AS decimal(10,2)) AS fleet_commission_inc_Vat
-- FROM "driverearnings"."public"."transactions_v"
--  WHERE env = 'ru'
-- and transaction_type_id = 71 -- fleet commission
-- and "created_at" >= date '2020-08-31'
-- ),

driver_promo AS
(
    SELECT dp.driver_gk,
    vendor_name,
    dd."fleet_gk",
    min(dp.from_date_key) AS from_date_key
    FROM "sheets"."default".driver_promo_3500 dp
    LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" dd ON dd."driver_gk" = cast(dp.driver_gk AS bigint)
    LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v v ON v.vendor_gk = dd.fleet_gk

    GROUP BY 1,2,3
)


SELECT
            dp.driver_gk,
            dd.driver_name,
            v."vendor_gk",
            v.vendor_name,
            gh.date_key,
            ar.numerator*1.000/nullif(ar.denominator,0) AS AR,
            gh,
            count(distinct CASE when order_status_key = 7 THEN order_gk ELSE null end) AS completed_orders,
            sum("driver_total_cost") AS driver_total_cost,
             sum("driver_total_commission_inc_vat") AS driver_total_comission_inc_vat
           --sum(fc.fleet_commission_inc_Vat) AS fleet_commission_inc_vat
     FROM driver_promo dp
     LEFT JOIN gh ON gh.driver_gk = cast(dp.driver_gk as bigint) and  gh."date_key" >= cast (from_date_key AS date)
    LEFT JOIN AR ON ar.driver_gk = cast(gh.driver_gk as bigint) and ar.date_key = gh.date_key
    left join emilia_gettdwh.dwh_fact_orders_v fo ON cast(dp.driver_gk AS bigint) = fo.driver_gk  and fo.date_key = gh.date_key
            and fo.country_key = 2
            and fo.class_type_key not IN (2000642, 2000886, 2000957, 20001129, 20001260, 20001286) --exclude routes
            and fo.lob_key IN (5,6)
            and fo.driver_total_cost > 0
    LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" dd ON cast(dp.driver_gk as bigint)= dd.driver_gk
    LEFT JOIN "emilia_gettdwh"."dwh_dim_vendors_v" v ON dd."fleet_gk" = v."vendor_gk"


    WHERE   1 = 1

GROUP BY 1,2,3,4,5,6,7
--)




SELECT dp.driver_gk,
vendor_name,
 (CASE when v.vendor_name like '%courier car%' THEN 'PHV'
 when v.vendor_name like '%courier pedestrian%' THEN 'pedestrians'
 when v.vendor_name like '%courier scooter%' THEN 'scooter'
  when v.vendor_name like '%courier trike%' THEN 'e-bike'
 ELSE 'taxi' end) AS supply_type,
dd."fleet_gk",
min(dp.from_date_key) AS from_date_key,
date_diff('day', cast(min(dp.from_date_key) AS timestamp), now()) AS days_in_promo

FROM "sheets"."default".driver_promo_3500 dp
LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" dd ON dd."driver_gk" = cast(dp.driver_gk AS bigint)
LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v v ON v.vendor_gk = dd.fleet_gk

GROUP BY 1,2,3,4

select *
from emilia_gettdwh.dwh_fact_drivers_hourly_v
where driver_gk = 2000742863
and date_key = date'2020-10-18'