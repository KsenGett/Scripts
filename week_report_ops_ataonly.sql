with ata as (
(select fo.date_key,
    tp.timecategory,
    tp.subperiod,
    tp.period,
    tp.subperiod2 AS time_period,
    'OF' as platform,
    (CASE when fo.lob_key = 6 THEN 'C2C'
   when am.name like '%Delivery%' or ca.account_manager_gk IN( 100079, 100096, 100090, 100073, 100088)
   THEN 'eCommerce' ELSE 'Corporate' end ) AS client_type,
    fo.ordering_corporate_account_gk,
    ca.corporate_account_name,
    accounts.name_internal,

    order_gk,
    (CASE when order_status_key = 7 THEN 'Completed' ELSE 'Cancelled ON Arrival' end) AS order_status,

    CASE when date_diff('second', fo.order_datetime,fo.driver_arrived_datetime)*1.00/60 > 0 THEN
    date_diff('second', fo.order_datetime, fo.driver_arrived_datetime)*1.00/60 end AS ata,

    count (distinct CASE when (fo.order_status_key = 7 or (fo.order_status_key = 4 and driver_total_cost > 0))
    THEN fo.order_gk ELSE null end) AS completed_and_cancelled_orders,
    count (distinct CASE when fo.order_status_key = 7 THEN fo.series_original_order_gk ELSE null end) AS completed_orders


    from emilia_gettdwh.dwh_fact_orders_v fo
        LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.hour_key = fo.hour_key and tp.date_key = fo.date_key
           and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
        LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
                  ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key
        LEFT JOIN "emilia_gettdwh"."dwh_dim_account_managers_v" am ON am."account_manager_gk" = ca."account_manager_gk"
        LEFT JOIN sheets."default".delivery_corp_accounts_20191203 AS accounts
            ON cast(accounts.company_gk AS bigint)=fo.ordering_corporate_account_gk

    where tp.timecategory is not null
        and fo.date_key >= date'2020-08-01'
        and fo.country_key = 2
        and fo.lob_key in (5,6)
        and ct.class_family not IN ('Premium')
        and ct.class_group not like 'Test'
        and fo.ordering_corporate_account_gk not in (20004730,200017459)
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13)

union

    (select
    date(j.scheduled_at) AS date_key,
    tp.timecategory,
    tp.subperiod,
    tp.period,
    tp.subperiod2 AS time_period,
    'NF' as platform,
    'eCommerce' client_type,
    cast(d.company_id as integer) ordering_corporate_account_gk,
    ca.corporate_account_name,
    'nan' name_internal,

    j.legacy_order_id order_gk, j.status order_status,

    (CASE when date_diff('second', j.scheduled_at , d.arrived_at)*1.00/60 > 0 THEN
    date_diff('second', j.scheduled_at , d.arrived_at) end)*1.00/60 AS ata,
    count (distinct CASE when (fo.order_status_key = 7 or (fo.order_status_key = 4 and driver_total_cost > 0))
    THEN fo.order_gk ELSE null end) AS completed_and_cancelled_orders,
    count (distinct CASE when fo.order_status_key = 7 THEN fo.series_original_order_gk ELSE null end) AS completed_orders

    FROM delivery.public.deliveries AS d
        LEFT JOIN delivery.public.journeys AS j ON j.id = d.journey_id
        LEFT JOIN delivery.public.cancellation_infos AS c ON d.id = c.cancellable_id and c.cancellable_type = 'deliveries'
        LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca ON cast(ca.source_id AS varchar(128)) = d.company_id and d.env = ca.country_symbol
        LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.date_key = date(j.scheduled_at) and tp.hour_key = 0
        and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
        --fo
        LEFT JOIN emilia_gettdwh.dwh_fact_orders_v AS fo ON j.legacy_order_id = fo.sourceid
        and fo.country_key = 2 and lob_key = 5 and year(fo.date_key) > 2019
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v ct ON fo.class_type_key=ct.class_type_key

    WHERE d.env ='RU'
      and lower(ca.corporate_account_name) not like '%test%'
      and date(j.scheduled_at) >= date'2020-08-01'
      and tp.timecategory is not null
      and ct.class_type_desc like '%ondemand%'
      and d.company_id  not in ('17459', '4730')
      and d.status IN ('completed', 'not_delivered', 'cancelled', 'rejected')
      and (date_diff('second', d.created_at, j.scheduled_at))*1.00/60 <= 20

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13)
)
(select date_key, date_key, timecategory, subperiod, "period", time_period, platform, client_type,
    ordering_corporate_account_gk,
    corporate_account_name,
    name_internal, order_status,
    ata, completed_orders, completed_and_cancelled_orders,

count(ata), count(case when ata > 30 then ata else null end) ata_over_30
from ata
--where platform = 'NF'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15);

;

/*(select platform, client_type, subperiod,name_internal,corporate_account_name,
avg(ata) aver_ata,
--sum(completed_and_cancelled_orders) CAA,
count(case when ata is not null then order_gk end) orders,
count(case when ata > 30 then order_gk end) over_30_ata

from ata
where subperiod in ('W36','W37','W38')
group by 1,2,3,4,5
order by subperiod, aver_ata);
 */

select pickup_location_key, scheduled_at, order_gk, journey_gk, delivery_status_id, delivery_gk,  arrived_at
from "model_delivery"."dwh_fact_deliveries_v"
limit 10;