/*
Триггеры:
Корпоративный заказ стоимостью Б 1 000
Если стоимость поездки для клиента > 1000 Подозрительная поездка - Проверить
Если стоимость поездки для водителя > 1000, проверяем  Подозрительная поездка - Проверить
Не включаем в отчет класс 1663 - это маршрутная доставка.
Б 15 минут пл. ож для клиента
Если (Фактическая продолжительность поездки < 1 минуты И Расстояние < 1 киллометра) - ТО Подозрительная поездка - Проверить
Если Время ожидания > 15 минут ТО Подозрительная поездка - Проверить
60 сек
Если (Фактическая продолжительность поездки < 1 минуты) Подозрительная поездка - Проверить ТО Предположительно поездка не состоялась
Not delivery
Если в delivery app  - not delivered > 0 - Подозрительная поездка - Проверить
Доп услуга:  Возврат посылки
Если Return parcel delivery Подозрительная поездка - Проверить
Для оптимизации процесса нам нужно создать дашборд.
1. В отчете должны быть колонки:
order id +
fleet id +
driver id +
est distance +
est duration +
customer inc vat
driver inc vat +
destination (address) +
from (address) +
customer inc vat +
driver inc vat +
company id +
corporate account +
name class category (delivery or transportation) +
order datetime +
start datetime +
class name +
order status +
fleet name +
duration (arrival to drop off) (?) - ATA+in ride or in ride only?
distance drop off (address) (?) - est distance?
stop points total +
waiting time paid - nowhere
waiting time (cost) +- only nf
return parcel key +- only nf

2. По каждому триггеру должен быть отдельный лист в отчете.

3. Отчет должен приходить 1 раз в день до 14:00 за предыдущий день
*/

select *--suspecious_reason_full, correction_check_type, platform,  count(order_gk)
    from
(
/*
Owner - Kozlova Kseniia
Cube Name - Fraud orders corrections
ID - 46126A9511EBD7F7266D0080EFC5586B
*/
with main_table as (
        with all_orders AS (
                SELECT fo.date_key   AS    dates, date_diff('day', current_date,fo.date_key) days_ago,
                       fo.fleet_gk,
                       v.vendor_name fleet_name,
                       fo.driver_gk,
                       cast(jo.est_duration_min as integer) est_duration,
                       fo.est_duration est_duration_fo,
                       cast(jo.est_distamce_km as integer) est_distance,
                       fo.est_distance est_distance_fo,
                       customer_cotract_id,
                        jo.est_distamce_km, jo.distance_cust_cost, jo.unit_cost, jo.free_km,
                        jo.est_duration_min, jo.duration_cust_cost, jo.min_cost, jo.free_mins,
                       fo.dest_full_address,
                       fo.origin_full_address,

                       dest_longitude, dest_latitude,origin_longitude, origin_latitude,

                       fo.ordering_corporate_account_gk AS    company_gk,
                       ca.corporate_account_name  AS    company_name,
                       am.name account_manager,
                       ct.class_type_desc  AS    class_type,
                       ct.class_type_key,
                       fo.gt_order_gk   AS    order_gk,
                       jo.journey_id,
                       fo.cust_care_comment,
                       fo.order_datetime,
                       jo.started_at started_datetime,
                       jo.returned_deliveries >0    return_tag,
                       jo.returned_deliveries   returned_deliveries_nf,
                       date_diff('second', jo.started_at, jo.arrived_at)* 1.00 / 60.00 ATA_min,
                       date_diff('second', jo.arrived_at, jo.picked_up_at) * 1.0 / 60.00 waiting_time,
                       jo.waiting_cost_customer,




                       (case when dropoff_latitude = -1 then null
                       else round(ST_Distance(to_spherical_geography(ST_Point(dest_longitude, dest_latitude)),
                           to_spherical_geography(ST_Point(dropoff_longitude, dropoff_latitude)))/1000,3) end)
                           AS distance_between_dest_and_dropoff,

                       (case when dropoff_latitude = -1 then null
                           else round(ST_Distance(to_spherical_geography(ST_Point(origin_longitude, origin_latitude)),
                               to_spherical_geography(ST_Point(dropoff_longitude, dropoff_latitude)))/1000,3) end)
                           AS distance_between_pickup_and_dropoff,

                       (case when dropoff_latitude = -1 then null
                           else round(ST_Distance(to_spherical_geography(ST_Point(origin_longitude, origin_latitude)),
                               to_spherical_geography(ST_Point(dest_longitude, dest_latitude)))/1000,3) end)
                           AS pickup_dest_dist,


                       date_diff('second', jo.started_at, jo.ended_at) * 1.0 / 60.00   ride_duration_min,
                    ct.class_type_desc like '%ondemand%' ondemand_tag,
                       fo.ordering_corporate_account_gk in (200025342, 505672, 200025786, 200025821,200025819) logistic_company, -- wb, ru post, kse, dpd,

                       -- driver_gk = 200013 i a CC user, all his orders should be marked AS cancelled
                       CASE
                           when fo.driver_gk = 200013 THEN 'cancelled'
                           else st.order_status_desc end   order_status_desc,
                       order_cancellation_stage_key,

                       (CASE
                            when v.vendor_name like '%courier car%' THEN 'PHV'
                            when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
                            when v.vendor_name like '%courier scooter%' THEN 'scooter'
                            ELSE 'taxi'
                           end)  AS    supply_type,

                       (case when ct.class_family = 'Premium' then 'NF' else 'OF'end) platform,

                     'eCommerce'  Client_type_desc,

                       (case
                            when jo.order_gk is not null then total_customer_amount_exc_vat
                            else fo.customer_total_cost end)  AS    customer_total_cost_ex_vat,


                       fo.driver_total_cost  AS    driver_total_cost,
                       fo.driver_total_commission_exc_vat  AS    driver_total_commission_exc_vat,
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
                       sum(CASE
                               when ct.class_family IN ('Premium') and ct.class_type_desc not like '%ondemand% '
                                   then jo.completed_deliveries
                               when ct.class_family IN ('Premium') and ct.class_type_desc like '%ondemand%'
                                   THEN jo.picked_up_deliveries
                               ELSE 0 end)  AS    paid_deliveries_NF,
                       sum(CASE
                               when ct.class_family IN ('Premium') THEN jo.completed_deliveries
                               ELSE 0 end)   AS    completed_deliveries_NF,
                       sum(CASE
                               when ct.class_family IN ('Premium') THEN jo.not_delivered
                               ELSE 0 end)   AS    not_delivered_NF,
                       sum(jo.gross_deliveries)   AS    gross_deliveries_NF

                FROM "emilia_gettdwh"."dwh_fact_orders_v" fo
                         LEFT JOIN (
                    SELECT j.order_gk,
                           j.journey_id,
                           j.ended_at,
                           dist.contract_id customer_cotract_id,
                           j.started_at,
                           wt.waiting_cost  waiting_cost_customer,

                           j.total_customer_amount_exc_vat,
                           j.created_at,
                           dist.est_distamce_km, dist.cost distance_cust_cost, vd.unit_cost, vd.free_km,
                           dur.est_duration_min, dur.cost duration_cust_cost, vdu.min_cost, vdu.free_mins,
                           min(d.arrived_at)  arrived_at,
                           min(d.picked_up_at)   picked_up_at,
                           COUNT(CASE
                                     WHEN d.delivery_status_id = 4 AND j.courier_gk <> 200013
                                         THEN d.delivery_gk END)    AS completed_deliveries, -- j.courier_gk = 200013 is a CC user that is used to cancel orders, all his orders should be marked AS cancelled
                           COUNT(CASE
                                     WHEN d.delivery_status_id IN (4, 7) AND j.courier_gk <> 200013
                                         THEN d.delivery_gk END)                                                             AS picked_up_deliveries, -- j.courier_gk = 200013 is a CC user that is used to cancel orders, all his orders should be marked AS cancelled
                           count(case
                                     when d.delivery_status_id = 7 AND j.courier_gk <> 200013
                                         then d.delivery_gk END)                                                                not_delivered,
                           COUNT(CASE
                                     WHEN d.delivery_type_id = 2 AND j.courier_gk <> 200013
                                         THEN d.delivery_gk END)                                                             AS returned_deliveries,
                           COUNT(CASE WHEN d.delivery_type_id <> 2 then d.delivery_gk end)                                   AS gross_deliveries

                    FROM model_delivery.dwh_fact_journeys_v AS j

                    left join (select journey_id, component_amount waiting_cost, contract_id
                                        from model_delivery.dwh_fact_company_monetisation_v
                                        where component_name = 'waiting'
                                          and component_amount > 0
                    ) wt on j.journey_id = wt.journey_id

                    left join
                        (select journey_id, component_amount cost, contract_id, try(cast(component_value as bigint))/1000.0 est_distamce_km

                         from model_delivery.dwh_fact_company_monetisation_v
                         where component_name = 'distance'
                         and component_amount > 0

                        ) dist on j.journey_id = dist.journey_id

                    left join
                        (select journey_id, component_amount cost,
                                contract_id,component_value est_duration_min

                         from model_delivery.dwh_fact_company_monetisation_v
                         where component_name = 'duration'
                         and component_amount > 0

                        ) dur on j.journey_id = dur.journey_id

                    left join (
                                   select contract_id,company_gk,
                                          3 free_km,
                                          approx_percentile(price_per_unit, 0.5) unit_cost
                                    from (
                                    select contract_id,journey_id,
                                           component_name,company_gk,
                                           round(component_amount/((cast(component_value as bigint)-3000)/1000.0/0.1),1) price_per_unit

                                    from  model_delivery.dwh_fact_company_monetisation_v
                                    where company_gk = 200023861
                                    and component_name in ('distance')
                                    -- payment starts from 3 km
                                    and (cast(component_value as bigint)/1000)*1.0>3
                                    and date(created_at) = current_date - interval '1' day
                                    and contract_id not in (212,267,268,271,314)
                                    and env ='RU'
                                    )
                                    group by 1,2,3

                        ) vd on dist.contract_id = vd.contract_id

                    left join (
                                select contract_id,component_name,free_mins,
                            approx_percentile(price_per_unit, 0.5) min_cost

                                    from (
                                    select contract_id,journey_id,component_value,component_amount,
                                           component_name, cast(component_value as bigint) units,
                                            (case when contract_id in (35,77,273,337,338) then 10
                                                                        when contract_id in (257,313,315) then 7
                                                                        when contract_id in (213,256) then 6 end) free_mins,
                                           round(component_amount/(cast(component_value as bigint)-
                                                                    (case when contract_id in (35,77,273,337,338) then 10
                                                                        when contract_id in (257,313,315) then 7
                                                                        when contract_id in (213,256) then 6 end)
                                                                    ),1) price_per_unit

                                    from  model_delivery.dwh_fact_company_monetisation_v
                                    where company_gk = 200023861
                                    and component_name in ('duration')
                                    -- payment starts from 3 km
                                    and component_amount>0
                                    --and (cast(component_value as bigint)/1000)*1.0>3
                                    and date(created_at) = current_date - interval '1' day
                                    --and contract_id not in (212,267,268,271,314)
                                    --and journey_id = 4781872
                                    and env ='RU'
                                    )
                                    group by 1,2,3
                                )    vdu on dist.contract_id = vdu.contract_id


                    LEFT JOIN model_delivery.dwh_fact_deliveries_v AS d ON d.journey_gk = j.journey_gk
                    WHERE 1 = 1
                      AND j.country_symbol = 'RU'

                      AND d.company_gk NOT IN (20001999) -- Test company
                      AND j.date_key BETWEEN CURRENT_DATE - interval '45' day and current_date

                    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
                ) AS jo ON jo.order_gk = fo.order_gk

                         LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
                                   ON ct.class_type_key = fo.class_type_key
                         LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
                                   ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
                         LEFT JOIN emilia_gettdwh.dwh_dim_locations_v AS loc
                                   ON loc.location_key = fo.origin_location_key
                         LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON d.driver_gk = fo.driver_gk
                         LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v AS v ON v.vendor_gk = d.fleet_gk
                         left join emilia_gettdwh.dwh_dim_order_statuses_v st on st.order_status_key = fo.order_status_key
                         LEFT JOIN emilia_gettdwh.dwh_dim_account_managers_v am
                                   ON am."account_manager_gk" = ca."account_manager_gk"


                WHERE fo.country_key = 2
                  AND ct.lob_key IN (5, 6)
                  AND ct.class_group NOT LIKE 'Test'
                  --and ct.class_family IN ('Premium')
                  and ct.class_type_desc not like '%c2c%'
                  and ordering_corporate_account_gk not IN
                      (20004730, 200017459, 20001999, 20001663) --dummy delivery user, test company, logistic company
                  AND fo.date_key BETWEEN CURRENT_DATE - interval '45' day and current_date
        --and fo.order_gk = 20001708743813
                GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
                         29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54
            )
        (select *,

               (case
                   when customer_total_cost_ex_vat>0 and buy_sell <= -500 then 'Cost difference 500+'
                   when (platform = 'NF' and logistic_company = False and ((customer_total_cost_ex_vat > 800) or (driver_total_cost > 800)))
                          and (buy_sell < 0
                                           or (buy_sell>=0 and lower(origin_full_address) like '%балашиха%') -- gps wrong work zone
                                           or (buy_sell >=0 and coalesce(est_distance, est_distance_fo) > 35)) -- wrong geocoding cases
                                           then 'Price over 800'

                    when platform = 'NF' and (ride_duration_min <= 2)  then 'Short ride'
                    when platform = 'NF' and waiting_cost_customer >= 300 then 'long waiting time'
                    when platform = 'NF' and not_delivered_NF >0 and logistic_company = False then 'not delivered'

                   when (gross_deliveries_NF < 2 or platform = 'OF') -- not aggregated
                            and order_status_desc = 'Completed' -- exclude cases when order was cancelled or corrections were charged
                            and ride_duration_min between 4 and 17 -- based on analysis
                            and (distance_between_dest_and_dropoff between 0.31 and 0.6) -- thr is got from analysis
                       then 'drop off 300m+ from destination'
                        when platform = 'NF' and order_status_desc = 'Completed' and
                             coalesce(customer_total_cost_ex_vat,0) = 0 and driver_total_cost > 0
                            then 'customer zero'
                        when company_gk = 200023861 and order_status_desc = 'Completed' and platform = 'NF' -- VV

                                and ((gross_deliveries_NF=1 and est_distance >3 and est_distance / pickup_dest_dist >=5)
                                         or (gross_deliveries_NF=1 and est_distance / pickup_dest_dist >= 3 and est_distance >= 8) --wrong est
                                         or buy_sell < -300) then 'VV dist'
                        when platform = 'NF' and
                            ((order_status_desc = 'Completed' and
                             coalesce(customer_total_cost_ex_vat,0) > 0 and driver_total_cost = 0)
                             )
                            then 'driver zero'
                        when platform = 'NF' and order_status_desc = 'Cancelled' and
                             coalesce(customer_total_cost_ex_vat,0) > 0 and driver_gk = -1
                             then 'cancellation bug'
                        when platform = 'NF' and (coalesce(customer_total_cost_ex_vat,0) < 0
                        or driver_total_cost < 0) then 'negative cost'

                   end)  suspecious_reason,

               (customer_total_cost_ex_vat > 800) or (driver_total_cost > 800) price_over800_tag,
               ride_duration_min <= 1 short_ride_tag


        from all_orders)


    union

    (select
    date("order datetime") dates, date_diff('day', current_date, date("order datetime")) days_ago,
    cast("fleet gk" as bigint) fleet_gk,
      "fleet name" fleet_name,
      cast(concat('2000', cast("driver id" as varchar)) as bigint) driver_gk,
    cast("est duration" as bigint) est_duration, null est_duration_fo,
             cast("est distance" as bigint) est_distance, null est_distance_fo,
    null customer_cotract_id,
    null est_distamce_km, null distance_cust_cost, null unit_cost, null free_km,
    null est_duration_min, null duration_cust_cost, null min_cost, null free_mins,
    "destination (address)" dest_full_address,
    "from (address)" origin_full_address,

     null  dest_longitude, null dest_latitude, null origin_longitude, null origin_latitude,
      cast(concat('2000', cast("company id" as varchar)) as bigint) company_gk,
      corporate_account_name company_name, null account_manager,
    "class name" class_type,
    null class_type_key,
    cast("gt order id"  as bigint) order_gk,
    null journey_id,
    "customer care comment",
      "order datetime",
    "start datetime" start_datetime,
    "return parcel key" > 0 return_tag,
     null return_deliveries_nf,
    null ATA_min,
    "total waiting time" waiting_time,
     "paid waiting time (cost)" waiting_cost_customer,
     null distance_between_dest_and_dropoff,
      null distance_between_pickup_and_dropoff,
null pickup_dest_dist,
      "duration (arrival to drop off)" ride_duration_min,
    null ondemand_tag,
    null logistic_company,
    "order status" order_status,
    null cancellation_stage,
    (CASE
      when "fleet name" like '%courier car%' THEN 'PHV'
      when "fleet name" like '%courier pedestrian%' THEN 'pedestrian'
      when "fleet name" like '%courier scooter%' THEN 'scooter'
      ELSE 'taxi' end)  AS    supply_type,
    'OF' platform,
    'Corporate' Client_type_desc,
    "customer inc vat" cistomer_total_cost_inc_vat,
    "driver inc vat" driver_total_cost,
     null driver_commission,
    (CASE
                          WHEN "customer inc vat" - "driver inc vat" >0
                          THEN round(("customer inc vat" - "driver inc vat")/1.2,2)
                          ELSE "customer inc vat" - "driver inc vat"
                      END) buy_sell,
      null paid_deliveries_NF,
      null completed_deliveries_NF,
      null not_delivered_NF,
      null gross_delivered_NF,
    reason suspecious_reason,
      "customer inc vat" >1000 or "driver inc vat" >1000 high_price,
      "duration (arrival to drop off)" <= 1 or ("duration (arrival to drop off)" <= 1 and "est distance" <= 1) short_journey

    from hive.analyst.driver_correction_report r
    LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
                               ON ca.corporate_account_gk = cast(concat('2000', cast(r."company id" as varchar)) as bigint)

    where date("alert created") between current_date - interval '45' day and current_date
    and "class category (delivery or transportation)" = 'delivery'
    and "company id" <> '4730'
    )

        )

        (
    select "main_table".*,
    bkk.comment bkk_correction_comment,
    bkk.comment like '%время ожидания%Обратился:Водитель%' driver_WT_charge_request_tag,
    bkk.comment like '%доплату (вес)%' waight_payment_tag,

    -- fullfill tags - REFRESH!
    (case when bkk.comment like '%время ожидания%Обратился:Водитель%' then 'Driver WT payment request'
    when suspecious_reason = 'dlvry cost' then 'Price over 1000'
     when suspecious_reason = 'dlvr waiting time' then 'long waiting time'
     when suspecious_reason = 'dlvr short ride' then 'Short ride'
    else suspecious_reason end) suspecious_reason_full,

    (case
    when coalesce(suspecious_reason,'0') not in ('0','customer zero', 'VV dist','driver zero','cancellation bug','negative cost')
            or bkk.comment like '%время ожидания%Обратился:Водитель%' then 'Fraud'
    when suspecious_reason in ('customer zero', 'VV dist','driver zero','cancellation bug','negative cost')
            or bkk.comment like '%доплату (вес)%' then 'correction check' end) correction_type,

    (case when suspecious_reason in ('customer zero', 'VV dist','driver zero','cancellation bug','negative cost') then suspecious_reason
    when bkk.comment like '%доплату (вес)%' then 'weight payment' end) correction_check_type,

    case when suspecious_reason = 'VV dist' then 'Изменение стоимости за дистанцию. Новая дистанция ___' end correction_comment_vv

    from "main_table"
        left join (
                    select * from
                    (
                        select
                        created_at change_time,
                        date (created_at) as dates,
                        cast (concat('2000', cast (order_id as varchar)) as bigint) order_gk,
                        bkk_order_id,
                        supplier_calculation_amount_exc_tax,
                        customer_calculation_amount_exc_tax,
                        user_email,
                        comment,
                        max(created_at) over (partition by bkk_order_id) last_change

                        from hive.bookkeeping.operations_features_v
                        where env = 'RU'
                        and operation_type = 'Operation::Modify'
                        and created_date >= date_trunc('day'
                            , current_date - interval '45' day )
                        and (env = 'RU'
                        or (env = 'GL'
                        and country = 'RU'))
                        and class_name like '%delivery%'
                    )
                    where (change_time <> last_change and (
                    comment like '%доплату (вес)%' or
                    comment like '%время ожидания%Обратился:Водитель%'))  or
                     change_time = last_change

                ) bkk on "main_table".order_gk = bkk.order_gk

    where (suspecious_reason is not null
            or bkk.comment like '%время ожидания%Обратился:Водитель%'
            or bkk.comment like '%доплату (вес)%')
        )
)
where days_ago >= -30
and suspecious_reason in ('driver zero',
'cancellation bug',
'negative cost')
--group by 1,2,3


;

select * from model_delivery.dwh_fact_company_monetisation_v
where env = 'RU'
--and journey_id = 1548335
and component_name = 'waiting' and component_amount >0

order by "journey_id"
limit 20


select journey_id, component_amount waiting_cost, contract_id
from desc model_delivery.dwh_fact_company_monetisation_v
where component_name = 'waiting' and component_amount > 0



select distinct distance_between_dest_and_dropoff = distance_between_pickup_and_dropoff same_dropof,
       was_return,
                bkk_correction is not null is_bkk_correction,correction_type,
       cancelled, *
       --count(distinct order_gk) cases, avg(distance_between_dest_and_dropoff)
from (
-- short ride
         select distinct fo.order_gk, bk.order_gk bkk_correction, correction_type,
                         jo.returned_deliveries >0 was_return, comment,
                         (driver_total_cost = 0 and customer_total_cost = 0) cancelled,
                         date_diff('second',jo.started_at, jo.ended_at)*1.0/60.0 ride_duration_min,
                         (case
                              when dropoff_latitude = -1 then null
                              else round(ST_Distance(to_spherical_geography(ST_Point(dest_longitude, dest_latitude)),
                                                     to_spherical_geography(
                                                             ST_Point(dropoff_longitude, dropoff_latitude))) / 1000,
                                         3) end)
                             AS                                              distance_between_dest_and_dropoff,

                         (case
                              when dropoff_latitude = -1 then null
                              else round(ST_Distance(
                                                 to_spherical_geography(ST_Point(origin_longitude, origin_latitude)),
                                                 to_spherical_geography(
                                                         ST_Point(dropoff_longitude, dropoff_latitude))) / 1000, 3) end)
                             AS                                              distance_between_pickup_and_dropoff,

                         (case
                              when dropoff_latitude = -1 then null
                              else round(ST_Distance(
                                                 to_spherical_geography(ST_Point(origin_longitude, origin_latitude)),
                                                 to_spherical_geography(ST_Point(dest_longitude, dest_latitude))) /
                                         1000, 3) end)
                             AS                                              pickup_dest_dist

         from emilia_gettdwh.dwh_fact_orders_v fo

             left join emilia_gettdwh.dwh_dim_class_types_v cl on fo.class_type_key = fo.class_type_key
            left join (
                select * from
            (select
                        distinct created_at,
                         cast (concat('2000', cast (order_id as varchar)) as bigint) order_gk,
                        comment,
                        (case when comment like '%Поездка не состоялась%Принято решение:Скорректировать%'
                            or comment like '%Поездка не состоялась%Принято решение:Отменить%'
                            then 'отмена'
                        when comment like '%Принято решение:Поездка корректна%' then 'прошел проверку'
                        else 'other' end) correction_type,
                        max(created_at) over (partition  by order_id) last_

                        from hive.bookkeeping.operations_features_v
                        where env = 'RU'
                        and operation_type = 'Operation::Modify'
                        and created_date >= date_trunc('day'
                            , current_date - interval '90' day )
                        and (env = 'RU'
                        or (env = 'GL'
                        and country = 'RU'))
                        and class_name like '%delivery%'
                        ) where (correction_type in ('отмена','прошел проверку') and created_at <> last_)
                             or created_at = last_
             ) as bk on bk.order_gk = fo.order_gk

            LEFT JOIN (
                    SELECT j.order_gk,
                           j.journey_id,
                           j.started_at, j.created_at, j.ended_at,

                           min(d.arrived_at)  arrived_at,
                           min(d.picked_up_at)   picked_up_at,
                           COUNT(CASE
                                     WHEN d.delivery_status_id = 4 AND j.courier_gk <> 200013
                                         THEN d.delivery_gk END)    AS completed_deliveries, -- j.courier_gk = 200013 is a CC user that is used to cancel orders, all his orders should be marked AS cancelled
                           COUNT(CASE
                                     WHEN d.delivery_status_id IN (4, 7) AND j.courier_gk <> 200013
                                         THEN d.delivery_gk END)                                                             AS picked_up_deliveries, -- j.courier_gk = 200013 is a CC user that is used to cancel orders, all his orders should be marked AS cancelled
                           count(case
                                     when d.delivery_status_id = 7 AND j.courier_gk <> 200013
                                         then d.delivery_gk END)                                                                not_delivered,
                           COUNT(CASE
                                     WHEN d.delivery_type_id = 2 AND j.courier_gk <> 200013
                                         THEN d.delivery_gk END)                                                             AS returned_deliveries,
                           COUNT(CASE WHEN d.delivery_type_id <> 2 then d.delivery_gk end)                                   AS gross_deliveries

                    from model_delivery.dwh_fact_journeys_v j
                    LEFT JOIN model_delivery.dwh_fact_deliveries_v AS d ON d.journey_gk = j.journey_gk
                    WHERE 1 = 1
                      AND j.country_symbol = 'RU'
                      AND d.company_gk NOT IN (20001999) -- Test company
                      AND j.date_key BETWEEN CURRENT_DATE - interval '90' day and current_date

                    GROUP BY 1, 2,3,4,5
                ) AS jo ON jo.order_gk = fo.order_gk



         where cl.country_key = 2
           and fo.lob_key = 5
           and fo.date_key >= current_date - interval '90' day
           and cl.class_family = 'Premium'
           --and driver_total_cost = 0
           --and customer_total_cost = 0
           and fo.country_key = 2
           and customer_total_cost is not null
           and dropoff_latitude <> -1
    and jo.gross_deliveries = 1
     )
where was_return = False and distance_between_dest_and_dropoff <> distance_between_pickup_and_dropoff
        and
      correction_type is not null

--group by 1,2,3,4,5

select
                        created_at change_time,
                        date (created_at) as dates,
                        cast (concat('2000', cast (order_id as varchar)) as bigint) order_gk,
                        bkk_order_id,
                        supplier_calculation_amount_exc_tax,
                        customer_calculation_amount_exc_tax,
                        user_email,
                        comment, max(created_at) over (partition by bkk_order_id) last_change

                        from hive.bookkeeping.operations_features_v
                        where env = 'RU'
                        and operation_type = 'Operation::Modify'
                        and created_date >= date_trunc('day'
                            , current_date - interval '45' day )
                        and (env = 'RU'
                        or (env = 'GL'
                        and country = 'RU'))
                        and class_name like '%delivery%'


-- select driver_gk,
--        count(distinct order_gk) cases,
-- avg(distance_between_dest_and_dropoff) avg_dev,
--        min(distance_between_dest_and_dropoff) min_dev,
--        max(distance_between_dest_and_dropoff) max_dev
--
-- from (
         select
                distinct fo.order_gk,
                j.journey_id, driver_gk,
                ordering_corporate_account_gk, gf.journey_id is not null geofencing_worked,
                (case
                     when dropoff_latitude = -1 then null
                     else round(ST_Distance(to_spherical_geography(ST_Point(dest_longitude, dest_latitude)),
                                            to_spherical_geography(ST_Point(dropoff_longitude, dropoff_latitude))) /
                                1000, 3) end)
                    AS distance_between_dest_and_dropoff

         from emilia_gettdwh.dwh_fact_orders_v fo
                  left join model_delivery.dwh_fact_journeys_v j on j.order_gk = fo.order_gk and j.country_symbol = 'RU'
             and j.date_key >= date '2021-06-01'
                  left join "delivery"."public".journey_history jh
                            on j.journey_id = jh.journey_id and "user" <> 'system@gett.com' and "user" is not null
                                and "action" in ('journey status updated', 'delivery status updated')
                                and description in ('From confirmed To completed')
                left join (
                    select distinct
                           cast((case when json_extract_scalar(from_utf8("payload"), '$.journey_id') is null then '0' end) as bigint) journey_id
                    from events
                    where event_name in ('courier|deliver_screen|away_from_drop_off|popup_appears')
                    and env = 'RU' and event_date >= date '2021-06-01'

             ) gf on gf.journey_id = j.journey_id

         where lob_key in (5, 6)
           and country_key = 2
           and j.journey_id is not null
           -- not CC
           and jh.journey_id is null
           and (case
                    when dropoff_latitude = -1 then null
                    else round(ST_Distance(to_spherical_geography(ST_Point(dest_longitude, dest_latitude)),
                                           to_spherical_geography(ST_Point(dropoff_longitude, dropoff_latitude))) /
                               1000, 3) end) > 0.5

           and fo.date_key >= date '2021-06-01'
           -- not IML from FH
            and driver_gk not in (2000984900,	2000914397,	2000715875,	2000543405,	2000841825,	2000876928,	2000662026,	2000867880,	2000879922,	2000579734,	2000560554,	2000613401,	2000415173,	2000547659,	2000547429,	2000547049,	2000550695,	2000420945,	2000499836,	2000661299,	2000616642,	2000587243,	2000998977,	2000890060,	20001063491,	2000491972,	2000620521,	2000976032,	2000525991,	2000526125,	2000469732,	2000917078,	2000819729,	2000537256,	2000866433,	2000525991,	2000889874,	2000907217,	2000422562,	2000550916,	2000614671,	2000542852,	2000628600,	2000617789,	2000479279,	2000424505,	2000649637,	2000649421,	2000613965,	2000618118,	20001053543,	20001278036,	2000543908,	20001032191,	2000801997,	2000699645,	20001032205,	2000879268,	20001049175,	2000265157,	2000651421,	2000496061,	2000864773,	2000929608,	20001045869,	20001025385,	2000547430,	2000614395,	2000652198,	2000778685,	20001018601,	20001054676,	2000994877,	2000794929,	2000985915,	2000991628,	20001042959,	2000593957,	20001059146,	2000639050,	2000550923,	2000730915,	2000945394,	2000426501,	20001006345,	20001074077,	2000614324,	20001081245,	20001020715,	20001025843,	20001031690,	20001059962,	2000542635,	2000580430,	20001065024,	2000518682,	2000547046,	2000537047,	2000613403,	2000551837,	2000614980,	2000526243,	2000651831,	2000527686,	2000615507,	2000443373,	2000451874,	2000937896,	2000634986,	2000557462,	2000389979,	2000541510,	2000617605,	2000484646,	2000555089,	200032325,	2000522558,	2000500065,	2000562167,	2000542545,	20001136150,	20001025329,	20001093743,	2000723630,	20001053328,	20001101797,	2000987391,	20001131367,	2000765827,	2000574073,	2000481007,	2000514361,	2000368955,	2000556209,	2000439922,	2000518978,	2000421377,	2000426341,	2000546294,	2000565924,	2000519230,	2000579738,	2000547794,	2000592693,	2000585935,	2000552909,	2000452473,	2000557136,	2000547048,	2000618811,	2000552226,	2000421378,	2000422747,	2000542852,	2000536398,	2000412858,	2000562167,	200032325,	20001100482,	20001053328,	2000617605,	2000372207,	2000109356,	2000843202,	2000522558,	2000340849,	2000550677,	2000533747,	20001044556,	2000993358,	20001030107,	20001057469,	20001036410,	2000415845,	20001012802,	2000956571,	20001063593,	20001078104,	2000995562,	20001040701,	2000617803,	2000518823,	2000547050,	2000442937,	2000540725,	2000550696,	2000466962,	2000547046,	2000633077,	2000550673,	2000474406,	2000550677,	2000902809,	2000454039,	2000580430,	2000613899,	2000527677,	2000460383,	2000550458,	2000425776,	2000908606,	2000902809,	2000221285,	20001137039,	20001136150,	2000390697,	2000614974,	2000551831,	2000616768,	2000885415,	2000542545,	20001065024,	200083185,	20001349426,	2000841855,	20001289429)
             -- not Gett delivery members
            and driver_gk not in (2000695617,2000811159,2000788061,200011,2000723672)

--      )
-- group by 1


select *
from "delivery"."public".journey_history
where journey_id = 3981394;

select * desc  "emilia_gettdwh"."dwh_dim_areas_v"

select 4765280 journey_id, 12 in_pricing, ((4104 - 3000)/1000.0/0.1) formuala1