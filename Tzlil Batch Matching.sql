with assigned_by_cc AS (SELECT env, cast(json_extract_scalar(json_parse(from_utf8(cc.payload)), '$.order_id')
                                                  AS bigint) AS order_assigned_by_cc, occurred_date
                        FROM app_events cc
                        WHERE 1 = 1--cc.env IN ('RU',)
                          and cc.occurred_date >= date_add('day', -80, current_date)
                          AND cc.event_name = 'matching|driver_assigned_by_cc')
-- , data_two AS (
--                SELECT
--                cast(json_extract_scalar(json_parse(from_utf8(cc.payload)), '$.order_id')
--                                                   AS bigint) AS source_id,
--                CASE when env = 'UK' THEN 'GB' ELSE env end env,
--                cast(json_extract(json_parse(from_utf8(cc.payload)), '$.data') AS varchar) test_group
-- --        , *
--                         FROM events cc
--                         WHERE cc.event_name = 'matching|dispatched_by'
--                         and event_date >= date_add('day', -15, current_date)
    , data AS (
    SELECT distinct event_at,
                              CASE when e.env = 'UK' THEN 'GB' ELSE e.env end                       env,
                              cast(area AS int)                                                     area_id,
                              ra.zone,
                              ra.routing_region,
                              a.title,
                              a.size,
                              ROW_NUMBER() OVER (PARTITION BY order_id, e.env ORDER BY size asc) AS rn,
                              order_id
              FROM app_events e
                     cross JOIN unnest(cast(json_extract(from_utf8(payload), '$.data.order_data.area_ids') AS
                                            array(JSON))) AS t (area)
                     JOIN area.public.areas a ON a.id = cast(area AS int) and a.category_id = 14
                     LEFT JOIN hive.analyst.areas_20181115 ra
                       ON cast(ra.area_id AS int) = cast(area AS int) --and ra.routing_region not like 'deliv%'
              WHERE --e.env = 'IL' and
                  e.occurred_date >= date_add('day', -80, current_date)
                and e.event_name = 'matching|fetch_strategy'),
 scoring_group AS (
  SELECT distinct /*edeited*/
    json_extract_scalar(from_utf8(payload), '$.batch_id')    sc_batch_id,
    json_extract_scalar(from_utf8(payload), '$.scoring_test_group') sc_test_group,
    CASE when env = 'UK' THEN 'GB' ELSE env end                      sc_env
  FROM events cc
  WHERE
    event_name = 'bulk_matching|batch_scoring'
    and event_date >= date_add('day', -80, current_date)
)
, bulk_offers AS (
  SELECT distinct /*edeited*/
    cast(json_extract_scalar(json_parse(from_utf8(cc.payload)), '$.order_id')
      AS bigint) AS                                            source_id,
    json_extract_scalar(from_utf8(payload), '$.data.batch_id') batch_id,
    CASE when env = 'UK' THEN 'GB' ELSE env end                env
  FROM events cc
  WHERE
    event_name = 'matching|bulk_matching_offers'
    and event_date >= date_add('day', -80, current_date)
)
, sc_gr_orders AS (
    SELECT distinct a.source_id, a.env, b.sc_test_group FROM bulk_offers a
--   LEFT
    inner JOIN scoring_group b ON a.batch_id = b.sc_batch_id
--   LEFT JOIN batch_ids c ON a.batch_id = c.b_batch_id
  )
, data_three AS (
  SELECT t.source_id, env, CASE when count(1) = 1 THEN min(sc_test_group) ELSE 'both_groups' end sc_test_group
  FROM sc_gr_orders t GROUP BY 1,2
),
time_groups AS (
SELECT
 json_extract_scalar(from_utf8(payload), '$.bulk_settings.chosen_settings') AS time_group,
 CASE when e.env = 'UK' THEN 'GB' ELSE e.env end AS env,
 CAST(json_extract( from_utf8(payload), '$.order_ids') AS array<Varchar>) AS orders
FROM app_events e
WHERE event_name='bulk_matching|batch_started'
and occurred_date >= date_add('day', -80, current_date)
and env = 'IL'
),
time_groups_by_order_id AS (
 SELECT
  distinct tg.time_group AS time_group,
  order_id,
  tg.env
 FROM time_groups tg
 cross JOIN unnest(tg.orders) AS time_groups(order_id)
),
 time_groups_per_orderid_normalize AS (
  SELECT cast(t.order_id AS bigint) AS order_id, t.env, CASE when count(1) = 1 THEN min(time_group) ELSE 'both_groups' end time_group
  FROM time_groups_by_order_id t GROUP BY 1,2
)
, rider_pr_0 AS (
  SELECT distinct
         json_extract_scalar(from_utf8(payload), '$.data.matching_info.rider_priority') rider_priority,
         cast(json_extract_scalar(from_utf8(payload), '$.data.order_data.company_id') AS bigint) company_id,
         cast(json_extract_scalar(from_utf8(payload), '$.order_id') AS bigint) AS source_id,
         CASE when env = 'UK' THEN 'GB' ELSE env end                      fetch_str_env
  FROM events e
  WHERE e.event_name = 'matching|fetch_strategy'
    -- and env = 'IL'
    -- and event_date = date'2020-02-10'
    and event_date >= date_add('day', -80, current_date)
)
, rider_pr AS (
  SELECT t.source_id, company_id, fetch_str_env, CASE when count(1) = 1 THEN min(rider_priority) ELSE 'both_groups' end rider_priority
  FROM rider_pr_0 t GROUP BY 1,2,3
)
, lean AS (
SELECT distinct lo.lo_id
,CASE when lo.env = 'uk' THEN 'GB' ELSE upper(lo.env) end AS env
,lo.gt_id
FROM "all-IN"."public".gt_lo_ids lo
)
,rides AS (
SELECT
  distinct /*edeited*/
                order_id
,CASE when env = 'UK' THEN 'GB' ELSE env end env
FROM app_events app
WHERE event_name = 'server|ride|started'
and app.occurred_date >= date_add('day', -80, current_date)
)
,routing AS (SELECT distinct order_id  /*edeited*/
,CASE when env = 'UK' THEN 'GB' ELSE env end env
FROM app_events app
WHERE event_name IN ('matching|sent_to_routing' ,'futureorder|send_to_routing')
and app.occurred_date >= date_add('day', -80, current_date)
  --dubli esli ubrat' distinct
)
, skips AS (
  SELECT
    --        *,
    json_extract_scalar(from_utf8(payload), '$.order_id') order_id,
    CASE when env = 'UK' THEN 'GB' ELSE env end env,
    count(*)                                              skip_count
  FROM events
  WHERE event_name = 'matching|skip_driver_offer_creation'
    and event_date >= date_add('day', -80, current_date)
  GROUP BY 1,2
)
,orders AS(
SELECT          distinct CASE when cast(lo.lo_id AS integer) = o.sourceid and lo.env = o.country_symbol THEN cast(gt_id AS integer)
                    ELSE try_cast(substr(cast(o.gt_order_gk AS varchar),5,length(cast(o.gt_order_gk AS varchar))) AS integer) end AS gett_order_id
                ,o.country_symbol
                ,o.order_gk
                ,o.gt_order_gk
                ,o.sourceid
                ,lo.gt_id
                ,o.date_key
                ,o.is_future_order_key
                ,o.order_status_key
                ,o.series_original_order_gk
                ,o.is_driver_assigned_key
                ,o.order_confirmed_datetime
                ,o.m_routing_duration
                ,o.order_datetime
                ,o.order_create_datetime
                ,o.cancellations_time_in_seconds
                ,o.driver_gk
                ,o.m_order_eta
                ,o.origin_location_key
                ,o.class_type_key
                ,o.country_key
                ,o.hour_key
                ,o.is_gett_fail
FROM emilia_gettdwh.dwh_fact_orders_v o
LEFT JOIN lean lo ON cast(lo.lo_id AS integer) = o.sourceid and lo.env = o.country_symbol
WHERE o.date_key >= date_add('day', -80, current_date)
      and sourceid is not null
--   and o.order_gk = 1000940121929;
)
,fact_orders AS (
SELECT distinct /*edited*/
       o.*
,r.order_id AS ride_id
,ro.order_id AS routing
,coalesce(s.skip_count,0) AS skip_count
,ROW_NUMBER() OVER (PARTITION BY o.series_original_order_gk ORDER BY o.order_create_datetime desc) AS rn_series_order
FROM orders o
LEFT JOIN rides r ON r.order_id = o.gett_order_id and o.country_symbol = r.env
LEFT JOIN skips s ON cast(s.order_id AS integer) = o.gett_order_id and o.country_symbol = s.env
LEFT JOIN routing ro ON ro.order_id = o.gett_order_id and o.country_symbol = ro.env
-- WHERE o.order_gk = 1000940121929
)
,orders_pivot AS (
SELECT
-- fo.order_gk, count(1)
    --   coalesce(test_group, 'Load Index')                                        test_group,
                             CASE
                               when (minute(fo.order_datetime) > 14 and minute(fo.order_datetime) < 30)
                                       THEN 'LoadIndex Test'
                               when (minute(fo.order_datetime) > 44) THEN 'LoadIndex Test'
                               ELSE 'LoadIndex Control' end                                            load_index_grop,

                            case when
                            substring(to_hex(md5(cast(cast(order_gk as varchar) as varbinary))), -1) in ('2','4','6','8','A','C','E')
                            then 'Order gk control' else 'Order gk test' end delivery_test_group,

                             fo.country_symbol,
                             fo.date_key,
                             coalesce(sch.time_period, 'No TimePeriod')                                time_period,
                             coalesce(data.zone, loc.city_name)                                        zone,
                             coalesce(data.routing_region, loc.region_name)                            routing_region,
                             fo.is_future_order_key,
                             cl.class_group,
                                      coalesce(dt.sc_test_group, 'NULL')                                        sc_test_group,
                                      coalesce(tg.time_group, 'NULL')                                           time_group,
                                      coalesce(rp.rider_priority, 'NULL')                                        rider_priority,
                                      coalesce(rp.company_id, -1)                                        company_id,
--                                      dt.sc_test_group,
                             cl.class_type_desc,
                             cl.lob_desc,
                             fo.hour_key,
                             loc.city_name,
                              count(distinct
                               CASE when fo.ride_id is not null THEN fo.gett_order_id ELSE null end)    AS rides,
                             count(distinct fo.gett_order_id)                                            AS gross_orders,
--                             count(distinct fo.series_original_order_gk)                            AS net_orders,
                             count(distinct CASE
                                              when fo.rn_series_order = 1
                                                      THEN fo.series_original_order_gk end)                         AS net_orders,
                             count(distinct CASE
                                              when fo.order_status_key = 9
                                                and fo.rn_series_order = 1
                                                      THEN fo.series_original_order_gk end)                         AS net_rejected,
                             count(distinct CASE
                                              when fo.order_status_key = 4
                                                and fo.rn_series_order = 1
                                                     and fo.order_confirmed_datetime is null
                                                      THEN fo.series_original_order_gk end)                         AS net_cancelled_before_assign,
                             count(distinct CASE
                                              when fo.order_status_key = 4
                                                and fo.rn_series_order = 1
                                                     and fo.order_confirmed_datetime is not null
                                                      THEN fo.series_original_order_gk end)                         AS net_cancelled_after_assign,
                             count(distinct CASE
                                              when fo.order_status_key = 9
                                                      THEN fo.gett_order_id end)                         AS gross_rejected,
                             count(distinct CASE
                                              when fo.order_status_key = 4
                                                     and fo.order_confirmed_datetime is null
                                                      THEN fo.gett_order_id end)                         AS gross_cancelled_before_assign,
                             count(distinct CASE
                                              when fo.order_status_key = 4
                                                     and fo.order_confirmed_datetime is not null
                                                      THEN fo.gett_order_id end)                         AS gross_cancelled_after_assign,
                             sum(CASE
                                   when fo.order_confirmed_datetime is not null and routing is not null
                                          and fo.cancellations_time_in_seconds is null
                                     and fo.order_confirmed_datetime >= fo.order_datetime --edited
                                           THEN date_diff('second', fo.order_datetime, fo.order_confirmed_datetime)
                                     end)                                                           AS clean_assignment_time_num,
                             count(distinct CASE
                                              when fo.order_confirmed_datetime is not null and
                                                   routing is not null
                                     and fo.order_confirmed_datetime >= fo.order_datetime --edited
                                                     and fo.cancellations_time_in_seconds is null THEN fo.gett_order_id
                                                end)                                                AS clean_assignment_time_denum,
                             count(distinct fo.driver_gk)                                              riding_drivers,
                             count(distinct
                               CASE when fo.ride_id is not null THEN acc.order_assigned_by_cc end)    cc_assigned_rides,
                             sum(CASE when fo.m_order_eta > 0 THEN fo.m_order_eta end)                 order_eta_sum,
                             count(CASE when fo.m_order_eta > 0 THEN fo.m_order_eta end)               order_eta_denum,
                             sum(CASE
                                   when (fo.order_status_key =4 or fo.ride_id is not null) and routing is not null
                                     and fo.order_confirmed_datetime >= fo.order_datetime --edited
                                           THEN coalesce(
                                                  date_diff('second', fo.order_datetime, fo.order_confirmed_datetime),
                                                  fo.cancellations_time_in_seconds)
                                   ELSE null end)                                                   AS assignment_time_num,
                             sum(CASE
                                   when (fo.order_status_key =4 or fo.ride_id is not null) and routing is not null
                                           THEN coalesce(fo.cancellations_time_in_seconds, fo.m_routing_duration)
                                   ELSE null end)                                                   AS assignment_time_stats_overview_num,
                             count(distinct CASE
                                              when (fo.order_status_key =4 or fo.ride_id is not null) and routing is not null
                                     and fo.order_confirmed_datetime >= fo.order_datetime --edited
                                                      THEN fo.gett_order_id end)                         AS assignment_time_denum,
                             count(distinct
                               CASE when fo.m_routing_duration < 21
                                    THEN fo.gett_order_id ELSE null end) AS good_assignment,
                             sum(CASE
                                   when fo.ride_id is not null and fo.m_order_eta is not null THEN fo.m_order_eta
                                   ELSE 0 end)                                                      AS net_eta_num,
                             count(distinct CASE
                                              when fo.ride_id is not null and fo.m_order_eta is not null
                                                      THEN fo.gett_order_id end)                         AS net_eta_denum,
                             count(distinct CASE
                                              when fo.is_gett_fail = 1
                                                      THEN fo.gett_order_id end)                         AS gross_failed,
                             sum(fo.skip_count) AS skip_count





                      FROM fact_orders fo
                        --     LEFT JOIN data_two ON fo.gett_order_id = data_two.source_id and fo.country_symbol = data_two.env
                             LEFT JOIN data_three dt ON fo.gett_order_id = dt.source_id and fo.country_symbol = dt.env
                             LEFT JOIN time_groups_per_orderid_normalize tg ON fo.gett_order_id = tg.order_id and fo.country_symbol = tg.env
                             LEFT JOIN rider_pr rp ON fo.gett_order_id = rp.source_id and fo.country_symbol = rp.fetch_str_env
                             LEFT JOIN emilia_gettdwh.dwh_dim_locations_v loc
                               ON loc.location_key = fo.origin_location_key
                             LEFT JOIN data ON fo.gett_order_id = data.order_id and fo.country_symbol = data.env and data.rn = 1
                             LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v cl ON cl.class_type_key = fo.class_type_key
                             LEFT JOIN assigned_by_cc acc ON fo.gett_order_id = acc.order_assigned_by_cc and fo.country_symbol = acc.env
                             LEFT JOIN hive.analyst.schedule_20181120 sch
                               ON cast(sch.dow AS int) = day_of_week(data.event_at)
                                    and sch.routing_region = data.routing_region
                                    and hour(cast(sch.time_begin AS time)) <= hour(data.event_at)
                                    and hour(cast(sch.time_end AS time)) >= hour(data.event_at)
                      WHERE fo.date_key >= date_add('day', -80, current_date)
                     --   and cl.lob_category = 'Private Transportation'

--                         and fo.order_gk = 1000940121929
--                         and fo.date_key = date'2019-12-22'
--
--                         and data_two.test_group = 'bulk_match|experiment'
--                         and loc.city_name IN ('Eilat', 'Jerusalem')
-- GROUP BY 1
-- HAVING count(1)>1;
                      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,14,15,16--,17
                      )
--     SELECT * FROM orders_pivot;
    ,offers_pivot AS (
      SELECT
     --distinct coalesce(test_group, 'Load Index')                               test_group,
                                      CASE
                                        when (minute(fo.order_datetime) > 14 and minute(fo.order_datetime) < 30)
                                                THEN 'LoadIndex Test'
                                        when (minute(fo.order_datetime) > 44) THEN 'LoadIndex Test'
                                        ELSE 'LoadIndex Control' end                                   load_index_grop,
                                      case when

                                        substring(to_hex(md5(cast(cast(order_gk as varchar) as varbinary))), -1) in ('2','4','6','8','A','C','E')
                                        then 'Order gk control' else 'Order gk test' end delivery_test_group,

                                      fo.country_symbol,
                                      fo.date_key,
                                      coalesce(sch.time_period, 'No TimePeriod')                       time_period,
                                      coalesce(data.zone, loc.city_name)                               zone,
                                      coalesce(data.routing_region, loc.region_name)                   routing_region,
                                      fo.is_future_order_key,
                                      cl.class_group,
                                      coalesce(dt.sc_test_group, 'NULL')                                        sc_test_group,
                                      coalesce(rp.rider_priority, 'NULL')                                        rider_priority,
                                      coalesce(rp.company_id, -1)                                        company_id,
--                                      dt.sc_test_group,
                                      cl.class_type_desc,
                                      cl.lob_desc,
                                      loc.city_name,
                                      fo.hour_key,
                                      coalesce(tg.time_group, 'NULL')                                           time_group,
                                      sum(CASE when fof.is_withdrawned = 1 THEN 1 end)                 sum_wd,
                                      count(distinct fof.offer_gk)                                     total_offers,
                                      sum(CASE when fof.Driver_Response_Key = 1 THEN 1 ELSE 0 END) AS  ar_numerator,
                                      (sum(CASE when fof.Delivered_Datetime IS NOT NULL THEN 1 ELSE 0 END)
                                         - sum(CASE
                                                 when fof.Delivered_Datetime IS NOT NULL AND
                                                      fof.Is_Withdrawned = 1 AND
                                                      fof.Driver_Response_Key <> 1
                                                         THEN 1
                                                 ELSE 0 END))                                      AS  ar_denominator,
                                      count(distinct fof.driver_gk)                                    offered_drivers,
                                      sum(fof.distance_from_order_on_creation)                         offers_distance,
                                      sum(CASE
                                            when fof.offer_screen_eta is not null and fof.offer_screen_eta > 1
                                                    THEN fof.offer_screen_eta
                                            ELSE null end)                                             offers_eta_num,
                                      count(CASE
                                            when fof.offer_screen_eta is not null and fof.offer_screen_eta > 600
                                                    THEN fof.offer_screen_eta
                                            ELSE null end)                                             long_offers_eta_num,
                                      count(CASE
                                              when fof.offer_screen_eta is not null and fof.offer_screen_eta > 1
                                                      THEN fof.offer_screen_eta
                                              ELSE null end)                                           offers_eta_denum,
                                      count(distinct
                                        CASE when fof.offer_gk is null THEN fo.order_gk ELSE null end) unoffered,
                                      count(distinct CASE
                                              when fof.driver_unassigned_datetime is not null and year(fof.driver_unassigned_datetime) > 2000
                                                      THEN fo.order_gk
                                               ELSE null end)                                           driver_unassigned
                      FROM fact_orders fo
                        --     LEFT JOIN data_two ON fo.gett_order_id = data_two.source_id and fo.country_symbol = data_two.env
                             LEFT JOIN data_three dt ON fo.gett_order_id = dt.source_id and fo.country_symbol = dt.env
                             LEFT JOIN time_groups_per_orderid_normalize tg ON fo.gett_order_id = tg.order_id and fo.country_symbol = tg.env
                             LEFT JOIN rider_pr rp ON fo.gett_order_id = rp.source_id and fo.country_symbol = rp.fetch_str_env
                             LEFT JOIN emilia_gettdwh.dwh_fact_offers_v fof
                               ON fo.gt_order_gk = fof.order_gk and fof.date_key > current_date - interval '90' day and fo.country_symbol = fof.country_symbol
                             LEFT JOIN emilia_gettdwh.dwh_dim_locations_v loc
                               ON loc.location_key = fo.origin_location_key
                             LEFT JOIN data ON fo.gett_order_id = data.order_id
                                                 and fo.country_symbol = data.env
                                                 and data.rn = 1
                             LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v cl ON cl.class_type_key = fo.class_type_key
                             LEFT JOIN hive.analyst.schedule_20181120 sch
                               ON cast(sch.dow AS int) = day_of_week(data.event_at)
                                    and sch.routing_region = data.routing_region
                                    and hour(cast(sch.time_begin AS time)) <= hour(data.event_at)
                                    and hour(cast(sch.time_end AS time)) >= hour(data.event_at)
                      WHERE fo.date_key >= date_add('day', -80, current_date)
                      --  and cl.lob_category = 'Private Transportation'

--                         and fo.date_key = date'2019-12-22'
--
--                         and data_two.test_group = 'bulk_match|experiment'
--                         and loc.city_name IN ('Eilat', 'Jerusalem')   ;
                      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,14,15,16--, 17
                      )
SELECT distinct ord.*,
                of.sum_wd,
                of.offers_eta_num,
                of.offers_eta_denum,
                of.long_offers_eta_num,
                of.total_offers,
                unoffered,
                of.ar_denominator,
                of.ar_numerator,
                of.offers_distance,
                of.sum_wd,
                of.offered_drivers,
                of.driver_unassigned
FROM orders_pivot ord
       LEFT JOIN offers_pivot of ON --ord.test_group = of.test_group
                                       ord.company_id = of.company_id
                                      and ord.rider_priority = of.rider_priority
                                      and ord.sc_test_group = of.sc_test_group
                                      and ord.date_key = of.date_key
                                      and ord.time_period = of.time_period
                                      and ord.routing_region = of.routing_region
                                      and ord.class_group = of.class_group
                                      and ord.class_type_desc = of.class_type_desc
                                      and ord.lob_desc = of.lob_desc
                                      and ord.city_name = of.city_name
                                      and ord.is_future_order_key = of.is_future_order_key
                                      and ord.country_symbol = of.country_symbol
                                      --and ord.load_index_grop = of.load_index_grop
                                      -- delivery
                                      and ord.delivery_test_group = of.delivery_test_group
                                      and ord.zone = of.zone
                                      and ord.hour_key = of.hour_key
                                      and ord.time_group = of.time_group;



select
order_gk,

-- 1. md5(cast(cast(order_gk as varchar) as varbinary)) - get Hashed key for an order_gk
-- 2. to_hex() - get hexadecimal (16, hex) number as varchar
-- 3. substring(to_hex, -1) - get last bit. ('2','4','6','8','A','C','E') are even bits = control group,
-- odd bits - test group
substring(to_hex(md5(cast(cast(order_gk as varchar) as varbinary))), -1) last_bit,
case when
substring(to_hex(md5(cast(cast(order_gk as varchar) as varbinary))), -1) in ('2','4','6','8','A','C','E')
then 'control' else 'test' end "group"
from emilia_gettdwh.dwh_fact_orders_v
limit 10









