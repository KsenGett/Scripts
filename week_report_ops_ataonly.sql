-- select
-- subperiod, min(date_key),
-- sum(count_ata)
--
-- from (

with ata as (
select fo.date_key,
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
    dl.city_name,
    order_gk,

    (CASE when order_status_key = 7 THEN 'completed' ELSE 'Cancelled ON Arrival' end) AS order_status,

    CASE when date_diff('second', fo.order_datetime,fo.driver_arrived_datetime)*1.00/60 > 0 THEN
    date_diff('second', fo.order_datetime, fo.driver_arrived_datetime)*1.00/60 end AS ata,

    (CASE when date_diff('second', fo.order_datetime,fo.driver_arrived_datetime)*1.00/60 > 0 THEN 1
    end) count_ata --  В Катином скрипте 0 учитываются


    from emilia_gettdwh.dwh_fact_orders_v fo
        LEFT JOIN  data_vis.periods_v AS tp ON tp.hour_key = fo.hour_key and tp.date_key = fo.date_key
           and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
        LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
                  ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key
        LEFT JOIN "emilia_gettdwh"."dwh_dim_account_managers_v" am ON am."account_manager_gk" = ca."account_manager_gk"
        LEFT JOIN sheets."default".delivery_corp_accounts_20191203 AS accounts
            ON cast(accounts.company_gk AS bigint)=fo.ordering_corporate_account_gk
        left join emilia_gettdwh.dwh_dim_locations_v dl on fo.origin_location_key = dl.location_key

    where tp.timecategory is not null
        --and fo.date_key >= date'2020-07-01'
        and fo.date_key BETWEEN date'2019-12-01' AND date'2020-04-01'
        and fo.country_key = 2
        and fo.lob_key in (5,6)
        and ct.class_family not IN ('Premium')
        and ct.class_group not like 'Test'
        and fo.ordering_corporate_account_gk not in (20004730,200017459)
        --and fo.order_status_key = 7 --compl, cancelled - AV.ru W36 with 7 974, w/o - 1012 = 4%
union

    select
    date(fd.scheduled_at) AS date_key,
    tp.timecategory,
    tp.subperiod,
    tp.period,
    tp.subperiod2 AS time_period,
    'NF' as platform,
    'eCommerce' client_type,
    company_gk ordering_corporate_account_gk,
    ca.corporate_account_name,
    ca.corporate_account_name name_internal,
    dl.city_name,
    fd.delivery_gk order_gk,
    ds.delivery_status_desc order_status ,

    (CASE when date_diff('second', coalesce(fd.requested_schedule_time, fd.scheduled_at) , fd.arrived_at)*1.00/60 > 0 THEN
    date_diff('second', coalesce(fd.requested_schedule_time, fd.scheduled_at) , fd.arrived_at) end)*1.00/60 AS ata,
    (CASE when date_diff('second', coalesce(fd.requested_schedule_time, fd.scheduled_at) , fd.arrived_at)*1.00/60 >= 0 THEN 1 end) count_ata

    FROM "model_delivery"."dwh_fact_deliveries_v" fd
        left join "model_delivery".dwh_dim_delivery_statuses_v ds on ds.delivery_status_id = fd.delivery_status_id
        LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca ON ca.corporate_account_gk = fd.company_gk
            and ca.country_symbol = 'RU'
        LEFT JOIN  data_vis.periods_v AS tp ON tp.date_key = date(fd.scheduled_at) and tp.hour_key = 0
        and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
        --journeys
        LEFT JOIN model_delivery.dwh_fact_journeys_v j ON fd.journey_gk = j.journey_gk
        and j.country_symbol ='RU' and date(j.created_at) >= date'2020-08-01'
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v ct ON j.class_type_key=ct.class_type_key
        left join emilia_gettdwh.dwh_dim_locations_v dl on fd.pickup_location_key = dl.location_key

    WHERE fd.country_symbol ='RU'
      and lower(ca.corporate_account_name) not like '%test%'
      --and date(fd.scheduled_at) >= date'2020-07-01'
      and date(fd.scheduled_at) BETWEEN date'2019-12-01' AND date'2020-04-01'
      and tp.timecategory is not null
      --and ct.class_type_desc like '%ondemand%'
      and fd.company_gk  not in (200017459, 20004730)
      and (date_diff('second', fd.created_at, fd.scheduled_at))*1.00/60 <= 20
      and ds.delivery_status_desc in ('completed', 'not_delivered', 'cancelled', 'rejected')
)

(select platform, name_internal, subperiod, time_period, client_type,
    corporate_account_name,  order_status,
    ordering_corporate_account_gk,time_period,timecategory,date_key,"period",
    city_name,
    --(case when ata is null then -1 end) null_ata,
    count(distinct order_gk) orders,
    sum(ata) ata_sum,
    sum(ata)/sum(count_ata) ata_avg_w,
    sum(count_ata) count_ata
    , count(case when ata > 30 then ata else null end) ata_over_30,
    count(case when ata > 20 then ata else null end) ata_over_20

from ata
group by 1,2,3,4,5,6,7,8,9,10,11,12,13)
-- )
-- where 1=1
-- and timecategory ='4.Months'
-- and platform = 'NF'
-- group by 1;
--and platform = 'NF'

select min(scheduled_at)
from model_delivery.dwh_fact_deliveries_v
where country_symbol = 'RU'

-- TODO report!
select *
from emilia_gettdwh.dwh_fact_orders_v where order_gk = 20001348663971;

--HEXafgon
CASE when hex_code(origin_latitude, origin_longitude) IN (54082050, 54082056, 54082058, 54082080, 54082082, 54081707, 54082049, 54082051, 54082057, 54082059, 54082081, 54082083, 54082089, 54082091, 54081702, 54081708, 54081710, 54082052, 54082054, 54082060, 54082062, 54082084, 54082086, 54082092, 54082094, 54081679, 54081701, 54081703, 54081709, 54081711, 54082053, 54082055, 54082061, 54082063, 54082085, 54082087, 54082093, 54082095, 54082181, 54081688, 54081690, 54081712, 54081714, 54081720, 54081722, 54082064, 54082066, 54082072, 54082074, 54082096, 54082098, 54082104, 54082106, 54082192, 54081683, 54081689, 54081691, 54081713, 54081715, 54081721, 54081723, 54082065, 54082067, 54082073, 54082075, 54082097, 54082099, 54082105, 54082107, 54082193, 54082195, 54081684, 54081686, 54081692, 54081694, 54081716, 54081718, 54081724, 54081726, 54082068, 54082070, 54082076, 54082078, 54082100, 54082102, 54082108, 54082110, 54082196, 54082198, 54081599, 54081685, 54081687, 54081693, 54081695, 54081717, 54081719, 54081725, 54081727, 54082069, 54082071, 54082077, 54082079, 54082101, 54082103, 54082109, 54082111, 54082197, 54082199, 54081640, 54081642, 54081728, 54081730, 54081736, 54081738, 54081760, 54081762, 54081768, 54082114, 54082120, 54082122, 54082144, 54082146, 54082152, 54082154, 54082240, 54082242, 54081635, 54081641, 54081643, 54081729, 54081731, 54081737, 54081739, 54081761, 54082123, 54082145, 54082147, 54082153, 54082155, 54082241, 54081636, 54081638, 54081644, 54081646, 54081732, 54081734, 54081740, 54081742, 54082126, 54082148, 54082150, 54082156, 54082158, 54082244, 54081615, 54081637, 54081639, 54081645, 54081647, 54081733, 54081735, 54082127, 54082149, 54082151, 54082157, 54082159, 54081624, 54081626, 54081648, 54081650, 54081656, 54081658, 54081744, 54081746, 54082138, 54082160, 54082162, 54082168, 54082170, 54081625, 54081627, 54081649, 54081651, 54081657, 54081659, 54081745, 54082137, 54082139, 54082161, 54082163, 54082169, 54081628, 54081630, 54081652, 54081654, 54081660, 54081662, 54081748, 54082140, 54082142, 54082164, 54082166, 54082172, 54081629, 54081631, 54081653, 54081655, 54081661, 54081663, 54082135, 54082141, 54082143, 54082165, 54082167, 54081794, 54081800, 54081802, 54081824, 54081826, 54081832, 54081834, 54081920, 54082304, 54082306, 54082312, 54082314, 54082336, 54082338, 54081793, 54081795, 54081801, 54081803, 54081825, 54081827, 54081833, 54081835, 54081921, 54081963, 54082305, 54082307, 54082313, 54082315, 54082337, 54081796, 54081798, 54081804, 54081806, 54081828, 54081830, 54081836, 54081838, 54081924, 54081926, 54081932, 54081934, 54081958, 54081964, 54081966, 54082308, 54082310, 54082316, 54082318, 54082340, 54081797, 54081799, 54081805, 54081807, 54081829, 54081831, 54081837, 54081839, 54081925, 54081927, 54081933, 54081935, 54081957, 54081959, 54081965, 54081967, 54082309, 54082311, 54082317, 54081808, 54081810, 54081816, 54081818, 54081840, 54081842, 54081848, 54081850, 54081936, 54081938, 54081944, 54081946, 54081968, 54081970, 54081976, 54081978, 54082320, 54082322, 54081809, 54081811, 54081817, 54081819, 54081841, 54081843, 54081849, 54081851, 54081937, 54081939, 54081945, 54081947, 54081969, 54081971, 54081977, 54081979, 54081812, 54081814, 54081820, 54081822, 54081844, 54081846, 54081852, 54081854, 54081940, 54081942, 54081948, 54081950, 54081972, 54081974, 54081980, 54081813, 54081815, 54081821, 54081823, 54081845, 54081847, 54081853, 54081855, 54081941, 54081943, 54081949, 54081951, 54081973, 54081975, 54081856, 54081858, 54081864, 54081866, 54081888, 54081890, 54081896, 54081898, 54081984, 54081986, 54081992, 54081857, 54081859, 54081865, 54081867, 54081889, 54081891, 54081897, 54081899, 54081860, 54081862, 54081868, 54081861)
THEN 'TTK w/o Sadovoye'
when hex_code(origin_latitude, origin_longitude) IN (54081725, 54081727, 54082069, 54082071, 54082077, 54081760, 54081762, 54081768, 54081770, 54082112, 54082114, 54082120, 54082122, 54081739, 54081761, 54081763, 54081769, 54081771, 54082113, 54082115, 54082121, 54082123, 54081740, 54081742, 54081764, 54081766, 54081772, 54081774, 54082116, 54082118, 54082124, 54082126, 54081735, 54081741, 54081743, 54081765, 54081767, 54081773, 54081775, 54082117, 54082119, 54082125, 54082127, 54081744, 54081746, 54081752, 54081754, 54081776, 54081778, 54081784, 54081786, 54082128, 54082130, 54082136, 54082138, 54081745, 54081747, 54081753, 54081755, 54081777, 54081779, 54081785, 54081787, 54082129, 54082131, 54082137, 54081662, 54081748, 54081750, 54081756, 54081758, 54081780, 54081782, 54081788, 54081790, 54082132, 54082134, 54082140, 54081663, 54081749, 54081751, 54081757, 54081759, 54081781, 54081783, 54081789, 54081791, 54082133, 54082135, 54081834, 54081920, 54081922, 54081928, 54081930, 54081952, 54081954, 54081960, 54081962, 54082304, 54081835, 54081921, 54081923, 54081929, 54081931, 54081953, 54081955, 54081961, 54081963, 54081924, 54081926, 54081932, 54081934, 54081956, 54081958, 54081964, 54081927, 54081933, 54081935, 54081957, 54081959)
THEN 'Sadovoye ring'
 ELSE 'other' end AS area,




 --TO COMPARE
 /*
(select platform ,  subperiod, name_internal,
        sum(ata)/sum(count_ata) ata_avg_w, sum(count_ata) count_ata
from ata
where subperiod in ('W37', 'W38','W39','W36')
group by 1,2
order by 1,2);
  */


--> (Kate's "Mvideo..." NF) DIFFERENCE FROM MINE:
-- 1) I do not calculate 0 ata,
-- 2) I filter by (date_diff('second', fd.created_at, fd.scheduled_at))*1.00/60 <= 20
-- 3) I do not calculate negative ATA

--> (Kate's "SLA final" OF) DIFFERENCE FROM MINE:
-- 1) I do not calculate 0 ata
-- 2) I calculate all statuses

with ata as (
select fo.date_key,
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
    dl.city_name,
    order_gk,

    (CASE when order_status_key = 7 THEN 'completed' ELSE 'Cancelled ON Arrival' end) AS order_status,

    CASE when date_diff('second', fo.order_datetime,fo.driver_arrived_datetime)*1.00/60 > 0 THEN
    date_diff('second', fo.order_datetime, fo.driver_arrived_datetime)*1.00/60 end AS ata,

    (CASE when date_diff('second', fo.order_datetime,fo.driver_arrived_datetime)*1.00/60 > 0 THEN 1
    end) count_ata --  В Катином скрипте 0 учитываются


    from emilia_gettdwh.dwh_fact_orders_v fo
        LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.hour_key = fo.hour_key and tp.date_key = fo.date_key
           and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
        LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
                  ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key
        LEFT JOIN "emilia_gettdwh"."dwh_dim_account_managers_v" am ON am."account_manager_gk" = ca."account_manager_gk"
        LEFT JOIN sheets."default".delivery_corp_accounts_20191203 AS accounts
            ON cast(accounts.company_gk AS bigint)=fo.ordering_corporate_account_gk
        left join emilia_gettdwh.dwh_dim_locations_v dl on fo.origin_location_key = dl.location_key

    where tp.timecategory is not null
        --and fo.date_key >= date'2020-07-01'
        and fo.date_key BETWEEN date'2019-12-01' AND date'2020-04-01'
        and fo.country_key = 2
        and fo.lob_key in (5,6)
        and ct.class_family not IN ('Premium')
        and ct.class_group not like 'Test'
        and fo.ordering_corporate_account_gk not in (20004730,200017459)
        --and fo.order_status_key = 7 --compl, cancelled - AV.ru W36 with 7 974, w/o - 1012 = 4%
union

    select
    date(fd.scheduled_at) AS date_key,
    tp.timecategory,
    tp.subperiod,
    tp.period,
    tp.subperiod2 AS time_period,
    'NF' as platform,
    'eCommerce' client_type,
    company_gk ordering_corporate_account_gk,
    ca.corporate_account_name,
    ca.corporate_account_name name_internal,
    dl.city_name,
    fd.delivery_gk order_gk,
    ds.delivery_status_desc order_status ,

    (CASE when date_diff('second', coalesce(fd.requested_schedule_time, fd.scheduled_at) , fd.arrived_at)*1.00/60 > 0 THEN
    date_diff('second', coalesce(fd.requested_schedule_time, fd.scheduled_at) , fd.arrived_at) end)*1.00/60 AS ata,
    (CASE when date_diff('second', coalesce(fd.requested_schedule_time, fd.scheduled_at) , fd.arrived_at)*1.00/60 >= 0 THEN 1 end) count_ata

    FROM "model_delivery"."dwh_fact_deliveries_v" fd
        left join "model_delivery".dwh_dim_delivery_statuses_v ds on ds.delivery_status_id = fd.delivery_status_id
        LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca ON ca.corporate_account_gk = fd.company_gk
            and ca.country_symbol = 'RU'
        LEFT JOIN  emilia_gettdwh.periods_v AS tp ON tp.date_key = date(fd.scheduled_at) and tp.hour_key = 0
        and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
        --journeys
        LEFT JOIN model_delivery.dwh_fact_journeys_v j ON fd.journey_gk = j.journey_gk
        and j.country_symbol ='RU' and date(j.created_at) >= date'2020-08-01'
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v ct ON j.class_type_key=ct.class_type_key
        left join emilia_gettdwh.dwh_dim_locations_v dl on fd.pickup_location_key = dl.location_key

    WHERE fd.country_symbol ='RU'
      and lower(ca.corporate_account_name) not like '%test%'
      --and date(fd.scheduled_at) >= date'2020-07-01'
      and date(fd.scheduled_at) BETWEEN date'2019-12-01' AND date'2020-04-01'
      and tp.timecategory is not null
      and ct.class_type_desc like '%ondemand%'
      and fd.company_gk  not in (200017459, 20004730)
      and (date_diff('second', fd.created_at, fd.scheduled_at))*1.00/60 <= 20
      and ds.delivery_status_desc in ('completed', 'not_delivered', 'cancelled', 'rejected')
)

(select platform, name_internal, subperiod, time_period, client_type,
    corporate_account_name,  order_status,
    ordering_corporate_account_gk,time_period,timecategory,date_key,"period",
    city_name,
    --(case when ata is null then -1 end) null_ata,
    count(distinct order_gk) orders,
    sum(ata) ata_sum,
    sum(ata)/sum(count_ata) ata_avg_w,
    sum(count_ata) count_ata
    , count(case when ata > 30 then ata else null end) ata_over_30

from ata
group by 1,2,3,4,5,6,7,8,9,10,11,12,13)