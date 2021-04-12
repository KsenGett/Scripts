-- Test for 23th February (3 days. 20 deliveries - 1000 rub)
-- All drivers (with no ftr, churned) from MSK except of those who made 19+ deliveries for 14 days before 2021-2-19

with GH AS (
    SELECT
    fdh.driver_gk,
    count(distinct CASE when (fdh.date_key between dd.ltp_date_key - interval '14' day and dd.ltp_date_key)
     and fdh.driver_status_key IN (2, 4, 5, 6) then date_key end) days,

    sum(CASE when (fdh.date_key between dd.ltp_date_key - interval '14' day and dd.ltp_date_key)
                    and fdh.driver_status_key IN (2, 4, 5, 6) THEN fdh.minutes_in_status ELSE 0 end)/60.0 AS gh,
    sum(case when (fdh.date_key between dd.ltp_date_key - interval '14' day and dd.ltp_date_key)
                    and fdh.driver_status_key IN (5, 6) THEN fdh.minutes_in_status ELSE 0 end)/60.0 in_ride

    FROM emilia_gettdwh.dwh_fact_drivers_hourly_v fdh
    left join emilia_gettdwh.dwh_dim_drivers_v dd on fdh.driver_gk = dd.driver_gk and dd.country_key = 2
    left join emilia_gettdwh.dwh_dim_vendors_v fl on dd.fleet_gk = fl.vendor_gk

     WHERE   1 = 1
                --and fdh.date_key between current_date - interval '40' day and current_date
                and fdh.country_key = 2
                and fl.vendor_name like '%courier%'
                --and dd.source_id in ({dasha_dr})
    GROUP BY 1
    )

, AR AS (
    SELECT
     fof.driver_gk,
     SUM(CASE WHEN (fof.date_key between dd.ltp_date_key - interval '14' day and dd.ltp_date_key)
     and fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) AS numerator, -- accepted


    (SUM(CASE WHEN (fof.date_key between dd.ltp_date_key - interval '14' day and dd.ltp_date_key)
                and fof.Delivered_Datetime IS NOT NULL or fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) --received
           - SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL AND fof.Is_Withdrawned=1
           AND fof.Driver_Response_Key<>1 THEN 1 ELSE 0 END) )  AS denominator

    FROM emilia_gettdwh.dwh_fact_offers_v fof
    left join emilia_gettdwh.dwh_dim_drivers_v dd on fof.driver_gk = dd.driver_gk and dd.country_key = 2
     LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v cl ON cl.class_type_key = fof.class_type_key
    WHERE lob_key IN (5,6)
    --and dd.source_id in ({dasha_dr})
    GROUP BY 1
    )
(
select
distinct source_id,
dd.registration_date_key,
dd.phone, dd.driver_name,
dd.ltp_date_key,
GH.gh, GH.days gh_days, GH.in_ride,
AR.numerator, AR.denominator,

fo.deliveries, fo.days, fo.days_15_deliveries,
days_19_del

from emilia_gettdwh.dwh_dim_drivers_v dd
left join emilia_gettdwh.dwh_dim_vendors_v fl on dd.fleet_gk = fl.vendor_gk
    left join --4 sec
      (
            select driver_gk,
            sum(coalesce(orders_OF,0)wo + coalesce(orders_NF, 0)) deliveries,
            count(distinct case when coalesce(nullif(orders_OF,0), nullif(orders_NF,0)) <> 0 then date_key end) days,
            count(distinct case when (orders_OF + coalesce(orders_NF, 0)) >= 15 then date_key end) days_15_deliveries,
            count(distinct case when (orders_OF_last_14_days + coalesce(orders_NF_last_14_days, 0)) > 19 then date_key end) days_19_del

            from
            (
            select
            fo.date_key,
            fo.driver_gk,
            -- orders from 5th to 18th Febr 2021
            count(distinct case when
                            (fo.date_key between current_date - interval '14' day and current_date) and
                            ordering_corporate_account_gk <> 20004730 and ct.class_family <> 'Premium'
                            then order_gk end) orders_OF_last_14_days,
            count(distinct case when
                            (fo.date_key between current_date - interval '14' day and current_date) and
                            ordering_corporate_account_gk = 20004730 then order_gk end) orders_NF_last_14_days,
            -- deliveries for driver's last 14th days
            count(distinct case when
                            (fo.date_key between dd.ltp_date_key - interval '14' day and dd.ltp_date_key) and
                            ordering_corporate_account_gk <> 20004730 and ct.class_family <> 'Premium'
                            then order_gk end) orders_OF,
            count(distinct case when
                            (fo.date_key between dd.ltp_date_key - interval '14' day and dd.ltp_date_key) and
                            ordering_corporate_account_gk = 20004730 then order_gk end) orders_NF

            from emilia_gettdwh.dwh_fact_orders_v fo
            left join emilia_gettdwh.dwh_dim_class_types_v AS ct
                ON ct.class_type_key = fo.class_type_key
            left join "emilia_gettdwh"."dwh_dim_locations" l on fo.origin_location_key = l.location_key
            left join emilia_gettdwh.dwh_dim_drivers_v dd on fo.driver_gk = dd.driver_gk and dd.country_key = 2
            left join emilia_gettdwh.dwh_dim_vendors_v fl on dd.fleet_gk = fl.vendor_gk

            where fo.lob_key in (5,6)
            and order_status_key = 7
            and fo.country_key = 2
            and fo.origin_location_key = 245
            and fl.vendor_name like '%courier%'

            group by 1,2
            )
            group by 1

        ) fo on fo.driver_gk = dd.driver_gk

left join AR on dd.driver_gk = AR.driver_gk
left join GH on dd.driver_gk = GH.driver_gk

where dd.country_key = 2
and fl.vendor_name like '%courier%'
and (fl.vendor_name like '%МСК%'
        or fl.vendor_gk in (200010351, 200012868, 200010116,200013818,200010350)) -- only moscow fleets
and (fo.days_19_del is null or fo.days_19_del = 0) -- no ftr or did less than 19 once within last 14 days
and dd.is_frozen <> 1
and dd.phone <> '8'

);


with GH AS (
    SELECT
    fdh.driver_gk,
    count(distinct CASE when (fdh.date_key between dd.ltp_date_key - interval '14' day and dd.ltp_date_key)
     and fdh.driver_status_key IN (2, 4, 5, 6) then date_key end) days,

    sum(CASE when (fdh.date_key between dd.ltp_date_key - interval '14' day and dd.ltp_date_key)
                    and fdh.driver_status_key IN (2, 4, 5, 6) THEN fdh.minutes_in_status ELSE 0 end)/60.0 AS gh,
    sum(case when (fdh.date_key between dd.ltp_date_key - interval '14' day and dd.ltp_date_key)
                    and fdh.driver_status_key IN (5, 6) THEN fdh.minutes_in_status ELSE 0 end)/60.0 in_ride

    FROM emilia_gettdwh.dwh_fact_drivers_hourly_v fdh
    left join emilia_gettdwh.dwh_dim_drivers_v dd on fdh.driver_gk = dd.driver_gk and dd.country_key = 2
    left join emilia_gettdwh.dwh_dim_vendors_v fl on dd.fleet_gk = fl.vendor_gk

     WHERE   1 = 1
                --and fdh.date_key between current_date - interval '40' day and current_date
                and fdh.country_key = 2
                and fl.vendor_name like '%courier%'
                --and dd.source_id in ({dasha_dr})
    GROUP BY 1
    )

, AR AS (
    SELECT
     fof.driver_gk,
     SUM(CASE WHEN (fof.date_key between dd.ltp_date_key - interval '14' day and dd.ltp_date_key)
     and fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) AS numerator, -- accepted


    (SUM(CASE WHEN (fof.date_key between dd.ltp_date_key - interval '14' day and dd.ltp_date_key)
                and fof.Delivered_Datetime IS NOT NULL or fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) --received
           - SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL AND fof.Is_Withdrawned=1
           AND fof.Driver_Response_Key<>1 THEN 1 ELSE 0 END) )  AS denominator

    FROM emilia_gettdwh.dwh_fact_offers_v fof
    left join emilia_gettdwh.dwh_dim_drivers_v dd on fof.driver_gk = dd.driver_gk and dd.country_key = 2
     LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v cl ON cl.class_type_key = fof.class_type_key
    WHERE lob_key IN (5,6)
    --and dd.source_id in ({dasha_dr})
    GROUP BY 1
    )
(
select
distinct source_id,
dd.registration_date_key,
dd.phone, dd.driver_name,
dd.ltp_date_key,
GH.gh, GH.days gh_days, GH.in_ride,
AR.numerator, AR.denominator,

orders_OF + coalesce(orders_NF, 0) deliveries,
fo.days

from emilia_gettdwh.dwh_dim_drivers_v dd
left join emilia_gettdwh.dwh_dim_vendors_v fl on dd.fleet_gk = fl.vendor_gk
    left join --4 sec
      (
            select
            fo.driver_gk,
            -- deliveries for driver's last 14th days
            count(distinct case when
                            (fo.date_key between dd.ltp_date_key - interval '14' day and dd.ltp_date_key)
                            then date_key end) days,
            count(distinct case when
                            (fo.date_key between dd.ltp_date_key - interval '14' day and dd.ltp_date_key) and
                            ordering_corporate_account_gk <> 20004730 and ct.class_family <> 'Premium'
                            then order_gk end) orders_OF,
            count(distinct case when
                            (fo.date_key between dd.ltp_date_key - interval '14' day and dd.ltp_date_key) and
                            ordering_corporate_account_gk = 20004730 then order_gk end) orders_NF

            from emilia_gettdwh.dwh_fact_orders_v fo
            left join emilia_gettdwh.dwh_dim_class_types_v AS ct
                ON ct.class_type_key = fo.class_type_key
            left join "emilia_gettdwh"."dwh_dim_locations" l on fo.origin_location_key = l.location_key
            left join emilia_gettdwh.dwh_dim_drivers_v dd on fo.driver_gk = dd.driver_gk and dd.country_key = 2
            left join emilia_gettdwh.dwh_dim_vendors_v fl on dd.fleet_gk = fl.vendor_gk

            where fo.lob_key in (5,6)
            and order_status_key = 7
            and fo.country_key = 2
            and fo.origin_location_key = 245
            and fl.vendor_name like '%courier%'

            group by 1

        ) fo on fo.driver_gk = dd.driver_gk

left join AR on dd.driver_gk = AR.driver_gk
left join GH on dd.driver_gk = GH.driver_gk

where dd.country_key = 2
and fl.vendor_name like '%courier%'
and (fl.vendor_name like '%МСК%'
        or fl.vendor_gk in (200010351, 200012868, 200010116,200013818,200010350)) -- only moscow fleets
and dd.is_frozen <> 1
and dd.phone <> '8'
and dd.source_id = 307642

)


-- fix deliveries before churn
select driver_gk,
            sum(coalesce(orders_OF,0) + coalesce(orders_NF, 0)) deliveries,
            count(distinct case when coalesce(nullif(orders_OF,0), nullif(orders_NF,0)) <> 0 then date_key end) days,
            count(distinct case when (coalesce(orders_OF,0) + coalesce(orders_NF, 0)) >= 15 then date_key end) days_15_deliveries

            from
            (
            select
            fo.date_key,
            fo.driver_gk,
            -- deliveries for driver's last 14th days
            count(distinct case when
                            (fo.date_key between date(sh.ltp_date_key) - interval '14' day and date(sh.ltp_date_key)) and
                            ordering_corporate_account_gk <> 20004730 and ct.class_family <> 'Premium'
                            then order_gk end) orders_OF,
            count(distinct case when
                            (fo.date_key between date(sh.ltp_date_key) - interval '14' day and date(sh.ltp_date_key)) and
                            ordering_corporate_account_gk = 20004730 then order_gk end) orders_NF

            from emilia_gettdwh.dwh_fact_orders_v fo
            join sheets."default".ab_test_support_ksen sh on sh.driver_gk = cast(fo.driver_gk as varchar)
            left join emilia_gettdwh.dwh_dim_class_types_v AS ct
                ON ct.class_type_key = fo.class_type_key


            where fo.lob_key in (5,6)
            and order_status_key = 7
            and fo.country_key = 2
            and fo.origin_location_key = 245

            group by 1,2
            )
group by 1

;

-- Get results

with gh as (
SELECT
    fdh.driver_gk,
    date_key,
    sum(fdh.minutes_in_status)/60.0 AS h_total, -- 1 offline 2 free 3 busy 4 in routing 5 iin ride 6 busy in ride
    sum(case when fdh.driver_status_key IN (2,4,5,6) THEN fdh.minutes_in_status ELSE 0 end)/60.0 gh_original,
    sum(case when fdh.driver_status_key IN (2) THEN fdh.minutes_in_status ELSE 0 end)/60.0 gh_free,
    sum(case when fdh.driver_status_key IN (3) THEN fdh.minutes_in_status ELSE 0 end)/60.0 busy,
    sum(case when fdh.driver_status_key IN (4) THEN fdh.minutes_in_status ELSE 0 end)/60.0 gh_routing,
    sum(case when fdh.driver_status_key IN (1) THEN fdh.minutes_in_status ELSE 0 end)/60.0 offline,
    sum(case when fdh.driver_status_key IN (5, 6) THEN fdh.minutes_in_status ELSE 0 end)/60.0 gh_in_ride

    FROM emilia_gettdwh.dwh_fact_drivers_hourly_v fdh

    WHERE   1 = 1
    and fdh.date_key >= date'2021-2-3'
    and fdh.country_key = 2
    and driver_gk in (

    GROUP BY 1,2
),

fo as
      (
            select
            distinct fo.driver_gk,
            fo.date_key,
            count(distinct case when
                            ordering_corporate_account_gk <> 20004730 and ct.class_family <> 'Premium'
                            then order_gk end) orders_OF,
            count(distinct case when
                            ordering_corporate_account_gk = 20004730 then order_gk end) orders_NF

            from emilia_gettdwh.dwh_fact_orders_v fo
            left join emilia_gettdwh.dwh_dim_class_types_v AS ct
                ON ct.class_type_key = fo.class_type_key
            left join "emilia_gettdwh"."dwh_dim_locations" l on fo.origin_location_key = l.location_key

            where fo.lob_key in (5,6)
            and order_status_key = 7
            and fo.country_key = 2
            and fo.date_key >= date'2021-2-3'
            and driver_gk in ()
            group by 1,2

),

, AR AS
    (
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
    and date_key >= date'2021-2-3'
    and driver_gk in ()
    GROUP BY 1,2
    )

(
select
gh.driver_gk,
gh.date_key, gh_original,
gh.h_total, gh.gh_in_ride,
gh.busy, gh.gh_free,
gh.offline, gh.gh_routing,
ar.numerator, ar.denominator,
coalesce(fo.orders_of, 0) + coalesce(fo.orders_nf, 0) deliveries

from GH as gh
left join fo on gh.driver_gk = fo.driver_gk and gh.date_key = fo.date_key
left join AR as ar on ar.driver_gk = fo.driver_gk and ar.date_key = fo.date_key
);


