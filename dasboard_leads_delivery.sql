with main as (

    with leads as (
        -- agents
            select distinct substring(d.phone ,-10) phone_number, d.driver_name as "name",
            '0' courier_details, 'Nan' request_id,
            'Nan' as city, d.registration_date_key,
            'agent' as "source",driver_gk, max(d.registration_date_key) as "lead_date"

            from emilia_gettdwh.dwh_dim_drivers_v d

            where fleet_gk in (200014202,200016265,200016266,
                                                        200016267,200016359,200016361)
            and d.registration_date_key >= date'2020-01-01'
            and d.driver_gk <> 2000683923
            group by 1,2,3,4,5,6,7,8
        UNION
        -- external sources
            select distinct phone_number, "name", l.courier_details, l.request_id,
            l.city, d.registration_date_key,
            "source", d.driver_gk, max(date(lead_date)) as lead_date

            FROM sheets."default".delivery_courier_leads_new l
            LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON substring(d.phone, -10) = l.phone_number
                and l.phone_number <> '8'
                and d.country_key = 2
                and d.driver_gk <> 2000683923
                and d.phone not IN ('89999999999', '8', '')
            LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v fl
            ON d.fleet_gk = fl.vendor_gk

            WHERE "source" <> 'source' --filter the bug that occured bacauese of union of tables IN google sheet
            and phone_2 <> 'phone_2'
            and fl.vendor_name like '%courier%'
            GROUP BY 1,2,3,4,5,6,7,8
        )
         ,
         gt as (
                select created_at, auditable_id
                from "gt-ru".gettaxi_ru_production.audits
                where  auditable_type = 'Driver'
                and created_at >= timestamp'2020-07-10 00:00:00' --first day of lead
                )

    (SELECT distinct dcln.driver_gk,
    dcln.name AS lead_name,
    gt_d.name AS registration_name,
    dcln.phone_number,
    dcln.courier_details,
    date(dcln.lead_date) AS lead_date,
    dcln.source,
    dcln.request_id,
    dcln.city,
    dcln.registration_date_key,
    (CASE when d.ftp_date_key = date '1900-01-01' THEN null ELSE  d.ftp_date_key end) AS first_time_ride_date,
    dce.date_start AS exam_date,
    dce.success = 'Сдал' as is_exam_passed,
    
    fl.vendor_name like '%courier%' is_couerier_vendor,
    gt_d.is_frozen = 1 is_frozen,
    date(gt_d.frozen_since)>= date(dcln.lead_date) frozen_after_lead, --and (gt_d.frozen_comment not like '%Не работал более%')
    --(case when substring("audited_changes", strpos("audited_changes", 'is_frozen:')+13, 1) = 't' then
    --date(gt.created_at) end) >= date(dcln.lead_date) as was_unfrozen_after_lead,

    max(date(gt.created_at)) >= date(dcln.lead_date) gt_change_after_lead ,
    max(date(gt.created_at)) gt_change_date,

    min(fo.date_key) first_delivery_date, -- it is in first delivery
    min(case when fo.date_key >= date(dcln.lead_date) then fo.date_key end) fdd_after_lead,

    -- orders 2 month before
    count(case when (fo.date_key >= date_add('month',-2, date(dcln.lead_date))and fo.date_key < date(dcln.lead_date))
        then fo.order_gk end)<> 0 as were_deliv_2month_before_lead,
    min(fdc.from_date_key) - interval '1' day AS delivery_class_activation_date --first delivery class activation date


    FROM leads dcln -- "dcln" is ald short name for sheet delivery_courier_leads_new
        --exams
        LEFT JOIN sheets."default"."delivery_courier_exams" dce ON substring (dce.phone, -10) = dcln.phone_number
            and dce.phone <> '8'
        --d
        left join emilia_gettdwh.dwh_dim_drivers_v d ON d.driver_gk = dcln.driver_gk
        --gt-ru names
        LEFT JOIN "gt-ru".gettaxi_ru_production.drivers gt_d ON gt_d.id = d.source_id
        --classes
        LEFT JOIN "emilia_gettdwh"."dwh_fact_drivers_classes_v" fdc ON fdc.driver_gk = d.driver_gk
            and fdc.is_current_allocation = 1
        LEFT JOIN "emilia_gettdwh"."dwh_dim_class_types_v" ct ON ct.class_type_key = fdc.class_type_key
            and ct.lob_key IN (5,6) and ct.country_key = 2 and is_current_allocation = 1
        -- fo
        left join emilia_gettdwh.dwh_fact_orders_v fo on d.driver_gk = fo.driver_gk
            and fo.lob_key in (5,6) and fo.order_status_key = 7
            and fo.country_key = 2 and fo.date_key >= date '2020-07-10'
            and fo.ordering_corporate_account_gk <> 20004730
        --vendor name
        left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
            and fl.country_key = 2

        -- gt_ru activity
        left join gt on gt.auditable_id = d.source_id

        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
)
, drivers_first_day as
    (
        (
        select distinct fo.driver_gk, "source", --min(date_key) ftr_after_lead,
            count(distinct fo.order_gk) as deliveries_num_after_lead

        from  sheets."default".delivery_courier_leads_new dcln
            LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON substring(d.phone,-10) = dcln.phone_number
                and dcln.phone_number not in ('89999999999', '8', '')
                and d.country_key = 2
            left join emilia_gettdwh.dwh_fact_orders_v fo on d.driver_gk = fo.driver_gk

            where fo.country_key = 2
            and fo.lob_key in (5,6)
            and fo.order_status_key = 7
            and fo.date_key >= date(dcln.lead_date)
            and fo.ordering_corporate_account_gk <> 20004730
            group by 1,2
        )
--2000563274 2020-04-20 ftr 1 delivery
        union
        (select distinct d.driver_gk, 'agent' "source", --min(date_key) ftr_after_lead,
            count(distinct fo.order_gk) as deliveries_num_after_lead

            from "emilia_gettdwh"."dwh_dim_drivers_v" d
            left join emilia_gettdwh.dwh_fact_orders_v fo
                ON fo.driver_gk = d.driver_gk
                and d.country_key = 2 and lob_key in (5,6)

            where fo.fleet_gk in (200014202,200016265,200016266,200016267)
            and fo.order_status_key = 7
            and d.registration_date_key >= date'2020-01-01'
            and date_key >= d.registration_date_key
            and fo.lob_key in (5,6)
            and substring(d.phone, -10) not in ('', '2222222222', '3333333333')
            and d.driver_gk <> 2000683923
            and fo.country_key = 2
            group by 1,2)
    )
(
select main.*, deliveries_num_after_lead, date_diff('day', lead_date, main.fdd_after_lead) lead_to_ftr_days,
-- is activated logic
(case when main.driver_gk is not null then
    (case when is_couerier_vendor = true then
        (case when registration_date_key < lead_date then
            (case when were_deliv_2month_before_lead = false then
                (case when is_frozen = true then
                    (case when frozen_after_lead = true then 1 else 0 end)
                                                    else 1 end)
                                                else 0 end)
                                            else 1 end)
                                    else 0 end)
                            else 0 end) is_lead

from main
left join drivers_first_day dfd on main.driver_gk = dfd.driver_gk
    and main.source = dfd.source
    );








-- leads deliveries
with main as (
with orders as (
    with leads as (
        -- agents
        -- there is no information about CITY - future step, to take from FO
            select distinct substring(d.phone ,-10) phone_number, d.driver_name as "name",
            '0' as courier_details, '0' as request_id, 'Nan' as city,
            'agent' as "source", d.registration_date_key as "lead_date"

            from emilia_gettdwh.dwh_dim_drivers_v d
            left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
                and fleet_gk in (200014202,200016265,200016266,200016267)

            where fleet_gk in (200014202,200016265,200016266,200016267,200016359,200016361)
            and d.ftp_date_key >= date'2019-07-01'
            and
        UNION
        -- external sources
            select distinct phone_number, "name",
            courier_details, request_id, city, "source", max(date(lead_date)) as lead_date
            from sheets."default".delivery_courier_leads_new
            where "source" <> 'source' --filter the bug that occured bacauese of union of tables in google sheet
            and phone_2 <> 'phone_2'
            group by 1,2,3,4,5,6
        )

    (select d.driver_gk, "source", fo.date_key, lead_date, count(fo.order_gk) orders

    FROM leads dcln -- "dcln" is ald short name for sheet delivery_courier_leads_new
    LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON substring(d.phone, -10) = dcln.phone_number
                and dcln.phone_number <> '8'
                and country_key = 2
    left join emilia_gettdwh.dwh_fact_orders_v fo on fo.driver_gk = d.driver_gk
        and fo.country_key = 2 and fo.date_key >= date '2020-07-01' and lob_key in (5,6)
        where fo.lob_key in (5,6)
        and fo.order_status_key = 7
        group by 1,2,3,4)
    )

    (select driver_gk, date_key, "source", lead_date,
        sum(orders) over(partition by driver_gk order by date_key asc) Nth_ride
    from orders)
)


(select main.date_key,
tp.timecategory,
    tp.subperiod,
    tp.period,
    tp.subperiod2 AS time_period, driver_gk, "source", lead_date, Nth_ride,
    min(main.date_key) ftr_date

from main
lEFT JOIN  emilia_gettdwh.periods_v tp ON tp.date_key = main.date_key and tp.hour_key = 0
        and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
where tp.timecategory is not null
group by 1,2,3,4,5,6,7,8,9);


select distinct driver_gk, date_key
from desc emilia_gettdwh.dwh_fact_orders_v fo
left join --2sec
    (
        select
        distinct courier_gk,
        date(created_at) date_key,
        count(distinct delivery_gk) deliveries,
        count(distinct journey_gk) journeys

        from model_delivery.dwh_fact_deliveries_v

        where
        date(created_at) >= date'2020-07-01'
        and delivery_status_id = 4

        group by 1,2

    ) md on md.courier_gk  = fo.driver_gk and md.date_key = fo.date_key

where
fleet_gk in (200014202,200016265,200016266,200016267,200016359,200016361)
















--FTR dynamic
with main as(
    with leads as (
        -- agents
            select distinct fo.driver_gk,
            d.driver_name as "name",
            'Nan' as city, d.registration_date_key,
            'agent' as "source", d.registration_date_key as "lead_date"

            from emilia_gettdwh.dwh_dim_drivers_v d
            left join emilia_gettdwh.dwh_fact_orders_v fo on fo.driver_gk = d.driver_gk


            where fo.fleet_gk in (200014202,200016265,200016266,
                      200016267,200016359,200016361)
            and d.registration_date_key >= date'2020-07-01'
            and lob_key in (5,6) and d.country_key =2 and order_status_key = 7
            and date_key >= date'2020-07-01'
            and d.driver_gk <> 2000683923
        UNION
        -- external sources
            select distinct driver_gk, "name",
            l.city, d.registration_date_key,
            "source", max(date(lead_date)) as lead_date

            FROM sheets."default".delivery_courier_leads_new l
            LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d
                ON substring(d.phone, -10) = l.phone_number
                    and l.phone_number <> '8'
                    and d.country_key = 2
                    and d.driver_gk <> 2000683923
                    and d.phone not IN ('89999999999', '8', '')
                    and d.registration_date_key >= date'2020-07-01'
            LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v fl
                ON d.fleet_gk = fl.vendor_gk

            WHERE "source" <> 'source' --filter the bug that occured bacauese of union of tables IN google sheet
            and phone_2 <> 'phone_2'
            and fl.vendor_name like '%courier%'
            GROUP BY 1,2,3,4,5
        )

    (
    select distinct dcln.driver_gk, "source",  lead_date,
    min(case when date_key >= lead_date then date_key end) ftr_date

    FROM leads dcln -- "dcln" is ald short name for sheet delivery_courier_leads_new

    left join emilia_gettdwh.dwh_fact_orders_v fo on fo.driver_gk = dcln.driver_gk
        and fo.country_key = 2 and fo.date_key >= date'2020-07-01' and lob_key in (5,6)
        and fo.order_status_key = 7

        where 1=1
        --and fo.date_key >= lead_date
        group by 1,2,3
        )
)
(select main.*, tp.timecategory, tp.subperiod, tp.period, tp.subperiod2 AS time_period
from main
lEFT JOIN  emilia_gettdwh.periods_v tp ON tp.date_key = main.ftr_date and tp.hour_key = 0
        and tp.timecategory IN ('2.Dates', '3.Weeks', '4.Months')
        and tp.timecategory is not null
)
;


-- Cities
    with leads as (
        -- agents
        -- there is no information about CITY - future step, to take from FO
            select distinct substring(d.phone ,-10) phone_number, d.driver_name as "name",
            '0' as courier_details, '0' as request_id, 'Nan' as city,
            'agent' as "source", max(d.registration_date_key) as "lead_date"

            from emilia_gettdwh.dwh_dim_drivers_v d
            left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
                and fleet_gk in (200014202,200016265,200016266,200016267)

            where fleet_gk in (200014202,200016265,200016266,200016267)
            and d.registration_date_key >= date'2020-05-01'
            and substring (d.phone, -10) not in ('', '3333333333', '2222222222')
            and d.phone is not null
            and d.registration_date_key >= date'2020-05-01'
            and d.driver_gk <> 2000683923

            group by 1,2,3,4,5,6
        UNION
        -- external sources
            select distinct phone_number, "name",
            courier_details, request_id, city, "source", max(date(lead_date)) as lead_date
            from sheets."default".delivery_courier_leads_new
            where "source" <> 'source' --filter the bug that occured bacauese of union of tables in google sheet
            and phone_number not in ('', '3333333333', '2222222222', '9999999999')
            group by 1,2,3,4,5,6
        )

    (select dcln.phone_number, d.driver_gk, "source",
    (case when source = 'agent' then (case when vendor_name like '%КУРЬЕРЫ%' then substring(vendor_name, 9, 4)
    else vendor_name end) else dcln.city end) source_city, loc.city_name delivery_city,

    case when vendor_name like '%КУРЬЕРЫ%' then substring(vendor_name, 9, 4)
    else vendor_name end fleet_city,
    max(lead_date) lead_date

    FROM leads dcln -- "dcln" is ald short name for sheet delivery_courier_leads_new
    LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON substring(d.phone, -10) = dcln.phone_number
                and d.phone not in ('8', '')
                and country_key = 2
    left join emilia_gettdwh.dwh_fact_orders_v fo on fo.driver_gk = d.driver_gk
        and fo.country_key = 2 and fo.date_key >= date '2020-01-01' and lob_key in (5,6)
    left join emilia_gettdwh.dwh_dim_locations_v loc
        on fo.origin_location_key = loc.location_key
    left join emilia_gettdwh.dwh_dim_vendors_v fl on fl.vendor_gk = d.fleet_gk
            and fl.country_key = 2
    group by 1,2,3,4,5,6
);



;




