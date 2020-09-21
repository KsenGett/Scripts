with
main as (

    SELECT distinct d.driver_gk,
    dcln.name AS lead_name,
    gt_d.name AS registration_name,
    dcln.phone_number,
    dcln.courier_details,
    date(dcln.lead_date) AS lead_date,
    dcln.source,
    dcln.request_id,
    dcln.city,
    d.registration_date_key,
    --fdc.is_current_allocation is_delivery_class_active,

    (CASE when d.ftp_date_key = date '1900-01-01' THEN null ELSE  d.ftp_date_key end) AS first_time_ride_date,
    dce.date_start AS exam_date,
    (CASE when dce.success = 'Сдал' THEN 1 ELSE 0 end) AS Training_completed,
    min(fdc.from_date_key) - interval '1' day AS delivery_class_activation_date --first delivery class activation date

    FROM sheets."default".delivery_courier_leads_new dcln
        --exams
        LEFT JOIN sheets."default"."delivery_courier_exams" dce ON dce.phone = dcln.phone_number
            and dcln.phone_number <> '8'
        --dim drivers
        LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON d.phone = dcln.phone_number
            and dcln.phone_number <> '8'
            and dcln.phone_number not in ('89999999999')
        --gt-ru names
        LEFT JOIN  "gt-ru".gettaxi_ru_production.drivers gt_d ON concat('2000', cast(gt_d.id AS varchar))=cast(d.driver_gk AS varchar)
        --classes
        LEFT JOIN "emilia_gettdwh"."dwh_fact_drivers_classes_v" fdc ON fdc.driver_gk = d.driver_gk and fdc.is_current_allocation = 1 --and year(from_date_key) > 2018
        LEFT JOIN "emilia_gettdwh"."dwh_dim_class_types_v" ct ON ct.class_type_key = fdc.class_type_key and ct.lob_key IN (5,6) and ct.country_key = 2

    where dcln.source <> 'source' --filter the bug that occured bacauese of union of tables in google sheet

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)


, drivers_first_day as
    (
    select driver_gk, min(date_key) first_delivery_day --23 sec
    from emilia_gettdwh.dwh_fact_orders_v
    where country_key = 2
    and lob_key in (5,6)
    and order_status_key = 7
    group by 1
    )
(
select main.*, dfd.first_delivery_day
from main
left join drivers_first_day dfd on main.driver_gk = dfd.driver_gk);


select min(lead_date) from sheets."default".delivery_courier_leads_new;
-- v2


with main as (

    with leads as ( -- all leads
        -- there is no information about CITY - future step, to take from FO
            select distinct d.phone phone_number, d.driver_name as "name",
            '0' as courier_details, d.registration_date_key as "lead_date", '0' as request_id, 'Nan' as city,
            'agent' as "source"

            from emilia_gettdwh.dwh_dim_drivers_v d
            left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
                and fleet_gk in (200014202,200016265,200016266,200016267)

            where fleet_gk in (200014202,200016265,200016266,200016267)
        UNION

            select distinct phone_number, "name",
            courier_details, date(lead_date) as lead_date, request_id, city,
            "source"
            from sheets."default".delivery_courier_leads_new
            where "source" <> 'source' --filter the bug that occured bacauese of union of tables in google sheet
        )

    (SELECT distinct d.driver_gk,
    dcln.name AS lead_name,
    gt_d.name AS registration_name,
    dcln.phone_number,
    dcln.courier_details,
    date(dcln.lead_date) AS lead_date,
    dcln.source,
    dcln.request_id,
    dcln.city,
    d.registration_date_key,
    (CASE when d.ftp_date_key = date '1900-01-01' THEN null ELSE  d.ftp_date_key end) AS first_time_ride_date,
    dce.date_start AS exam_date,
    fl.vendor_name like '%courier%' is_couerier_vendor,
    gt_d.is_frozen = 1 is_frozen,
    --date(gt_d.frozen_since)>= date(dcln.lead_date) frozen_after_lead, --and (gt_d.frozen_comment not like '%Не работал более%')
    (case when substring("audited_changes", strpos("audited_changes", 'is_frozen:')+13, 1) = 't' then
    date(created_at) end) as unfreez_date,

    min(fo.date_key) first_delivery_date,
    min(case when fo.date_key > date(dcln.lead_date) then fo.date_key end) fdd_after_lead,
    -- orders per 2 month before
    count(case when (fo.date_key >= date_add('month',-2, date(dcln.lead_date))and fo.date_key < date(dcln.lead_date))
        then fo.order_gk end)<> 0 as were_deliv_2month_before_lead,
    min(fdc.from_date_key) - interval '1' day AS delivery_class_activation_date --first delivery class activation date

    FROM leads dcln -- "dcln" is ald short name for sheet delivery_courier_leads_new
        --exams
        LEFT JOIN sheets."default"."delivery_courier_exams" dce ON dce.phone = dcln.phone_number
            and dcln.phone_number <> '8'
        --dim drivers
        LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON d.phone = dcln.phone_number
            and dcln.phone_number <> '8'
            and dcln.phone_number not in ('89999999999')
            and country_key = 2
        --gt-ru names
        LEFT JOIN "gt-ru".gettaxi_ru_production.drivers gt_d ON concat('2000', cast(gt_d.id AS varchar))=cast(d.driver_gk AS varchar)
        --classes
        LEFT JOIN "emilia_gettdwh"."dwh_fact_drivers_classes_v" fdc ON fdc.driver_gk = d.driver_gk
            and fdc.is_current_allocation = 1
        LEFT JOIN "emilia_gettdwh"."dwh_dim_class_types_v" ct ON ct.class_type_key = fdc.class_type_key
            and ct.lob_key IN (5,6) and ct.country_key = 2
        -- fo
        left join emilia_gettdwh.dwh_fact_orders_v fo on d.driver_gk = fo.driver_gk
            and fo.lob_key in (5,6) and fo.order_status_key = 7 and fo.country_key = 2
        --vendor name
        left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
            --and fo.country_key = 2 and fo.lob_key in (5,6) and fo.order_status_key = 7
        -- gt_ru to get unfreez date
        left join "gt-ru".gettaxi_ru_production.audits gt
            on cast(concat('2000', cast(gt.auditable_id as varchar)) as integer) = d.driver_gk
            and "action" = 'update' and auditable_type = 'Driver' and date(gt.created_at) >= date('2020-07-10') --first day of lead

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15)
)
, drivers_first_day as
    (
    select fo.driver_gk,
    count(distinct fo.order_gk) as deliveries_num_after_lead--23 sec,

    from  sheets."default".delivery_courier_leads_new dcln
        LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d ON d.phone = dcln.phone_number
            and dcln.phone_number <> '8'
            and dcln.phone_number not in ('89999999999')
            and d.country_key = 2
        left join emilia_gettdwh.dwh_fact_orders_v fo on d.driver_gk = fo.driver_gk
            and fo.lob_key in (5,6) and fo.order_status_key = 7 and fo.country_key = 2
    where fo.country_key = 2
    and fo.lob_key in (5,6)
    and fo.order_status_key = 7
    and fo.date_key > date(dcln.lead_date)

    group by 1
    )
(
select main.*, deliveries_num_after_lead,
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
left join drivers_first_day dfd on main.driver_gk = dfd.driver_gk);



select * from  "emilia_gettdwh"."dwh_fact_drivers_classes_v" limit 2