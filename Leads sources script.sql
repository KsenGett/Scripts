drop table if exists analyst.delivery_leads
create table analyst.delivery_leads
as;

    select
    distinct
            d.driver_gk,
            d.phone phone_number,
            d.driver_name registration_name,

            (case when d.driver_gk = prog.driver_gk then
            (case when json_format(json_array_get(cast(source as json), 0)) like '%web%' then 'Plan-net'
                when json_format(json_array_get(cast(source as json), 0)) like '%workle%' then 'Workle' end) end) external_source,
            d.fleet_gk,

            (case when d.driver_gk = ref.driver_gk then 'Reff'
            when d.fleet_gk in (200014202,200016265,200016266,200016267,200016359,200016361) then 'Gorizont'
            when d.fleet_gk in (200017083,200017177,200017412,200017342,200017205,200017203, 200017524,200017523,200017517,200017430) then 'Scouts'
            when d.fleet_gk = 200017111 then 'D_Uspekha'
            else 'Fleet' end) source,

            d.registration_date_key,
            nullif(d.ftp_date_key, date'1900-1-1') first_ftr,
            max(case when ride_type = 'ReFTRD' then date(rftr.date_key) end) reFTR,
            max(case when d.driver_gk = prog.driver_gk then date(lead_date) end) as "external_source_lead_date"

        from emilia_gettdwh.dwh_dim_drivers_v d
        left join bp_ba.sm_ftr_reftr_drivers rftr on d.driver_gk = rftr.driver_gk
            -- to filter by fleet name selecting only couriers
            left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
            -- reff
            left join
            (
                select
                d.driver_gk,
                ( case when date(rftr.date_key) is null then d.ftp_date_key else date(rftr.date_key) end)
                 between cast("start" as date) and cast("end" as date)

                from emilia_gettdwh.dwh_dim_drivers_v d
                left join bp_ba.sm_ftr_reftr_drivers rftr on d.driver_gk = rftr.driver_gk
                left join "sheets"."default".ru_fleet_promo ref on cast(ref.fleet_gk as integer) = d.fleet_gk

                where 1=1
                -- select drivers who were led by reff
                and (
                    (( case when date(rftr.date_key) is null then d.ftp_date_key else date(rftr.date_key) end)
                                            between cast("start" as date) and cast("end" as date))
                    or (d.registration_date_key between cast("start" as date) and cast("end" as date))
                        )

                and d.country_key = 2

            ) ref on ref.driver_gk = d.driver_gk

            -- external sources: workle, website etc. It's taken from GoogleSheet filled by Valera
            left join
            (
                    select distinct d.phone phone_number, "name",
                    d.driver_name registration_name,
                    d.fleet_gk, driver_gk,
                    vendor_name,
                    --courier_details, request_id, leads.city,
                    array_agg("source") source,
                    max(date(lead_date)) as lead_date

                    -- google sheet
                    from sheets."default".delivery_courier_leads_new leads
                    -- get info about drivers by their phones
                    LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d
                        ON substring(d.phone, -10) = leads.phone_number
                            and d.phone not in ('89999999999', '8', '')
                            and country_key = 2
                    left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk

                    where "source" <> 'source' --filter the bug that occured because of union of tables in google sheet
                    and phone_2 <> 'phone_2'
                    and phone_number not in ('8', '', '9999999999', ' ', '3333333333', '2222222222') -- dummy phones
                    and phone_number is not null
                    and cast(lead_date as date) >= date'2020-07-01'
                    group by 1,2,3,4,5,6
            ) prog on prog.driver_gk = d.driver_gk


            where 1=1
            and d.phone is not null
            and d.driver_gk <> 2000683923 -- some old bug
            and fl.vendor_name like '%courier%'
            and d.country_key = 2
            --and d.registration_date_key >= date'2020-07-01'
            group by 1,2,3,4,5,6,7,8
;

grant all privileges on analyst.delivery_leads to role public with grant option
