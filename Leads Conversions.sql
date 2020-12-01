-- agents, ref
select
(case when
d.fleet_gk = cast(ref.fleet_gk AS bigint) then 'Reff'
when d.fleet_gk in (200014202,200016265,200016266,200016267,200016359,200016361)  then 'Agent' end) source,
count(distinct d.driver_gk) registration,
count(distinct case when ftp_date_key <> date'1900-01-01' then d.driver_gk  end) ftr,
count(distinct case when fo.orders + (case when md.deliveries is not null then md.deliveries end) >= 5
            then d.driver_gk end) five_rides

        from emilia_gettdwh.dwh_dim_drivers_v  d
            -- to filter by fleet name selecting only couriers
            left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
            -- reff - to learn original fleet
            left join "sheets"."default".ru_fleet_promo ref on fl.vendor_gk = cast(ref.fleet_gk as bigint)

        --activity
        LEFT JOIN --14 sec
  (
        SELECT
        distinct d.driver_gk,

        count(distinct CASE when ct.class_family <> 'Premium'
            and (order_status_key = 7 or driver_total_cost > 0) THEN order_gk end) orders

        FROM emilia_gettdwh.dwh_fact_orders_v fo
        left join "emilia_gettdwh"."dwh_dim_drivers_v" d on fo.driver_gk = d.driver_gk
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
            ON ct.class_type_key = fo.class_type_key
        LEFT JOIN emilia_gettdwh.dwh_dim_locations_v loc ON
        fo.origin_location_key = loc.location_key and loc.country_id = 2

        WHERE fo.lob_key IN (5,6)
        and date_key >= date'2020-11-1'
        and ordering_corporate_account_gk <> 20004730
        and fo.country_key = 2

        GROUP BY 1

    ) fo ON fo.driver_gk = d.driver_gk

-- Deliveries NF
 LEFT JOIN --2sec
    (
        SELECT distinct courier_gk,
        count(distinct delivery_gk) deliveries

        FROM model_delivery.dwh_fact_deliveries_v
        WHERE
        date(created_at) >= date'2020-11-1'
        and delivery_status_id in (4,7)
        GROUP BY 1

    ) md ON md.courier_gk  = d.driver_gk

            where 1=1
            and d.phone is not null
            and d.driver_gk <> 2000683923 -- some old bug
            and d.country_key = 2
            and d.registration_date_key >= date'2020-11-1'
group by 1


-- external sources

select
"source",
count(distinct phone_number) leads,
count(distinct d.driver_gk) activated,
count(distinct case when ftp_date_key <> date'1900-01-01' then d.driver_gk  end) ftr,
count(distinct case when fo.orders + (case when md.deliveries is not null then md.deliveries end) >= 5
then d.driver_gk end) five_rides

-- google sheet
from sheets."default".delivery_courier_leads_new leads
-- get info about drivers by their phones
                    LEFT JOIN "emilia_gettdwh"."dwh_dim_drivers_v" d
                        ON substring(d.phone, -10) = leads.phone_number
                            and d.phone not in ('89999999999', '8', '')
                            and country_key = 2
                    left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk

                            LEFT JOIN --14 sec
  (
        SELECT
        distinct d.driver_gk,

        count(distinct CASE when ct.class_family <> 'Premium'
            and (order_status_key = 7 or driver_total_cost > 0) THEN order_gk end) orders

        FROM emilia_gettdwh.dwh_fact_orders_v fo
        left join "emilia_gettdwh"."dwh_dim_drivers_v" d on fo.driver_gk = d.driver_gk
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
            ON ct.class_type_key = fo.class_type_key
        LEFT JOIN emilia_gettdwh.dwh_dim_locations_v loc ON
        fo.origin_location_key = loc.location_key and loc.country_id = 2

        WHERE fo.lob_key IN (5,6)
        and date_key >= date'2020-11-1'
        and ordering_corporate_account_gk <> 20004730
        and fo.country_key = 2

        GROUP BY 1

    ) fo ON fo.driver_gk = d.driver_gk

-- Deliveries NF
 LEFT JOIN --2sec
    (
        SELECT distinct courier_gk,
        count(distinct delivery_gk) deliveries

        FROM model_delivery.dwh_fact_deliveries_v
        WHERE
        date(created_at) >= date'2020-11-1'
        and delivery_status_id in (4,7)
        GROUP BY 1

    ) md ON md.courier_gk  = d.driver_gk


                    where "source" <> 'source' --filter the bug that occured because of union of tables in google sheet
                    and phone_2 <> 'phone_2'
                    and phone_number not in ('8', '', '9999999999', ' ', '3333333333', '2222222222') -- dummy phones
                    and phone_number is not null
                    and cast(lead_date as date) >= date'2020-11-1'

                    group by 1






--- FLEETS
select count(distinct fo.driver_gk) new,
count(distinct case when (fo.orders + (case when md.deliveries is not null then md.deliveries end)) >= 1
 then d.driver_gk  end) ftr,
count(distinct case when (fo.orders + (case when md.deliveries is not null then md.deliveries end)) >= 5
then d.driver_gk end) five_rides

from "emilia_gettdwh"."dwh_dim_drivers_v" d
-- get info about drivers by their phones
LEFT JOIN sheets."default".delivery_courier_leads_new leads
ON substring(d.phone, -10) = leads.phone_number
  and d.phone not in ('89999999999', '8', '')
 and d.country_key = 2

-- reff - to learn original fleet
left join "sheets"."default".ru_fleet_promo ref on d.fleet_gk = cast(ref.fleet_gk as bigint)

LEFT JOIN --14 sec
  (
        SELECT
        distinct d.driver_gk,

        count(distinct CASE when ct.class_family <> 'Premium'
            and (order_status_key = 7 or driver_total_cost > 0) THEN order_gk end) orders

        FROM emilia_gettdwh.dwh_fact_orders_v fo
        left join "emilia_gettdwh"."dwh_dim_drivers_v" d on fo.driver_gk = d.driver_gk
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
            ON ct.class_type_key = fo.class_type_key
        LEFT JOIN emilia_gettdwh.dwh_dim_locations_v loc ON
        fo.origin_location_key = loc.location_key and loc.country_id = 2

        WHERE fo.lob_key IN (5,6)
        and date_key >= date'2020-11-1'
        and ordering_corporate_account_gk <> 20004730
        and fo.country_key = 2
        and fo.fleet_gk not in (200014202,200016265,200016266,200016267,200016359,200016361)

        GROUP BY 1

    ) fo ON fo.driver_gk = d.driver_gk

-- Deliveries NF
 LEFT JOIN --2sec
    (
        SELECT distinct courier_gk,
        count(distinct delivery_gk) deliveries

        FROM model_delivery.dwh_fact_deliveries_v
        WHERE
        date(created_at) >= date'2020-11-1'
        and delivery_status_id in (4,7)
        GROUP BY 1

    ) md ON md.courier_gk  = d.driver_gk

left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk

where d.fleet_gk not in (200014202,200016265,200016266,200016267,200016359,200016361) -- not agent
and d.fleet_gk <> cast(ref.fleet_gk AS bigint) --not ref
and phone_number is null
and fl.vendor_name like '%courier%'
and registration_date_key >= date'2020-11-1'
;

-- courier fleets new registrations
select count(distinct driver_gk) activated

from emilia_gettdwh.dwh_dim_drivers_v d
left join emilia_gettdwh.dwh_dim_vendors_v fl
on d.fleet_gk = fl.vendor_gk

where registration_date_key >= date'2020-11-1'
and  fl.vendor_name like '%courier%'


;


---- overall
select
count(distinct d.driver_gk),
count(distinct case when fo2.date_key >= date'2020-11-1' then d.driver_gk end) fo,
count(distinct case when ftp_date_key >= date'2020-11-1' then d.driver_gk end) ftr,
count(distinct case when (fo2.orders + (case when md.deliveries is not null then md.deliveries end)) >= 5
then d.driver_gk end) five_rides


from emilia_gettdwh.dwh_dim_drivers_v d
left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk
    and d.country_key = 2

 LEFT JOIN --14 sec
  (
        SELECT
        distinct d.driver_gk,
        date_key,

        count(distinct CASE when ct.class_family <> 'Premium'
            and (order_status_key = 7 or driver_total_cost > 0) THEN order_gk end) orders

        FROM emilia_gettdwh.dwh_fact_orders_v fo
        left join "emilia_gettdwh"."dwh_dim_drivers_v" d on fo.driver_gk = d.driver_gk
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
            ON ct.class_type_key = fo.class_type_key
        LEFT JOIN emilia_gettdwh.dwh_dim_locations_v loc ON
        fo.origin_location_key = loc.location_key and loc.country_id = 2

        WHERE fo.lob_key IN (5,6)
        and date_key >= date'2020-11-1'
        and ordering_corporate_account_gk <> 20004730
        and fo.country_key = 2
        and fo.fleet_gk not in (200014202,200016265,200016266,200016267,200016359,200016361)

        GROUP BY 1,2

    ) fo2 ON fo2.driver_gk = d.driver_gk

-- Deliveries NF
 LEFT JOIN --2sec
    (
        SELECT distinct courier_gk,
        date(created_at) ddate_key,
        count(distinct delivery_gk) deliveries

        FROM model_delivery.dwh_fact_deliveries_v
        WHERE
        date(created_at) >= date'2020-11-1'
        and delivery_status_id in (4,7)
        GROUP BY 1,2

    ) md ON md.courier_gk  = d.driver_gk

where fl.vendor_name like '%courier%'
and registration_date_key >= date'2020-11-1'


select ftp_date_key, count(distinct driver_gk)
from emilia_gettdwh.dwh_dim_drivers_v
where fleet_gk in (200014781,	200011028,	200010116,	200013914,	200016340,	200014286,	200011836,	200014069,	200014178,	200015834,	200016832,	200014069,	200013452,	200014236,	200016254,	200013915)
and ftp_date_key >= date'2020-11-26'
group by 1