with deliv as
(
        select
        fo.driver_gk, count(distinct fo.date_key) work_days,
        sum(orders + (case when deliveries is not null then deliveries else 0 end)) deliv,
        sum(orders + (case when journeys is not null then journeys else 0 end)) jorn

        from
          (
                --select count(distinct driver_gk) from (
                select
                distinct
                fo.driver_gk,
                date_key,

                -- orders only on OF
                count(distinct case when ct.class_family <> 'Premium'
                 and ordering_corporate_account_gk <> 20004730 then order_gk end) orders

                from emilia_gettdwh.dwh_fact_orders_v fo
                left join emilia_gettdwh.dwh_dim_class_types_v AS ct
                    ON ct.class_type_key = fo.class_type_key
                left join emilia_gettdwh.dwh_dim_locations_v loc on
                    fo.origin_location_key = loc.location_key and loc.country_id = 2
                left join emilia_gettdwh.dwh_dim_drivers_v d on fo.driver_gk = d.driver_gk

                where fo.lob_key in (5,6)
                and date_key between ftp_date_key and ftp_date_key + interval '14' day
                and order_status_key = 7
                and fo.country_key = 2
                and date_key >= date'2020-10-1'
                and fo.driver_gk <> 200013
                group by 1,2
                --)

            ) fo

        -- Deliveries NF
        join --2sec
            (
                select
                distinct courier_gk,
                date(scheduled_at) date_key,
                count(distinct delivery_gk) deliveries,
                count(distinct journey_gk) journeys

                from model_delivery.dwh_fact_deliveries_v fd
                left join emilia_gettdwh.dwh_dim_drivers_v d on fd.courier_gk = d.driver_gk

                where 1=1
                and date(scheduled_at) between ftp_date_key and ftp_date_key + interval '14' day
                and date(scheduled_at) >= date'2020-10-1'
                and delivery_status_id = 4
                and fd.country_symbol = 'RU'
                and courier_gk <> 200013

                group by 1,2

            ) md on md.courier_gk  = fo.driver_gk and md.date_key = fo.date_key

        group by 1
)
(
SELECT d."fleet_gk",
            p.programme,
         taxi_station_id,
         vendor_name AS fleet_name,
         city AS fleet_city,
         Documents_signed,
         Target,
         ftp_date_key,
         d.driver_gk,
         driver_name,
         "registration_date_key",
         "ltp_date_key",
         work_days,
         deliv deliv_14days,
         jorn journeys_14days

FROM "emilia_gettdwh"."dwh_dim_drivers_v" d
    inner JOIN sheets."default".ru_fleet_promo p ON d."fleet_gk" = cast(p.fleet_gk AS integer)
    and ftp_date_key between cast("start" as date) and cast("end" as date)
left join deliv on d.driver_gk = deliv.driver_gk

where ftp_date_key between cast("start" as date) and cast("end" as date)
);
