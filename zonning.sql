-- bigelford
with areas as (
        select * from emilia_gettdwh.dwh_dim_areas_v
        WHERE true
        and area_desc like '%%Moscow delivery district%%'
        --and area_desc = 'Moscow delivery district Izmaylovo 11.03'
        ),
deliveries as (
        select requested_schedule_time, order_gk,
             cast(substring(cast(company_gk as varchar),5) as bigint) company_id

        from model_delivery.dwh_fact_deliveries_v
        where date_key >= date '{date_start}'
        )
select o.order_gk,
       requested_schedule_time,
       origin_latitude, origin_longitude, dest_latitude,
       dest_longitude,
       origin_area.area_gk as origin_area_gk,
       dest_area.area_gk as dest_area_gk,
       delivery_order_level, company_id

from emilia_gettdwh.dwh_fact_orders_v o
            JOIN emilia_gettdwh.dwh_dim_class_types_v dct ON dct.class_type_key = o.class_type_key
            JOIN deliveries ON deliveries.order_gk = o.order_gk
            JOIN areas as origin_area on st_contains(ST_Polygon(origin_area.borders), ST_Point(o.origin_longitude, o.origin_latitude))
            JOIN areas as dest_area on st_contains(ST_Polygon(dest_area.borders), ST_Point(o.dest_longitude, o.dest_latitude))

            WHERE o.country_symbol = 'RU'
            AND o.date_key >= date '{date_start}'
            --AND o.date_key <= date'2021-01-04'
            AND lob_category='Deliveries'
            AND dct.lob_key=5
             --and o.order_status_key=7
             --and dct.lob_key=5 -- 5 for coorporate & ecommerce, 6 for private


select md.requested_schedule_time, md.order_gk,
       substring(cast(company_gk as varchar),5) md_company_id,
    d.requested_schedule_time, legacy_order_id, company_id

from desc model_delivery.dwh_fact_deliveries_v md
left join delivery.public.deliveries d
    on cast(d.journey_id as varchar) = substring(cast(md.journey_gk as varchar),5)
         and d.created_at >= date'2021-07-01'
        and env = 'RU'

where country_symbol='RU'
and date_key >= date'2021-07-01'

limit 20;

desc emilia_gettdwh.dwh_fact_orders_v