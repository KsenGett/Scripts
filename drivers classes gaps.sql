with m as
     (
         select distinct dr.driver_gk,
                dr.city_name  driver_city,
                p.class_name_desc class_should_be,
                p.class_number class_type_key,
                p.supply_connect supply_should_connect,
                dr.courier_type,
--                dr.courier_type_cl,
                dr.cycle

--ct.class_type_key class_on

-- classes that should be cannected to all drivers
         from sheets."default".delivery_classes_all p

--         join emilia_gettdwh.dwh_dim_class_types_v clt
--             on cast(p.class_number as integer) = clt.internal_class_id
--             and clt.country_key = 2
-- all active couriers
        cross join (
             select distinct driver_gk,
                             location_key,
                             city_name,
                             (case when car_model like '%велосипед%' then 'cycle' else courier_type end) courier_type,
                             (case when courier_type in ('scooter','pedestrian') or car_model like '%велосипед%'  then 'rest' else courier_type end) courier_type_cl,
                             (case when car_model like '%велосипед%' then 'cycle' end) cycle


             from emilia_gettdwh.dwh_dim_drivers_v d
                      left join emilia_gettdwh.dwh_dim_locations_v loc on d.primary_city_id = loc.city_id
                 and loc.country_key = 2
             where is_courier = 1
               and ltp_date_key >= current_date - interval '30' day
               and d.country_key = 2
               and is_test = 0
               --and courier_type = 'car'
                and is_frozen = 0
            and is_test = 0
         ) dr

-- drivers
        left join emilia_gettdwh.dwh_fact_drivers_classes_v ct
        on cast(p.class_number as integer) = ct.class_type_key
        and dr.driver_gk = ct.driver_gk


where ct.driver_gk is null
and dr.driver_gk <> 200013
and dr.location_key = cast(p.location_key as integer)
and (case when p.supply_connect in ('car','rest') then dr.courier_type_cl = p.supply_connect
    when p.supply_connect = 'cycle' then dr.cycle = p.supply_connect end)

order by driver_city, class_name_desc
)
(select
driver_city, class_should_be,supply_should_connect,class_type_key,
count(driver_gk)
from m

-- where class_type_key = 2000703
group by 1,2,3,4
order by count(distinct driver_gk)
);


-- TO CHECK
--except
(
    select
        fl.vendor_name not like '%courier%',
           vendor_name,
           is_courier,
        courier_type, d.driver_gk
    --count(distinct c.driver_gk) drivers

from emilia_gettdwh.dwh_dim_drivers_classes_v c
join emilia_gettdwh.dwh_dim_drivers_v d on c.driver_gk =d.driver_gk
                                                    and d.country_key=2
                                                    --and is_courier=1
                                                    --and courier_type='car'
                                                    --and is_frozen = 0
                                                    --and primary_city_id=246
                                                    and ltp_date_key >= current_date - interval '30' day
left join emilia_gettdwh.dwh_dim_vendors_v fl on d.fleet_gk = fl.vendor_gk and fl.country_key = 2
join emilia_gettdwh.dwh_dim_class_types_v cl on c.class_type_key = cl.class_type_key and lob_key in (5,6)

where c.country_key = 2
and is_courier = 0
and vendor_name = 'ООО "Риалтакси"/привлечение'
--and c.class_type_key = 20001483 -- moscow delivery b2b
  --group by 1,2,3,4
    )
;




select distinct courier_type, car_model
from emilia_gettdwh.dwh_dim_drivers_v where country_key = 2
                                                                     and ltp_date_key >= current_date - interval '30' day
and is_courier=1;


-- other logic, to save
with main_class as (
    select class_type_desc,
           class_type_key,
           active_drivers,
           rank() over (order by active_drivers desc) rank
    from (
             select class_type_desc,
                    ct.class_type_key,
                    count(distinct cl.driver_gk) drivers,
                    count(distinct case
                                       when dd.ltp_date_key >= current_date - interval '30' day
                                           then dd.driver_gk end) active_drivers

             from emilia_gettdwh.dwh_dim_drivers_v dd
                      join emilia_gettdwh.dwh_fact_drivers_classes_v cl on dd.driver_gk = cl.driver_gk
                      left join emilia_gettdwh.dwh_dim_class_types_v ct on ct.class_type_key = cl.class_type_key
---join sheets."default".classes_delivery ...

             where is_courier = 1
               and dd.country_key = 2

             group by 1, 2
         )
)
select
*
;







