select
dr.driver_gk, loc.city_name,
p.class_name_desc class_should_be,
--ct.class_type_key class_on

-- classes that should be cannected to all drivers
from sheets."default".delivery_classes_all p
join emilia_gettdwh.dwh_dim_class_types_v clt on cast(p.class_number as integer) = clt.internal_class_id
-- all active couriers
cross join (
         select driver_gk, primary_city_id
         from emilia_gettdwh.dwh_dim_drivers_v
         where is_courier = 1
           and ltp_date_key >= current_date - interval '30' day
           and country_key = 2
     ) dr

-- drivers
left join emilia_gettdwh.dwh_fact_drivers_classes_v ct
    on cast(clt.class_type_key as integer) = ct.class_type_key
    and dr.driver_gk=ct.driver_gk
left join emilia_gettdwh.dwh_dim_locations_v loc on dr.primary_city_id = loc.location_key

where ct.driver_gk is null
and dr.driver_gk <> 200013

order by class_should_be;

select * from sheets.default.ab_test_support_ksen;


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







