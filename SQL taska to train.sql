/* 1. Выбрать водителей доставки (driver gk, driver name),которые за
предыдущую неделю (с пн по вс) выполнили не менее 150 заказов.
 */
select orders.driver_gk order_id, drivers.driver_name, count(orders.order_gk) orders_number
from emilia_gettdwh.dwh_fact_orders_v orders join emilia_gettdwh.dwh_dim_drivers_v drivers
on orders.driver_gk = drivers.driver_gk
where orders.country_key = 2
and orders.order_status_key = 7
and orders.lob_key in (5,6)
and date_key between date'2020-06-22' and date'2020-06-28'
group by orders.driver_gk, drivers.driver_name
having count(orders.order_gk) >= 150;

/*2. Вывести средний чек В2С клиента в доставке за июнь в Москве. Округлить до целых.
 */
select round(avg(customer_total_cost)) average_B2C_bill_june
from emilia_gettdwh.dwh_fact_orders_v orders
where orders.order_status_key = 7
and orders.lob_key = 6
and orders.origin_location_key = 245
and date_key between date'2020-06-01' and date'2020-06-30';

/*3. Посчитать количество компаний, которые пользовались услугами доставки в
2019 году, по городам. Вывести города, количество выполненных заказов, отсортировав
по убыванию кол-ва выполненных заказов
 */
select  city.city_name city, count(distinct ordering_corporate_account_gk) companies_number,
count(order_gk) orders7_number
from emilia_gettdwh.dwh_fact_orders_v orders
join emilia_gettdwh.dwh_dim_locations_v city on orders.origin_location_key = city.location_key
where orders.lob_key = 5
and orders.country_key = 2
and orders.order_status_key = 7
and year(orders.date_key) = 2019
group by city.city_name
/*having city.city_name <> 'Russia - General' */
order by orders7_number desc

/*4. Как называется компания, которая заказала больше всего доставок за май 2020 года?
 */


with companies as (
    select company.corporate_account_name company_name, count(orders.order_gk) orders_number
    from emilia_gettdwh.dwh_fact_orders_v orders
    join emilia_gettdwh.dwh_dim_corporate_accounts_v company
    on orders.ordering_corporate_account_gk = company.corporate_account_gk
    where orders.lob_key = 5
    and orders.country_key = 2
    and orders.order_status_key = 7
    and month(orders.date_key) = 05 and year(orders.date_key) = 2020
    group by company.corporate_account_name)
select company_name
from companies
where orders_number = (select max(orders_number) from companies)


select company.corporate_account_name, count(order_gk) orders_number
from emilia_gettdwh.dwh_fact_orders_v orders
join emilia_gettdwh.dwh_dim_corporate_accounts_v company
on orders.ordering_corporate_account_gk = company.corporate_account_gk
where orders.lob_key = 5
and orders.country_key = 2
and orders.order_status_key = 7
and month(orders.date_key) = 05 and year(orders.date_key) = 2020
group by company.corporate_account_name
order by count(order_gk) desc

--1. Вывести список городов, в которых есть В2В доставка
select  distinct city.city_name
from emilia_gettdwh.dwh_fact_orders_v orders
join emilia_gettdwh.dwh_dim_locations_v city on orders.origin_location_key = city.location_key
where orders.lob_key = 5
and orders.country_key = 2
/*having city.city_name <> 'Russia - General' */



/* ИЗУЧИТЬ
Price seen to Order conversion in RU B2C classes
 */
with orders as (
select distinct session_id_fix,
ct.class_type_desc
from app_events ae
left join emilia_gettdwh.dwh_dim_class_types_v ct
on cast(ct.class_type_key as varchar) = concat('2000', cast(ae.class_id as varchar))
where event_name = 'server|order|created'
and ct.lob_key = 6
and occurred_date >= date '2020-06-01'
and env = 'RU'
),
prices as (
select distinct session_id_fix, ct.class_type_desc,
ae.occurred_date
from app_events ae
left join emilia_gettdwh.dwh_dim_class_types_v ct
on cast(ct.class_type_key as varchar) = concat('2000', cast(ae.class_id as varchar))
where ae.occurred_date >= date '2020-06-01'
and ae.env = 'RU'
and ct.lob_key = 6
and event_name = 'order_confirmation_screen|pricing_element|pricing_element_appears'
)
select --prices.class_type_desc,
prices.occurred_date,
count(distinct prices.session_id_fix) as prices,
count(distinct orders.session_id_fix) as orders
from prices left join orders on prices.session_id_fix = orders.session_id_fix
group by 1

/*AVG distance by Cities in RU Delivery
 */
select city_name,
avg(ride_distance_key) as avg_distance
from "emilia_gettdwh".dwh_fact_orders_v fo
left join "emilia_gettdwh"."dwh_dim_class_types_v" ct on ct.class_type_key = fo.class_type_key
left join "emilia_gettdwh"."dwh_dim_locations_v" l ON l.location_key = fo.origin_location_key
where fo.country_key = 2
and ct.lob_category = 'Deliveries'
and order_status_key = 7
and date_key >= date '2020-03-01'
and ride_distance_key > 0
group by 1

/*Unique number of clients, orders and avg client check for lockers
(класс доставки из постаматов
 */
select l.city_name,
count(distinct riding_user_gk) as unique_riders,
count(distinct order_gk) as rides,
sum(customer_total_cost_inc_vat)/count(distinct order_gk) as AVG_client_check,
avg(customer_total_cost_inc_vat)
from "emilia_gettdwh".dwh_fact_orders_v fo
left join "emilia_gettdwh"."dwh_dim_class_types_v" ct on ct.class_type_key = fo.class_type_key
left join "emilia_gettdwh"."dwh_dim_locations_v" l ON l.location_key = fo.origin_location_key
where fo.country_key = 2
and ct."class_type_desc" like '%lockers%'
and order_status_key = 7
and date_key >= date '2020-04-01'
and customer_total_cost_inc_vat > 0
group by 1

/*Courier Drivers - details for OPS team
 */
select distinct  dl.city_name,
d.name,
d.birthdate,
d.car_model,
d.license_no as car_number,
d.driver_license_id,
case when vendor_name like '%courier car%' then 'PHV'
when vendor_name like '%courier pedestrian%' then 'pedestrians'
when vendor_name like '%courier scooter%' then 'Scooters'
else 'taxi' end as supply_type
from "emilia_gettdwh"."dwh_fact_orders_v" fo
left join  "gt-ru".gettaxi_ru_production.drivers d on cast(fo.driver_gk as varchar) = concat('2000', cast(d.id as varchar))
left join "emilia_gettdwh"."dwh_dim_class_types_v" ct on ct.class_type_key = fo.class_type_key
left join "emilia_gettdwh"."dwh_dim_drivers_v" dd on dd.driver_gk = fo.driver_gk
LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v AS v ON v.vendor_gk = dd.fleet_gk
LEFT JOIN emilia_gettdwh.dwh_dim_locations_v AS dl ON dl.city_id = dd.primary_city_id
where fo.lob_key in (5,6)
-- and v.vendor_name not like '%pedestrian%'
and fo.date_key >= date'2020-02-20'
and fo.order_status_key = 7
and d.name not like '%customer care%'

/*Alternative w/o company gk hard-code:

 */
FROM emilia_gettdwh.dwh_fact_orders_v AS fo
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
ON ct.class_type_key = fo.class_type_key
WHERE ct.class_family NOT IN ('Premium') and fo.lob_key in (5,6)

/*IL/RU B2B & B2C Reliability Slides - by Month

 */
with t1 as(

with t2 as (
select  date_format(f.order_datetime, '%Y-%m') as y_m,
date_format(f.order_datetime, '%m') as months,
year(f.order_datetime)  as years,
 f.lob_key,
 count (distinct case when f.order_status_key = 7 then f.order_gk else null end) as completed_orders,
 count (distinct f.order_gk) as gross_orders,
 count(distinct f.series_original_order_gk) as net_orders
 FROM emilia_gettdwh.dwh_fact_orders_v as f
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = f.class_type_key
WHERE date_key>= date '2019-01-01' and date_key<= date '2020-02-29'
and f.country_key = 2 --for IL put 1
and f.lob_key in (5,6)
and ct.class_family not in ('Premium')
and ct.class_group not like 'Test'
group by 1,2,3,4)

select months,
max(case when lob_key=5 and years = 2019 then gross_orders else 0 end ) as gross_orders_b2b_2019,
max(case when lob_key=5 and years = 2019 then net_orders else 0 end ) as net_orders_b2b_2019,
max(case when lob_key=5 and years = 2019 then completed_orders else 0 end ) as completed_orders_b2b_2019,
max(case when lob_key=6 and years = 2019 then gross_orders else 0 end ) as gross_orders_b2c_2019,
max(case when lob_key=6 and years = 2019 then net_orders else 0 end ) as net_orders_b2c_2019,
max(case when lob_key=6 and years = 2019 then completed_orders else 0 end ) as completed_orders_b2c_2019,
max(case when lob_key=5 and years = 2020 then gross_orders else 0 end ) as gross_orders_b2b_2020,
max(case when lob_key=5 and years = 2020 then net_orders else 0 end ) as net_orders_b2b_2020,
max(case when lob_key=5 and years = 2020 then completed_orders else 0 end ) as completed_orders_b2b_2020,
max(case when lob_key=6 and years = 2020 then gross_orders else 0 end ) as gross_orders_b2c_2020,
max(case when lob_key=6 and years = 2020 then net_orders else 0 end ) as net_orders_b2c_2020,
max(case when lob_key=6 and years = 2020 then completed_orders else 0 end ) as completed_orders_b2c_2020
from t2
group by 1)


select months,
  completed_orders_b2c_2019*1.0/1000 as deliveries_B2C_2019,
  completed_orders_b2c_2020*1.0/1000 as deliveries_B2C_2020,
  (completed_orders_b2c_2019*1.00/net_orders_b2c_2019*1.00) AS B2C_NCR_2019,
  (completed_orders_b2c_2020*1.00/nullif(net_orders_b2c_2020*1.00,0)) AS B2C_NCR_2020,
  (completed_orders_b2c_2019*1.00 /gross_orders_b2c_2019 *1.00) AS B2C_GCR_2019,
  (completed_orders_b2c_2020*1.00 /nullif(gross_orders_b2c_2020 *1.00,0)) AS B2C_GCR_2020,
  completed_orders_b2b_2019*1.0/1000 as deliveries_B2B_2019,
  completed_orders_b2b_2020*1.0/1000 as deliveries_B2B_2020,
  (completed_orders_b2b_2019 *1.00/net_orders_b2b_2019*1.00 ) AS B2B_NCR_2019,
  (completed_orders_b2b_2020 *1.00/nullif(net_orders_b2b_2020*1.00,0 )) AS B2B_NCR_2020,
  (completed_orders_b2b_2019*1.00 /gross_orders_b2b_2019*1.00 ) AS B2B_GCR_2019,
  (completed_orders_b2b_2020*1.00 /nullif(gross_orders_b2b_2020*1.00,0) ) AS B2B_GCR_2020
from t1
order by 1



/*Rating and ATA by Month, LOB, supply_type:

P:P only at the moment, add an identifier for B2B/B2C.
 ATA: order from created to arrival, future orders added):*/

with t2 AS (
    with t1 as (
    SELECT  month(date_key) AS months,
     year(date_key) as years,
     fo.lob_key as LOB,
     (case when fo.country_key = 1 then
           (case when fo.fleet_gk in (1000334, 1000364) then v.vendor_name else 'taxi' end)
            when fo.country_key = 2 then
           (case when v.vendor_name like '%courier car%' THEN 'courier car'
            when v.vendor_name like '%courier pedestrian%' THEN 'pedestrians'
            when v.vendor_name like '%courier scooter%' THEN 'scooters'
            ELSE 'taxi'
      end) else ''  end) AS supply_type,
    count (distinct fo.order_gk) AS deliveries,
    --sum (fo.m_rating) AS rating_sum,
    --sum (CASE when fo.m_rating > 0 THEN 1 ELSE 0 end) AS rating_count,
    sum(date_diff('second', fo.order_datetime, fo.driver_arrived_datetime)/60.0) as ATA_sum,
    count( distinct case when fo.m_order_ata >0 then order_gk else null end ) as ATA_count
FROM emilia_gettdwh.dwh_fact_orders_v AS fo
LEFT JOIN emilia_gettdwh.dwh_dim_drivers_v AS d ON d.driver_gk = fo.driver_gk
LEFT JOIN emilia_gettdwh.dwh_dim_vendors_v AS v ON v.vendor_gk = d.fleet_gk
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v ct ON ct.Class_Type_Key = fo.class_type_key
WHERE fo.country_key = 2 --for IL put 1
    and fo.lob_key in (5,6)
    and fo.date_key>= date '2019-01-01' and date_key<= date '2020-02-29'
    and fo.order_status_key = 7
    and ct.class_group not like 'Test'
    and ct.class_family not in ('Premium')
    and fo.m_order_ata > 0
GROUP BY 1,2,3,4)

 select LOB,
 supply_type,
 years,
 months,
 --rating_sum*1.00/rating_count*1.0 as rating,
 ATA_sum*1.00/ATA_count*1.0 as ATA
 from t1
 where supply_type not in ('taxi'))

select supply_type,
months,
--max(case when years = 2019 then rating else 0 end) as rating_2019,
--max(case when years = 2020 then rating else 0 end) as rating_2020,
max(case when years = 2019 and LOB = 5 then ATA else null end) as B2B_ATA_2019,
max(case when years = 2020 and LOB = 5 then ATA else null  end) as B2B_ATA_2020,
max(case when years = 2019 and LOB = 6 then ATA else null  end) as C2C_ATA_2019,
max(case when years = 2020 and LOB = 6 then ATA else null  end) as C2C_ATA_2020
from t2
group by 1,2
order by 1,2



/*Active couriers and Taxi Share (for now its p:p only but we should reconsider it)
Drivers excl. taxi, at least 1 paid delivery.
Please add an identifier for the type of supply in case needed*/

with t2 as (
with t1 as (

select fo.country_key,
year(date_key) as years,
month(date_key) as months,
    (case when fo.country_key = 1 then
    (case when d.fleet_gk in (1000334, 1000364) then v.vendor_name else 'taxi' end)
    when  fo.country_key = 2 then
    (case when v.vendor_name like '%courier car%' THEN 'courier car'
            when v.vendor_name like '%courier pedestrian%' THEN 'pedestrian'
            when v.vendor_name like '%courier scooter%' THEN 'scooters'
            when v.vendor_name is null THEN NULL
            ELSE 'taxi'
       end) else ''  end) AS supply_type,
fo.lob_key,
count(distinct fo.driver_gk) as couriers,
count(distinct order_gk) as deliveries
from emilia_gettdwh.dwh_fact_orders_v fo
left join emilia_gettdwh.dwh_dim_drivers_v d on d.driver_gk = fo.driver_gk
left join emilia_gettdwh.dwh_dim_vendors_v v ON v.vendor_gk = d.fleet_gk
LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key
where fo.country_key = 2 -- for IL put 1
and fo.lob_key in (5,6)
and ct.class_family not in ('Premium')
and ct.class_group not like 'Test'
and date_key>= date '2019-01-01' and date_key<= date '2020-02-29'
and (order_status_key = 7 or order_status_key = 4 and driver_total_cost > 0)
group by 1,2,3,4,5)

select years,
months,
sum(case when supply_type <> 'taxi' then couriers else 0 end) as active_couriers,
sum( case when supply_type = 'taxi' then deliveries else 0 end)*1.00/sum(deliveries)*1.0 as taxi_share,
sum(deliveries) all_deliveries,
sum( case when supply_type = 'taxi' then deliveries else 0 end) as taxi_deliveries
from t1
group by 1,2
order by 1,2)

select months,
max(case when years = 2019 then active_couriers else 0 end) as active_couriers_2019,
max(case when years = 2020 then active_couriers else 0 end) as active_couriers_2020,
max(case when years = 2019 then taxi_share else 0 end) as taxi_share_2019,
max(case when years = 2020 then taxi_share else 0 end) as taxi_share_2020
from t2
group by 1
order by 1


select * from sheets."default".steal_cases_delivery_04to0720
-- stealing cases by companies


select company.corporate_account_name, count(order_gk) orders_number
    from emilia_gettdwh.dwh_fact_orders_v fo
    join emilia_gettdwh.dwh_dim_locations_v loc on
    fo.origin_location_key = loc.location_key and fo.country_key = 2
    and fo.date_key between '2020-04-06' and current_date
    join emilia_gettdwh.dwh_dim_corporate_accounts_v company
    on fo.ordering_corporate_account_gk = company.corporate_account_gk
        where fo.lob_key = 5
        and fo.origin_location_key = 245
    --and orders.order_status_key in 7
    join sheets."default".steal_cases_delivery_04to0720 steal
    on steal.order_id = fo.order_id
    group by company.corporate_account_name


-- stolen by companies
with
all_orders as (
select count(fo.sourceid) all_orders_number, ordering_corporate_account_gk
    from emilia_gettdwh.dwh_fact_orders_v fo
    where fo.date_key between date'2020-04-06' and current_date
    and fo.origin_location_key = 245
    and fo.lob_key = 5
    and fo.order_status_key = 7
    group by fo.ordering_corporate_account_gk)

, stolen as
(select ordering_corporate_account_gk, company.corporate_account_name company_name,
    count(steal.order_id) stolen_orders, sum(try_cast(parcel_value_Rub as integer)) cost_lost
    from emilia_gettdwh.dwh_fact_orders_v fo
    --join emilia_gettdwh.dwh_dim_locations_v loc on
    --fo.origin_location_key = loc.location_key and fo.country_key = 2

    -- company name
    join emilia_gettdwh.dwh_dim_corporate_accounts_v company
    on fo.ordering_corporate_account_gk = company.corporate_account_gk
    and fo.date_key between date'2020-04-06' and current_date
    -- steal case
    left join sheets."default".steal_cases_delivery_04to0720 steal
    on cast(steal.order_id as varchar ) = cast(fo.sourceid as varchar)

    where cast(steal.order_id as varchar ) = cast(fo.sourceid as varchar)
    and fo.origin_location_key = 245
    group by company.corporate_account_name, ordering_corporate_account_gk)

select
(case when stolen.company_name = internal.name_regular then internal.name_internal
else stolen.company_name end) company,
stolen.stolen_orders, all_orders.all_orders_number,
stolen.stolen_orders*1.000/all_orders.all_orders_number*100 percent_stolen,
stolen.cost_lost
from all_orders
--stolen
join stolen on all_orders.ordering_corporate_account_gk = stolen.ordering_corporate_account_gk
--internal name
left join sheets."default".delivery_corp_accounts_20191203 internal on
internal.name_regular = stolen.company_name
order by stolen_orders desc, all_orders_number desc





--stealing cases by fleets
select fl.vendor_name, count(order_gk) stolen_orders_number
from emilia_gettdwh.dwh_fact_orders_v fo
--fleet
join emilia_gettdwh.dwh_dim_vendors_v fl on fo.fleet_gk = fl.vendor_gk and fo.country_key = 2
and fo.origin_location_key = 245
--steal case
left join sheets."default".steal_cases_delivery_04to0720 steal
on cast(steal.order_id as varchar ) = cast(fo.sourceid as varchar)
where fo.lob_key = 5
and fo.origin_location_key = 245
and cast(steal.order_id as varchar ) = cast(fo.sourceid as varchar)
group by fl.vendor_name
order by stolen_orders_number desc


-- + name of drivers
select dr.driver_name, dr.phone,  count(order_gk) stolen_orders_number, fl.vendor_name
from emilia_gettdwh.dwh_fact_orders_v fo
--fleet
join emilia_gettdwh.dwh_dim_vendors_v fl on fo.fleet_gk = fl.vendor_gk and fo.country_key = 2
and fo.origin_location_key = 245
--steal case
left join sheets."default".steal_cases_delivery_04to0720 steal
on cast(steal.order_id as varchar ) = cast(fo.sourceid as varchar)
--drivers
join emilia_gettdwh.dwh_dim_drivers_v dr on fo.driver_gk = dr.driver_gk
where fo.lob_key = 5
and fo.origin_location_key = 245
and cast(steal.order_id as varchar ) = cast(fo.sourceid as varchar)
group by dr.driver_name, dr.phone, fl.vendor_name
order by dr.phone, driver_name, stolen_orders_number desc


-- costs lost
select sum(try_cast(parcel_value_Rub as integer ))
from sheets."default".steal_cases_delivery_04to0720 desc


--company and fleet
select company.corporate_account_name company_name,
fl.vendor_name
, count(steal.order_id) steal_number, sum(try_cast(parcel_value_Rub as integer))

    --fact order
    from emilia_gettdwh.dwh_fact_orders_v fo
    --company
    join emilia_gettdwh.dwh_dim_corporate_accounts_v company
    on fo.ordering_corporate_account_gk = company.corporate_account_gk
    and fo.date_key between date'2020-04-06' and current_date
    --company internal name
    left join sheets."default".delivery_corp_accounts_20191203 internal on
    cast(internal.company_gk as integer) = company.corporate_account_gk
    --steal cases
    left join sheets."default".steal_cases_delivery_04to0720 steal
    on cast(steal.order_id as varchar ) = cast(fo.sourceid as varchar)
    --fleet
    join emilia_gettdwh.dwh_dim_vendors_v fl on fo.fleet_gk = fl.vendor_gk
    and fo.country_key = 2
    and fo.origin_location_key = 245
    where cast(steal.order_id as varchar ) = cast(fo.sourceid as varchar)
group by company.corporate_account_name, fl.vendor_name
order by company.corporate_account_name, steal_number desc


--steal delivery big sheet
with
all_orders as
(select ordering_corporate_account_gk,
    count(fo.sourceid) all_orders_number
    from emilia_gettdwh.dwh_fact_orders_v fo
    where fo.date_key between date'2020-04-06' and date'2020-07-10'
    and fo.origin_location_key = 245
    and fo.lob_key = 5
    and fo.order_status_key = 7
    group by ordering_corporate_account_gk)

, steal_case as
(select company.corporate_account_gk corporate_account_gk,
    company.corporate_account_name company_name,
    fl.vendor_name fleet,
    dr.driver_name, dr.phone driver_phone_number,
    sourceid order_id,
    fo.date_key order_date, month(fo.date_key) order_month,
    try_cast(parcel_value_Rub as integer) order_cost,
    fo.Is_Future_Order_Key Is_Future_Order

    --fact order
    from emilia_gettdwh.dwh_fact_orders_v fo
    --company
    join emilia_gettdwh.dwh_dim_corporate_accounts_v company
    on fo.ordering_corporate_account_gk = company.corporate_account_gk
    and fo.date_key between date'2020-04-06' and current_date
    --steal cases
    left join sheets."default".steal_cases_delivery_04to0720 steal
    on cast(steal.order_id as varchar ) = cast(fo.sourceid as varchar)
    --fleet
    left join emilia_gettdwh.dwh_dim_vendors_v fl on fo.fleet_gk = fl.vendor_gk
    --drivers
    left join emilia_gettdwh.dwh_dim_drivers_v dr on fo.driver_gk = dr.driver_gk
    and fo.country_key = 2
    and fo.origin_location_key = 245
    where cast(steal.order_id as varchar ) = cast(fo.sourceid as varchar))

select
(case when steal_case.corporate_account_gk = cast(internal.company_gk as integer) then internal.name_internal
else steal_case.company_name end) company,
steal_case.fleet,
(CASE when steal_case.fleet like '%courier car%' THEN 'PHV'
            when steal_case.fleet like '%courier pedestrian%' THEN 'pedestrian'
            when steal_case.fleet like '%courier scooter%' THEN 'scooter'
            when steal_case.fleet is null THEN NULL
            ELSE 'taxi'
       end) AS supply_type,
steal_case.driver_name, steal_case.driver_phone_number,
steal_case.order_id, steal_case.order_date, steal_case.Is_Future_Order,
steal_case.order_cost,
all_orders.all_orders_number
from all_orders
join steal_case on all_orders.ordering_corporate_account_gk = steal_case.corporate_account_gk
--company internal name
left join sheets."default".delivery_corp_accounts_20191203 internal on
cast(internal.company_gk as integer) = steal_case.corporate_account_gk
order by company,steal_case.order_cost




with
dim_week_corporders as (
select ordering_corporate_account_gk,
    week(date_key) week, count(sourceid) orders_per_week
    from emilia_gettdwh.dwh_fact_orders_v
    where date_key between date'2020-04-06' and current_date
    and origin_location_key = 245
    and lob_key = 5
    and order_status_key = 7
    group by ordering_corporate_account_gk, week(date_key))
, dim_month_corporders as (
select ordering_corporate_account_gk,
    month(date_key) month_num, count(sourceid) orders_per_month
    from emilia_gettdwh.dwh_fact_orders_v
    where date_key between date'2020-04-06' and current_date
    and origin_location_key = 245
    and lob_key = 5
    and order_status_key = 7
    group by ordering_corporate_account_gk, month(date_key))

, dim_quoter_corporders as (
select ordering_corporate_account_gk,
    quarter(date_key) Q, count(sourceid) orders_per_q
    from emilia_gettdwh.dwh_fact_orders_v
    where date_key between date'2020-04-06' and current_date
    and origin_location_key = 245
    and lob_key = 5
    and order_status_key = 7
    group by ordering_corporate_account_gk, quarter(date_key)
)
, fact_corporders_steal as (
select ordering_corporate_account_gk,
    date_key, week(date_key) w, month(date_key) m, quarter(date_key) Q
    from emilia_gettdwh.dwh_fact_orders_v
    where date_key between date'2020-04-06' and current_date
    and origin_location_key = 245
    and lob_key = 5
    and order_status_key = 7)
select distinct fo.ordering_corporate_account_gk, fo.date_key,
w.week, w.orders_per_week, m.month_num, m.orders_per_month,
quar.Q, quar.orders_per_q
from fact_corporders_steal fo
join dim_week_corporders w on fo.w =w.week
join dim_month_corporders m on fo.m = m.month_num
join dim_quoter_corporders quar on fo.Q = quar.Q



-- big sheet V2
with
all_orders as
(select ordering_corporate_account_gk,
    count(fo.sourceid) all_orders_number
    from emilia_gettdwh.dwh_fact_orders_v fo
    where fo.date_key between date'2020-04-06' and current_date
    and fo.origin_location_key = 245
    and fo.lob_key = 5
    and fo.order_status_key = 7
    group by ordering_corporate_account_gk)

, steal_case as
(select company.corporate_account_gk corporate_account_gk,
    company.corporate_account_name company_name,
    fl.vendor_name fleet,
    dr.driver_name, dr.phone driver_phone_number,
    sourceid order_id,
    fo.date_key order_date, month(fo.date_key) order_month,
    try_cast(parcel_value_Rub as integer) order_cost,
    fo.Is_Future_Order_Key Is_Future_Order

    --fact order
    from emilia_gettdwh.dwh_fact_orders_v fo
    --company
    join emilia_gettdwh.dwh_dim_corporate_accounts_v company
    on fo.ordering_corporate_account_gk = company.corporate_account_gk
    and fo.date_key between date'2020-04-06' and current_date
    --steal cases
    left join sheets."default".steal_cases_delivery_04to0720 steal
    on cast(steal.order_id as varchar ) = cast(fo.sourceid as varchar)
    --fleet
    join emilia_gettdwh.dwh_dim_vendors_v fl on fo.fleet_gk = fl.vendor_gk
    --drivers
    join emilia_gettdwh.dwh_dim_drivers_v dr on fo.driver_gk = dr.driver_gk
    and fo.country_key = 2
    and fo.origin_location_key = 245
    where cast(steal.order_id as varchar ) = cast(fo.sourceid as varchar))

, dim_date as
(   with
    dim_week_orders as (
    select week(date_key) week, count(sourceid) orders_per_week
        from emilia_gettdwh.dwh_fact_orders_v
        where date_key between date'2020-04-06' and current_date
        and origin_location_key = 245
        and lob_key = 5
        and order_status_key = 7
        group by week(date_key))
    , dim_month_orders as (
    select month(date_key) month_num, count(sourceid) orders_per_month
        from emilia_gettdwh.dwh_fact_orders_v
        where date_key between date'2020-04-06' and current_date
        and origin_location_key = 245
        and lob_key = 5
        and order_status_key = 7
        group by month(date_key))
    , fact_orders_steal as (
    select date_key, week(date_key) w, month(date_key) m
        from emilia_gettdwh.dwh_fact_orders_v
        where date_key between date'2020-04-06' and current_date
        and origin_location_key = 245
        and lob_key = 5
        and order_status_key = 7)
    select distinct fo.date_key, w.week, w.orders_per_week, m.month_num, m.orders_per_month
    from fact_orders_steal fo
    join dim_week_orders w on fo.w =w.week
    join dim_month_orders m on fo.m = m.month_num)

select
(case when steal_case.corporate_account_gk = cast(internal.company_gk as integer) then internal.name_internal
else steal_case.company_name end) company,
steal_case.fleet,
(CASE when steal_case.fleet like '%courier car%' THEN 'PHV'
            when steal_case.fleet like '%courier pedestrian%' THEN 'pedestrian'
            when steal_case.fleet like '%courier scooter%' THEN 'scooter'
            when steal_case.fleet is null THEN NULL
            ELSE 'taxi'
       end) AS supply_type,
steal_case.driver_name, steal_case.driver_phone_number,
steal_case.order_id,
steal_case.order_date, dim_date.week, dim_date.orders_per_week,
dim_date.month_num, dim_date.orders_per_month,
steal_case.Is_Future_Order,
steal_case.order_cost,
all_orders.all_orders_number
from all_orders
join steal_case on all_orders.ordering_corporate_account_gk = steal_case.corporate_account_gk
--company internal name
left join sheets."default".delivery_corp_accounts_20191203 internal on
cast(internal.company_gk as integer) = steal_case.corporate_account_gk
--date+orders_number
join dim_date on steal_case.order_date = dim_date.date_key
order by company,steal_case.order_cost


select company.corporate_account_name, week(fo.date_key),
count(fo.sourceid) all_orders_number
from emilia_gettdwh.dwh_fact_orders_v fo
left join sheets."default".steal_cases_delivery_04to0720 steal
on cast(steal.order_id as varchar ) = cast(fo.sourceid as varchar)
   --company
    join emilia_gettdwh.dwh_dim_corporate_accounts_v company
    on fo.ordering_corporate_account_gk = company.corporate_account_gk
    and fo.date_key between date'2020-04-06' and current_date

where fo.date_key between date'2020-04-06' and current_date
and fo.origin_location_key = 245
and fo.lob_key = 5
group by company.corporate_account_name, week(fo.date_key)
having cast(steal.order_id as varchar ) = cast(fo.sourceid as varchar)
order by company.corporate_account_name, week(fo.date_key)