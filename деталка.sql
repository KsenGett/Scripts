 SELECT
        'Доставка',
        -- to select by company
        d.company_gk,
        j.journey_id,
        d.order_id_representation,
        d.display_identifier,

        j.date_key,
        date_format(d.requested_schedule_time, '%T') scheduled_at,
        date_format(j.started_at, '%T') started_at,
        date_format(d.arrived_to_drop_off_at, '%T') arrived_to_drop_off_at,
        date_format(d.ended_at, '%T') complited,

        delivery_status_desc,
        est_distance_m,
        est_duration_min,

        d.vendor_name,
        pickup_address,
        drop_off_address,

        -- подача
        const_cost*1.2 const_vat,

        dist.cost * 1.2 dist_cost_vat,
        dur.cost * 1.2 dur_cost_vat,
        wt.waiting_cost * 1.2 wt_cost_vat,

        -- если дорогая отмена и корректировки не было
        case when waiting_cost >=300 and corct.journey_id is null then 0 end wt_cost_vat_corrected,

        return_cost*1.2 return_vat,
        case when journey_status_id = 3 and j.total_customer_amount_exc_vat >0
            then j.total_customer_amount_exc_vat*1.2 end cancellation_cost_vat,

        case when corct.journey_id is not null then 'Да' else 'Нет' end correction,
        j.total_customer_amount_exc_vat*1.2 total_cost_vat,

        -- to check
        case when j.total_customer_amount_exc_vat is null then 'null'
            when j.total_customer_amount_exc_vat = 0 then 'zero'
                when j.total_customer_amount_exc_vat < 0 then 'negative'
            end check_lable,



FROM model_delivery.dwh_fact_journeys_v AS j


left join (select journey_id, component_amount waiting_cost, contract_id
           from  model_delivery.dwh_fact_company_monetisation_v
            where component_name = 'waiting'
             and component_amount > 0 and env = 'RU'
                and date(created_at) between date'2021-07-01' and date'2021-07-31'
            ) wt on j.journey_id = wt.journey_id

left join
    (select journey_id, component_amount cost,
                contract_id,
                component_amount est_distance_m

    from model_delivery.dwh_fact_company_monetisation_v
      where component_name = 'distance'
      and env = 'RU'
    and date(created_at) between date'2021-07-01' and date'2021-07-31'
      --and component_amount > 0
    ) dist on j.journey_id = dist.journey_id

 left join
    (select journey_id, component_amount cost,
             contract_id,component_value est_duration_min

    from model_delivery.dwh_fact_company_monetisation_v
    where component_name = 'duration'
    and env = 'RU'
    and date(created_at) between date'2021-07-01' and date'2021-07-31'
      --and component_amount > 0
) dur on j.journey_id = dur.journey_id

 left join
    (select journey_id, component_amount const_cost,
             contract_id

    from model_delivery.dwh_fact_company_monetisation_v
    where component_name = 'const'
    and env = 'RU'
    and date(created_at) between date'2021-07-01' and date'2021-07-31'
      --and component_amount > 0
) cnst on j.journey_id = cnst.journey_id


 left join
    (select journey_id, component_amount return_cost,
             contract_id

    from  model_delivery.dwh_fact_company_monetisation_v
    where component_name = 'returns'
    and env = 'RU'
    and date(created_at) between date'2021-07-01' and date'2021-07-31'
      --and component_amount > 0
) ret on j.journey_id = ret.journey_id

left join
(select distinct j.journey_id, manual
    from "delivery-pricing"."public".transactions t
    join model_delivery.dwh_fact_journeys_v j on t.journey_id = j.journey_id
     and country_symbol = 'RU'
    where date_key between date '2021-07-01' and date '2021-07-31'
    and t.env = 'RU'
     and manual = true
    and side = 'customer'
) corct on j.journey_id = corct.journey_id

left join (
                select journey_id, delivery_id, sum(amount) "sum"
                from "delivery-pricing"."public".transactions
                where env='RU' and date(created_at) >= date'2021-07-01'
                and side = 'customer'

                --and journey_id = 4227829
                group by 1,2
                order by journey_id

     ) tr on (tr.journey_id = d.jpurney_id and (tr.journey_id = d.jpurney_id)) or (tr.journey_id = d.jpurney_id )

LEFT JOIN model_delivery.dwh_fact_deliveries_v AS d ON d.journey_gk = j.journey_gk
left join "model_delivery"."dwh_dim_delivery_statuses_v" ds
                            on ds."delivery_status_id" = d."delivery_status_id"

WHERE 1 = 1
aND j.country_symbol = 'RU'
AND d.company_gk NOT IN (20001999) -- Test company
AND j.date_key between date'2021-07-01' and date'2021-07-31'

and company_gk = 200023861
and j.total_customer_amount_exc_vat >0

order by journey_id

limit 10


SELECT distinct
       'Доставка' as order_level,
        j.journey_id ,
        d.order_id_representation ,
       d.display_identifier,
       j.date_key ,
      date_format(d.requested_schedule_time, '%T') as scheduled_at,
      date_format(d.arrived_at, '%T') arrived_at, --время прибытия курьера на пикап
      date_format(d.arrived_to_drop_off_at, '%T') arrived_to_drop_off_at,
       date_format(d.ended_at, '%T') completed,

     --   delivery_status_desc,
        case when est_distance_m is not null then est_distance_m else est_distance_m_2 end as est_distance_m,
        case when est_duration_min is not null then est_duration_min else est_duration_min_2 end as est_duration_min,

     d.vendor_name,
        pickup_address,
        drop_off_address,
        --min_fare*1.2 as min_fare_vat, -- минималка за маршрут

        -- подача
        const_cost*1.2 const_vat,
        -- дистанции и длительность показываем на всех доставках, цену аллоцируем на 1-ю доставку в маршруте
        case when rn = 1 then dist.cost * 1.2 end dist_cost_vat,
        case when rn = 1 then dur.cost * 1.2 end dur_cost_vat,
        wt.waiting_cost * 1.2 wt_cost_vat,

        -- если дорогая отмена и корректировки не было
        --case when waiting_cost >=300 and corct.journey_id is null then 0 end wt_cost_vat_corrected,

        rt_cost*1.2 rt_cost_vat,
        case when d.delivery_status_id = 2
            then (coalesce (del_cost,0) + coalesce (jr_cost,0) + coalesce (rt_cost,0))*1.2 end cancellation_cost_vat,

        case when corct.journey_id is not null then 'Да' else 'Нет' end correction,
       j.total_customer_amount_exc_vat*1.2 total_cost_vat, -- just to check on a journey level
       (coalesce (del_cost,0) + coalesce (jr_cost,0) + coalesce (rt_cost,0))*1.2 as ttl_delivery_cost_vat,

        -- to check
        case when j.total_customer_amount_exc_vat is null then 'null'
            when j.total_customer_amount_exc_vat = 0 then 'zero'
                when j.total_customer_amount_exc_vat < 0 then 'negative'
            end check_label



FROM model_delivery.dwh_fact_journeys_v AS j


-- distance_client
left join
    (select journey_id, component_amount cost,
                contract_id,
                component_value est_distance_m

    from model_delivery.dwh_fact_company_monetisation_v
      where component_name = 'distance'
      and env = 'RU'
    and date(created_at) between date'2021-07-01' and date'2021-07-31'

    ) dist on j.journey_id = dist.journey_id

    -- distance_courier

left join
    (select journey_id,
                contract_id,
                component_value est_distance_m_2

    from model_delivery.dwh_fact_courier_monetisation_v
      where component_name = 'distance'
      and env = 'RU'
    and date(created_at) between date'2021-07-01' and date'2021-07-31'

    ) dist2 on j.journey_id = dist2.journey_id


--- duration_client
 left join
    (select journey_id, component_amount cost,
             contract_id,component_value est_duration_min

    from model_delivery.dwh_fact_company_monetisation_v
    where component_name = 'duration'
    and env = 'RU'
    and date(created_at) between date'2021-07-01' and date'2021-07-31'

) dur on j.journey_id = dur.journey_id


--- duration_courier
 left join
    (select journey_id,
             contract_id,component_value est_duration_min_2

    from model_delivery.dwh_fact_courier_monetisation_v
    where component_name = 'duration'
    and env = 'RU'
    and date(created_at) between date'2021-07-01' and date'2021-07-31'

) dur2 on j.journey_id = dur2.journey_id

-- corrections

left join
(select distinct journey_id, manual
    from "delivery-pricing"."public".transactions t
    where date(created_at) >= date '2021-07-01'
    and t.env = 'RU'
     and manual = true
    and side = 'customer'
    and company_id = 23861
) corct on j.journey_id = corct.journey_id

-- all deliveries
LEFT JOIN
(select *, row_number() over (partition by journey_gk, "delivery_type_id" order by ended_at asc) as rn
from model_delivery.dwh_fact_deliveries_v )AS d ON d.journey_gk = j.journey_gk

--- стоимость за подачу
left join
    (select journey_id, component_amount const_cost,
             contract_id

    from model_delivery.dwh_fact_company_monetisation_v
    where component_name = 'const'
    and calculation_type = 'sum' -- чтобы исключить минималку
    and env = 'RU'
    and date(created_at) between date'2021-07-01' and date'2021-07-31'
) cnst on j.journey_id = cnst.journey_id and d.rn = 1

--- route min fare
 left join
    (select journey_id, component_amount min_fare,
             contract_id

    from model_delivery.dwh_fact_company_monetisation_v
    where component_name = 'const'
    and calculation_type = 'max' -- чтобы исключить минималку
    and env = 'RU'
    and date(created_at) between date'2021-07-01' and date'2021-07-31'
) min_fare on j.journey_id = min_fare.journey_id and d.rn = 1

-- waiting time
left join (select journey_id, component_amount waiting_cost, contract_id
           from  model_delivery.dwh_fact_company_monetisation_v
            where component_name = 'waiting'
             and component_amount > 0 and env = 'RU'
                and date(created_at) between date'2021-07-01' and date'2021-07-31'
            ) wt on j.journey_id = wt.journey_id

-- delivery level cost

left join
(select delivery_id,
sum(amount) as del_cost
from "delivery-pricing"."public".transactions
where date(created_at) >= date '2021-07-01'
    and env = 'RU'
    and side = 'customer'
    and company_id = 23861
     and delivery_id is not null
    group by 1
) del_cost on del_cost.delivery_id = d.source_id

--- journey level cost

left join
(select journey_id,
sum(amount) as jr_cost
from "delivery-pricing"."public".transactions
where date(created_at) >= date '2021-07-01'
    and env = 'RU'
    and side = 'customer'
    and company_id = 23861
    and delivery_id is null
    group by 1
) jr_cost on jr_cost.journey_id = j.journey_id and d.rn = 1


--return_cost to allocate on 1st delivery in a route
left join
(select journey_id,
sum(amount) as rt_cost
from "delivery-pricing"."public".transactions tr
join "model_delivery"."dwh_fact_deliveries_v" fd on tr.delivery_id = fd.source_id and fd.delivery_type_id = 2
where date_key between date'2021-07-01' and date'2021-07-31'
    and env = 'RU'
    and side = 'customer'
    and company_id = 23861
    group by 1
) return_cost on return_cost.journey_id = j.journey_id and d.rn = 1

left join "model_delivery"."dwh_dim_delivery_statuses_v" ds
                            on ds."delivery_status_id" = d."delivery_status_id"

WHERE 1 = 1
aND j.country_symbol = 'RU'
AND d.company_gk NOT IN (20001999) -- Test company
AND j.date_key between date'2021-07-01' and date'2021-07-31'
and d.delivery_type_id = 1 -- exclude returns
and d.company_gk = 200023861
and j.total_customer_amount_exc_vat >0
and (coalesce (del_cost,0) + coalesce (jr_cost,0) + coalesce (rt_cost,0)) > 0 -- only paid deliveries
order by j.journey_id


