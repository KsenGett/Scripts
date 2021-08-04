 SELECT
        -- to select by company
        d.company_gk,
        j.journey_id,
        d.order_id_representation,
        d.display_identifier,

        j.date_key,
        date_format(d.requested_schedule_time, '%T') scheduled_at,
        date_format(d.started_at, '%T') started_at,
        date_format(d.arrived_to_drop_off_at, '%T') arrived_to_drop_off_at,
        date_format(d.ended_at, '%T') complited,

        delivery_status_desc,
        est_distance_m,
        est_duration_min,

        pickup_address,
        drop_off_address,

        -- подача кост,
        dist.cost * 1.2 dist_cost_vat,
        dur.cost * 1.2 dur_cost_vat,
        wt.waiting_cost * 1.2 wt_cost_vat,

        return_cost*1.2 return_vat,
        -- подача
        const_cost*1.2 const_vat,

        case when journey_status_id = 3 and j.total_customer_amount_exc_vat >0 then j.total_customer_amount_exc_vat*1.2 end cancellation_cost,

        -- pereschet
        j.total_customer_amount_exc_vat*1.2 total_cost_vat



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


LEFT JOIN model_delivery.dwh_fact_deliveries_v AS d ON d.journey_gk = j.journey_gk
left join "model_delivery"."dwh_dim_delivery_statuses_v" ds
                            on ds."delivery_status_id" = d."delivery_status_id"

WHERE 1 = 1
aND j.country_symbol = 'RU'
AND d.company_gk NOT IN (20001999) -- Test company
AND j.date_key = date'2021-07-03'