/*
Owner - Ekaterina Nesterova
Cube Name - New Pricing
ID - 259B88D611EB12D110170080EF8596C8
*/


with all_orders as (
    with bkk_corrections as (
        select distinct bkk_order_id,
                        array_agg(comment)                     as correction_comments,
                        array_join(array_agg(user_email), ',') as user_email
        from hive.bookkeeping."operations_features_v"
        where 1 = 1
                  and (env = 'RU' or (env = 'GL' and country = 'RU'))
                  and class_name like '%delivery%'
                  and operation_type = 'Operation::Modify'
                  and year (created_at) >2019
group by 1
    )

    (
SELECT j.legacy_order_id,
    j.id as journey_id,
    j.scheduled_at,
    ct.class_type_desc,
    "corporate_account_name",
    am.name AS account_manager,
    tr."company_id",
    date (j.scheduled_At) AS date_key,
    j.supplier_id,
    j.status,
    fo.origin_full_address,
    --customer_total_cost AS OLD_customer_total_cost,
    customer_calculation_amount as OLD_customer_total_cost,
    "last_operation_at",
    case when bkk_c.bkk_order_id is not null then 1 else 0 end as bkk_correction_flag,
    bkk_c.correction_comments,
    bkk_c.user_email,
    driver_total_cost_inc_vat as OLD_driver_total_cost_inc_vat,
    driver_total_commission_exc_vat,

    array_join(array_agg( tr."delivery_id"), ', ') AS deliveries_list,
    max(wt.waiting_cost) WT_customer_cost,
    count (distinct tr.delivery_id) as del_num,
    sum ( case when tr.side = 'supplier' then tr.amount end) AS NEW_supplier_total_cost_with_VAT,
    sum ( case when tr.side = 'customer' then tr.amount end) AS NEW_customer_total_cost_wo_VAT
FROM "delivery-pricing".public.transactions AS tr
left join
    (select distinct journey_id, component_amount waiting_cost, contract_id
    from model_delivery.dwh_fact_company_monetisation_v
    where component_name = 'waiting'
    and component_amount > 0
    ) wt on tr.journey_id = wt.journey_id
    LEFT JOIN "delivery"."public"."journeys" j
ON tr.journey_id = j.id
    LEFT JOIN emilia_gettdwh.dwh_fact_orders_v AS fo ON fo.sourceid = j.legacy_order_id and fo.country_symbol = j.env and fo.lob_key = 5 and fo.date_key >= date '2020-01-01'
    left join "emilia_gettdwh"."dwh_dim_class_types_v" ct on ct.class_type_key = fo.class_type_key
    left join hive.bookkeeping.orders_features_v bkk on external_id = legacy_order_id and bkk.class_name like '%delivery%' and year (bkk.created_at)>2019
    left join bkk_corrections bkk_c on bkk_c.bkk_order_id = bkk.bkk_order_id
    LEFT JOIN "emilia_gettdwh"."dwh_dim_corporate_accounts_v" ca ON ca.source_id = tr.company_id and ca."country_symbol" = tr."env"
    LEFT JOIN emilia_gettdwh.dwh_dim_account_managers_v am ON am."account_manager_gk" = ca."account_manager_gk"


WHERE 1=1
  and j.env = 'RU'
  and date (tr.created_at) >= date '2020-10-01'
  and date (j.scheduled_at) >= date '2020-10-01'
  and ca.corporate_account_name is not null
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
    )
    )
       (select * from all_orders
       where (OLD_customer_total_cost > 0
       or OLD_driver_total_cost_inc_vat > 0
       or NEW_customer_total_cost_wo_VAT > 0
       or NEW_supplier_total_cost_with_VAT > 0)
       )

;