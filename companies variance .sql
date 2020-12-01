select
date_key,
ordering_corporate_account_gk,
count(distinct CASE when (order_status_key = 7 or (order_status_key = 4 and driver_total_cost > 0))
THEN order_gk ELSE null end) CAA_orders,
count(distinct order_gk) gross_orders

from emilia_gettdwh.dwh_fact_orders_v

where ordering_corporate_account_gk in
(20007665,20008770,20005031,20004469,200025241,20007748,200010175,200010176,200025197)
and date_key >= date'2020-10-01'

group by 1,2

union

select
date(created_at) date_key,
cast(company_id as integer),
count(distinct CASE when status IN ('completed', 'not_delivered')THEN id end ) AS completed_and_cancelled_orders,
count(distinct id) AS gross_orders

FROM delivery.public.deliveries
where company_id = '25140'
and date(created_at) >= date'2020-10-01'

group by 1,2;