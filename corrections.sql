-- Correction tracking
-- to find cases which were corrected in BKK but not in New Pricing (only manual charges)
with new_pricing as (
     with all_prices as (
          select
           distinct date(created_at) as dates,
           company_id ,
           journey_id,
           side,
            manual, comment,
        sum(amount) over (partition by journey_id,side) as total_amounts,
        max(created_at) over (partition by journey_id,side) as last_time_tr,
       max(created_at) over (partition by journey_id,side,manual) as last_time_tr_2
           --tr.bkk_order_id,


        FROM "delivery-pricing".public.transactions AS tr
         where tr.env = 'RU'
         --and manual = true
             --and journey_id = 2257191
          and tr.created_at >= date_trunc('day', current_date - interval '8' day )

     )
             , customers as (
               select distinct
                company_id,
                journey_id, manual corrections_tag, comment,
                last_time_tr,total_amounts,last_time_tr_2

               from all_prices
               where side = 'customer'

             )
             , suppliers as (
              select distinct
                company_id,
                journey_id, manual corrections_tag, comment,
                last_time_tr,total_amounts,last_time_tr_2
               from all_prices
               where side = 'supplier'
             )
         (select
          coalesce(cus.company_id, sup.company_id) as company_id,
          coalesce(cus.journey_id, sup.journey_id) as journey_id,
          coalesce(cus.last_time_tr , sup.last_time_tr) as last_tr,

        cus.total_amounts as customer_cost,
          cus.corrections_tag customer_correction, cus.comment customer_comment,
          sup.total_amounts as supplier_cost,
            sup.corrections_tag supplier_corrections, sup.comment supplier_comments,
          cus.last_time_tr as customers_last_time_tr,
          sup.last_time_tr as suppliers_last_time_tr
         from customers cus
          full outer join suppliers sup
           on cus.journey_id = sup.journey_id
               -- to select only one final correction lable (was corrected at all finally or not) cosidering that correction = max transaction
               and sup.last_time_tr = sup.last_time_tr_2
            and cus.company_id = sup.company_id
         where cus.last_time_tr = cus.last_time_tr_2
        )
               )
, bkk_c as (
 select
  date(created_at) as dates,
  order_id,
  bkk_order_id,
  supplier_calculation_amount_exc_tax,
  customer_calculation_amount_exc_tax ,
  user_email,
  comment
 from hive.bookkeeping.operations_features_v
 where env = 'RU'
  and operation_type = 'Operation::Modify'
  and created_date >= date_trunc('day', current_date - interval '8' day )
  and (env = 'RU' or (env = 'GL' and country = 'RU'))
  and class_name like '%delivery%'
)
(select
 distinct

--general
 bkk_c.order_id,
 np.journey_id,
 np.company_id,
date(j.started_at) as ride_date,

-- bkk correction
  user_email,
  comment bkk_correction_comment,
bkk_c.dates as date_bkk_change,
np.last_tr last_tr_np,

-- customer
 np.customer_cost as customer_cost_np,
   np.customer_correction customer_correction_tag_np, np.customer_comment customer_comment_np,
bkk_c.customer_calculation_amount_exc_tax as customer_cost_bkk,
 np.customers_last_time_tr as customers_last_time_tr_np,

-- supplier
np.supplier_cost as supplier_cost_np,
  np.supplier_corrections supplier_corrections_tag_np, np.supplier_comments supplier_comment_np,
 bkk_c.supplier_calculation_amount_exc_tax as supplier_cost,
 np.suppliers_last_time_tr as suppliers_last_time_tr_np

from new_pricing np
 left join delivery.public.journeys j
  on j.id = np.journey_id
 left join hive.bookkeeping.orders_features_v bkk
  on bkk.external_id = j.legacy_order_id
 left join bkk_c on bkk_c.bkk_order_id = bkk.bkk_order_id --and bkk.class_name like '%delivery%' and year(bkk.created_at)>2019

where 1=1
 and np.company_id = 25140
 and bkk_c.order_id is not null -- correction was in BKK
 and np.customer_correction = False -- was not manual correction in New pricing
 and comment not like '%Ничего не исправлялось%' -- in BKK corrections sis not cause price changes

-- old filters from Kolya
 --and np.journey_id is not null
--and order_id = 1629918525
 --and np.customer_cost = 0
 --and date(j.started_at) = np.customers_last_time_tr
  --and (np.suppliers_last_time_tr < bkk_c.dates or np.customers_last_time_tr < bkk_c.dates )
 --and np.customer_cost <> bkk_c.customer_calculation_amount_exc_tax
 --and bkk_c.supplier_calculation_amount_exc_tax > 0
)