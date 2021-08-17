/*
Owner - Kozlova Kseniia
Cube Name - check manual corrections
ID -
*/

select correction_date,
       journey_date,
       journey_id,
       order_gk,
       amount,
       comment,
       side,
       company_id,
       date_diff('day', current_date, date(correction_date)) days_ago

from (
    select tr.journey_id, amount, comment, side, date (tr.created_at) correction_date,
    company_id, j.date_key journey_date, order_gk,
    array_agg(comment) over (partition by tr.journey_id) comments

    from "delivery-pricing".public.transactions tr
    left join model_delivery.dwh_fact_journeys_v j on tr.journey_id = j.journey_id
                and country_symbol = 'RU'
                and j.date_key between current_date - interval '45' day and current_date


    where date (tr.created_at) between current_date - interval '45' day
      and current_date
      and env = 'RU'
      and (abs(amount) = 10
       or abs(amount) >= 4000)
      and manual = True
      and comment <> 'test'
    group by 1, 2, 3, 4, 5, 6, 7
    order by tr.journey_id
    )
-- exclude cases when wrong correction for a journey has been already fixed
where contains(comments, 'ошибочная корректировка') = false