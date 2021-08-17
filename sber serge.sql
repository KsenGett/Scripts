select
fj.journey_id,
fj.date_key,
hour(fj.created_at) hour_key,
week(fj.date_key) week,
pickup_address,

c.contract_id, contract_id in (488,480) surge, --488 10-11 480 16-20

       -- AR
fof.numerator, fof.denominator,

       --GCR
       COUNT(CASE WHEN fd.delivery_status_id = 4 AND fj.courier_gk <> 200013 THEN fd.delivery_gk END) AS completed_deliveries, -- j.courier_gk = 200013 is a CC user that is used to cancel orders, all his orders should be marked AS cancelled
COUNT(CASE WHEN fd.delivery_status_id IN (4,7) AND fj.courier_gk <> 200013 THEN fd.delivery_gk END) AS picked_up_deliveries, -- j.courier_gk = 200013 is a CC user that is used to cancel orders, all his orders should be marked AS cancelled
COUNT(fd.delivery_gk) AS gross_deliveries,

  --routing
sum(CASE when fj.is_future_order_key <> 1  THEN
date_diff('second',fj.scheduled_at, coalesce(fj."started_at",fj.cancelled_at))*1.000/60 end) AS routing_time,
sum((CASE when fj.is_future_order_key <> 1 THEN 1 end)) count_rouing,
         --ATA
sum((CASE when date_diff('second', coalesce(fd.requested_schedule_time, fd.scheduled_at) , fd.arrived_at)*1.00/60 > 0 THEN
date_diff('second', coalesce(fd.requested_schedule_time, fd.scheduled_at) , fd.arrived_at) end)*1.00/60) AS ata,
sum((CASE when date_diff('second', coalesce(fd.requested_schedule_time, fd.scheduled_at) , fd.arrived_at)*1.00/60 >= 0 THEN 1 end))
 count_ata



from model_delivery.dwh_fact_deliveries_v fd
left join  model_delivery.dwh_fact_journeys_v fj on fj.journey_gk = fd.journey_gk

left join (
        select distinct journey_id, company_gk, contract_id

        from model_delivery.dwh_fact_company_monetisation_v
        where date(created_at) >= date'2021-07-19'
        and company_gk = 200025410
        ) c on c.journey_id = fj.journey_id

left join (SELECT
     fof.order_gk,
     SUM(CASE WHEN fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) AS numerator, -- accepted

    (SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL or fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) --received
           - SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL AND fof.Is_Withdrawned=1
           AND fof.Driver_Response_Key<>1 THEN 1 ELSE 0 END) )  AS denominator

    FROM emilia_gettdwh.dwh_fact_offers_v fof

    WHERE true
    and date_key >= date'2021-07-19'
      and fof.origin_order_location_key = 245
    and ordering_corporate_account_gk = 200025410
    GROUP BY 1) fof on fof.order_gk = fj.order_gk

where fd.pickup_location_key =245
and fj.date_key >= date'2021-07-19'
and fd.company_gk = 200025410 -- sber

group by 1,2,3,4,5,6,7,8,9;
