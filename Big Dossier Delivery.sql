with orders as -- orders, eta, surge
(
        select
        fo.date_key,
        fo.origin_location_key,
        fo.ordering_corporate_account_gk,
        count(distinct CASE when ct.class_family <> 'Premium' and fo.ordering_corporate_account_gk <> 20004730
                            and order_status_key = 7 or (order_status_key=4 and "order_cancellation_stage_key" = 3)
                            THEN fo.order_gk end
                ) AS paid_deliveries_of,
        count(distinct CASE when ct.class_family <> 'Premium' and fo.ordering_corporate_account_gk <> 20004730
                            and order_status_key = 7 and driver_gk <> 200013 THEN fo.order_gk end
                ) AS completed_deliveries_of,
        count(distinct CASE when ct.class_family <> 'Premium' and fo.ordering_corporate_account_gk <> 20004730
                            then fo.order_gk end
                ) AS gross_deliveries_of,
        sum(CASE when ct.class_family = 'Premium' and ct.class_type_desc not like '%ondemand% 'then jo.completed_deliveries
                            when ct.class_family = 'Premium' and ct.class_type_desc like '%ondemand%'
                            THEN jo.picked_up_deliveries ELSE 0 end) AS paid_deliveries_NF,
        sum(CASE when ct.class_family = 'Premium' THEN jo.completed_deliveries ELSE 0 end) AS completed_deliveries_NF,
        sum(CASE when ct.class_family = 'Premium' THEN jo.gross_deliveries ELSE 0 end) AS gross_deliveries_NF,

        sum(case when ct.class_family = 'Premium' and fo.ordering_corporate_account_gk <> 20004730
                            and fo.user_base_surge > 1 then jo.gross_deliveries
                             when ct.class_family <> 'Premium' and fo.ordering_corporate_account_gk <> 20004730
                             then 1 end) surged_orders,

        sum(fo.m_order_eta) sum_eta,
        count(case when fo.m_order_eta > 0 then fo.m_order_eta end) count_eta,
        count(case when fo.m_order_eta >= 10 then 1 end) count_eta_10plus


        FROM "emilia_gettdwh"."dwh_fact_orders_v" fo
        LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
          ON ct.class_type_key = fo.class_type_key
        LEFT JOIN
        (
                SELECT  j.order_gk ,
                -- supplier_id = 13 - CC user that is used to cancel orders, all his orders should be marked AS cancelled
                count(distinct CASE when d.delivery_status_id = 4 and j.courier_gk <> 200013 THEN d.delivery_gk end) AS completed_deliveries,
                count(distinct CASE when d.delivery_status_id IN (4,7) and j.courier_gk <> 200013 THEN d.delivery_gk end) AS picked_up_deliveries,
                count(d.delivery_gk) AS gross_deliveries

                FROM model_delivery.dwh_fact_journeys_v AS j

                LEFT JOIN model_delivery.dwh_fact_deliveries_v AS d ON d.journey_gk = j.journey_gk
                WHERE 1=1
                and d.delivery_status_id IN (4,7,10,2) ---('completed','not_delivered', 'rejected', 'cancelled')
                and j.country_symbol ='RU'
                and d.display_identifier <> 'Returns'
                and d.company_gk <> 20001999
                GROUP BY 1
                ) AS jo ON jo.order_gk = fo.order_gk

        where fo.country_key = 2
        and fo.lob_key in (5,6)
        --and fo.date_key between current_date - interval '12' month and current_date
        and fo.date_key between current_date - interval '12' day and current_date
        group by 1,2,3
)

, offers as -- long short offers, AR
(
        select
        fof.date_key,
        fof.origin_order_location_key,
        fof.ordering_corporate_account_gk,
        count(distinct offer_gk) gross_offers,
        count(distinct case when offer_screen_eta >= 45 then offer_gk end) long_offers,
        count(distinct case when offer_screen_eta <= 10 then offer_gk end) short_offers,

        SUM(  CASE WHEN fof.Driver_Response_Key=1 THEN 1 ELSE 0 END) AS numerator,

        (SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL THEN 1 ELSE 0 END)
                - SUM(CASE WHEN fof.Delivered_Datetime IS NOT NULL AND fof.Is_Withdrawned=1
                AND fof.Driver_Response_Key<>1 THEN 1 ELSE 0 END) )  AS denominator


        from emilia_gettdwh.dwh_fact_offers_v fof
        JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct
          ON ct.class_type_key = fof.class_type_key
          and lob_key in (5,6) and ct.country_key = 2

        where fof.country_key = 2
        and fof.date_key between current_date - interval '12' day and current_date

        group by 1,2,3
)

, ata as -- ATA
(
        select date_key,
        ordering_corporate_account_gk,
        origin_location_key, sum(ata) ata, sum(count_ata) count_ata
        from
        (
        select fo.date_key,
            fo.ordering_corporate_account_gk,
            fo.origin_location_key,

            sum(CASE when date_diff('second', fo.order_datetime,fo.driver_arrived_datetime)*1.00/60 > 0 THEN
            date_diff('second', fo.order_datetime, fo.driver_arrived_datetime)*1.00/60 end) AS ata,
            sum(CASE when date_diff('second', fo.order_datetime,fo.driver_arrived_datetime)*1.00/60 > 0 THEN 1
            end) count_ata


            from emilia_gettdwh.dwh_fact_orders_v fo
            LEFT JOIN emilia_gettdwh.dwh_dim_class_types_v AS ct ON ct.class_type_key = fo.class_type_key

            where 1=1
                and fo.date_key between current_date - interval '12' day and current_date
                and fo.country_key = 2
                and fo.lob_key in (5,6)
                and ct.class_family not IN ('Premium')
                and ct.class_group not like 'Test'
                and fo.order_status_key in (1,2,3,4,5,6,7)
                and fo.ordering_corporate_account_gk not in (20004730,200017459)
            group by 1,2,3

        union

            select
            date(fd.scheduled_at) AS date_key,
            company_gk ordering_corporate_account_gk,
            j.pickup_location_key,

            sum(CASE when date_diff('second', coalesce(fd.requested_schedule_time, fd.scheduled_at) , fd.arrived_at)*1.00/60 > 0 THEN
            date_diff('second', coalesce(fd.requested_schedule_time, fd.scheduled_at) , fd.arrived_at) end)*1.00/60 AS ata,
            sum(CASE when date_diff('second', coalesce(fd.requested_schedule_time, fd.scheduled_at) , fd.arrived_at)*1.00/60 >= 0 THEN 1 end) count_ata

            FROM "model_delivery"."dwh_fact_deliveries_v" fd

                LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca ON ca.corporate_account_gk = fd.company_gk
                    and ca.country_symbol = 'RU'
                LEFT JOIN model_delivery.dwh_fact_journeys_v j ON fd.journey_gk = j.journey_gk
                and j.country_symbol ='RU' and date(j.created_at) >= date'2020-08-01'

            WHERE fd.country_symbol ='RU'
              and lower(ca.corporate_account_name) not like '%test%'
              and date(fd.scheduled_at) between current_date - interval '12' day and current_date
              and fd.company_gk  not in (200017459, 20004730)
              and (date_diff('second', fd.created_at, fd.scheduled_at))*1.00/60 <= 20
              and fd.delivery_status_id IN (4,7,10,2)

              group by 1,2,3
        )
        group by 1,2,3
)

, assignment_time as
(
        select
        fo.date_key,
        fo.ordering_corporate_account_gk,
        fo.origin_location_key,
        sum(CASE when fo.order_confirmed_datetime is not null
                and app.order_id is not null
                and fo.cancellations_time_in_seconds is null
                and fo.order_confirmed_datetime >= fo.order_datetime
            THEN date_diff('second', fo.order_datetime, fo.order_confirmed_datetime) end) AS clean_assignment_time_num_r, -- seconds

        count(CASE when fo.order_confirmed_datetime is not null
                            and app.order_id is not null
                            and fo.order_confirmed_datetime >= fo.order_datetime
                            and fo.cancellations_time_in_seconds is null
                THEN fo.order_gk end) AS clean_assignment_time_denum_r

        from emilia_gettdwh.dwh_fact_orders_v fo
        -- routing
                left join
                (
                    select order_id
                    FROM app_events
                    WHERE event_name IN ('matching|sent_to_routing' ,'futureorder|send_to_routing')
                    and occurred_date between current_date - interval '12' day and current_date
                    and env = 'RU'
                ) app on substring(cast(fo.gt_order_gk as varchar), 5) = cast(app.order_id as varchar)
                                    and fo.country_key = 2

        where fo.country_key = 2
        and fo.lob_key in (5,6)
        and fo.date_key between current_date - interval '12' day and current_date
        and fo.ordering_corporate_account_gk <> 20004730

        group by 1,2,3
)

(
select o.*,
a.ata *1.00 / a.count_ata ATA, -- min
at1.clean_assignment_time_num_r *1.00 / at1.clean_assignment_time_denum_r "AT" --seconds

from orders o
left join ata a on a.date_key = o.date_key and
        o.origin_location_key = a.origin_location_key and
        o.ordering_corporate_account_gk = a.ordering_corporate_account_gk

left join assignment_time at1 on
        at1.date_key = o.date_key and
        o.origin_location_key = at1.origin_location_key and
        o.ordering_corporate_account_gk = at1.ordering_corporate_account_gk
)


;


select j.order_gk, j.m_order_eta /60.00 j_eta, fo.m_order_eta/60.00 fo_eta,
(case when fof.driver_response_key = 1 then offer_screen_eta/60.00 end) offer_eta,
fo.origin_full_address

from model_delivery.dwh_fact_journeys_v j
left join emilia_gettdwh.dwh_fact_orders_v fo on j.order_gk = fo.order_gk
left join emilia_gettdwh.dwh_fact_offers_v fof on fo.order_gk = fof.order_gk
    and fof.country_key = 2 and fof.date_key >= date'2021-2-1'
    1
where j.country_symbol = 'RU'
and fo.date_key >= date'2021-2-1'
and j.m_order_eta is not null
and fof.driver_response_key = 1
order by order_gk


