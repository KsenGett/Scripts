select distinct
        driver_id, ride_type, ftr_date, ftr_cohort,
        ftr_fleet_name,

       -- stat by days
       order_date, order_month,
        (case when row_number_date = 1 then paid_deliveries end) paid_deliveries,
       (case when row_number_date = 1 then days_from_month_end end) days_from_month_end,
        (case when row_number_date = 1 then  days_from_month_start end) days_from_month_start,

       (case when row_number_date = 1 then DAU end) DAU, -- DAU per day and ftr_cohort

       -- CAC -- to save doubled payment information only once by join on month
       (case when row_number_payment = 1 and payment_month=order_month then payment_month end) payment_month,
       programm,
       (case when row_number_payment = 1 and payment_month=order_month then payoff end) payoff

from(

    select *,
           -- count DAU with ignore of doubles
           count(case when row_number_date = 1 then driver_id end) over (partition by order_date, ftr_cohort) DAU
    from
    (
    select
           da.*,
           ord.date_key order_date,
           date_format(ord.date_key, '%Y-%m') AS order_month,
           paid_deliveries,
           -- to calculate avg dau by filter by month order dates (like avg DAU for last 14 month days or for first 10 month days)
           days_from_month_end, days_from_month_start,
           -- to count doubles and calculate dau only for 1st row. doubles are due to payment info. one driver may have two in two months
           row_number() over (partition by da.driver_id,ord.date_key) row_number_date,

           -- to save payment only once
          row_number() over (partition by da.driver_id, da.payment_month, date_format(ord.date_key, '%Y-%m') order by ord.date_key) row_number_payment

    from (
        select cast(driver_id as bigint) driver_id,
           ride_type,
           ftr_date,
           date_format( date(ftr_date), '%Y-%m') AS ftr_cohort,
           programm,
           ftr_fleet_name,
     --            week stat_week,
     --            payment_week,
           payment_month,
           sum(cast(payoff as integer)) payoff

            from sheets.default.payed_drivers_acquisition da
            LEFT JOIN (
                                    select subperiod stat_week,
                                           min(date_key) stat_week_day,
                                           concat('W',cast(week(min(date_key)+ interval '7' day) as varchar)) payment_week,
                                           date_format( min(date_key)+interval '7' day, '%Y-%m') payment_month
                                    from data_vis.periods_v
                                    where timecategory IN ( '3.Weeks')
                                      and hour_key=0 and period= '2021'
                                    group by 1
                        ) tp on tp.stat_week = concat('W',da.week)
            where true
                  and cast(payoff as integer)>=0
                  --and driver_id = '1038070'
            group by 1,2,3,4,5,6,7--,8,9
            order by driver_id
            ) da
    left join
            (select distinct
              dd.driver_gk,
              dd.source_id,
              fo.date_key,
              tp.week, tp.month,
              coalesce(fo.orders, 0) + coalesce(md.deliveries,0) paid_deliveries,
                   -- to calculate avg dau by filter by month order dates (like avg DAU for last 14 month days or for first 10 month days)
             days_from_month_end, days_from_month_start

            from emilia_gettdwh.dwh_dim_drivers_v dd
            join sheets.default.payed_drivers_acquisition da on dd.source_id = cast(da.driver_id as bigint)

            left join
                (
                    select distinct driver_gk,
                                    fo.date_key,
                                    count(distinct case when ct.class_family <> 'Premium' and ordering_corporate_account_gk <> 20004730 then order_gk end) orders,
                        count(distinct case when ordering_corporate_account_gk = 20004730 then order_gk end) orders_nf
                    from emilia_gettdwh.dwh_fact_orders_v fo
                    left join emilia_gettdwh.dwh_dim_class_types_v ct on fo.class_type_key=ct.class_type_key

                    where fo.lob_key in (5, 6)
                      and date_key >= date '2021-04-01'
                      --and ordering_corporate_account_gk <> 20004730
                      and customer_total_cost>0
                      and fo.country_key = 2
                      --and ct.class_family <> 'Premium'

                    group by 1, 2
                ) fo on fo.driver_gk = dd.driver_gk

            left join --2sec
                    (
                        select distinct courier_gk,
                                        date_key,
                                        count(distinct delivery_gk) deliveries,
                                        count(distinct journey_gk)  journeys

                        from model_delivery.dwh_fact_deliveries_v d

                        where
                            date (d.created_at) >= date'2021-04-01'
                            and delivery_status_id = 4
                            and d.country_symbol= 'RU'
                            and total_customer_amount_exc_vat > 0
                            group by 1,2
                    ) md on md.courier_gk = fo.driver_gk and fo.date_key = md.date_key

            left join
                        (
                            select *,
                        date_diff('day',date_key,last_month_date) days_from_month_end,
                        date_diff('day',first_month_date,date_key) days_from_month_start
                        from(
                                select
                                subperiod week,
                                date_format(date_key, '%Y-%m') "month",
                                date_key,
                                max(date_key) over (partition by date_format(date_key, '%Y-%m')) last_month_date,
                                min(date_key) over (partition by date_format(date_key, '%Y-%m')) first_month_date


                                from data_vis.periods_v
                                 where timecategory IN ( '3.Weeks')
                                and hour_key=0 and period= '2021'
                        )) tp on tp.date_key = fo.date_key

        where true --and month(fo.date_key) = 6--da.driver_id = '102806'
            ) ord on ord.source_id = da.driver_id

    where date(da.ftr_date) >= date'2021-04-01'
    --and ord.date_key = date'2021-05-01'
    --and da.driver_id in (102806,1382167)
    and date_format(ord.date_key, '%Y-%m') >= date_format( date(ftr_date), '%Y-%m')
    order by da.driver_id desc
    )
)
-- delete double rows with nulls
where true
and (row_number_date = 1 or (row_number_payment = 1 and payment_month=order_month))
--and driver_id in (102806)--102806,1382167)
order by driver_id, order_date