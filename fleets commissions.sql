
         select *, driver_total_commission_inc_vat * (com_shouldbe - fleet_commission_inc_vat) loss

         from (
                  select fleet_gk,
                         fl.vendor_name,
                         fo.date_key,
                         fo.hour_key,
                         order_gk,
                         driver_total_cost,
                         driver_total_commission_inc_vat,
                         driver_total_commission_exc_vat,
                         (case
                              when fl.vendor_name like '%МСК%' and fl.vendor_name like '%courier cars%' then 0.18
                              when fl.vendor_name like '%МО%' and fl.vendor_name like '%courier cars%' then 0.18
                              when fl.vendor_name like '%МСК%' and fl.vendor_name like '%courier trike%' then 0.18
                              when fl.vendor_name like '%СПБ%' and fl.vendor_name like '%courier cars%' then 0.18
                              when fl.vendor_name like '%СПб%' and fl.vendor_name like '%courier cars%' then 0.18
                              when fl.vendor_name like '%scooter%' then 0.18
                              when fl.vendor_name like '%pedestrian%' then 0.12

                              else 0.12
                             end)                          com_shouldbe,
                         sum(driver_total_cost)            driver_cost,
                         sum(driver_total_commission_inc_vat) * (-1) /
                         nullif(sum(driver_total_cost), 0) fleet_commission_inc_vat


                  from emilia_gettdwh.dwh_fact_orders_v fo
                           left join emilia_gettdwh.dwh_dim_vendors_v fl on fo.fleet_gk = fl.vendor_gk

                  where lob_key in (5, 6)
                    and fo.country_key = 2
                    --and order_status_key = 7
                    and ordering_corporate_account_gk <> 20004730
--and date_key between date'2021-04-01' and date'2021-05-05'
                    and date_key >= date '2021-6-01'
                    and vendor_name like '%courier%'

                  group by 1, 2, 3, 4, 5, 6, 7, 8
              )

         where com_shouldbe > fleet_commission_inc_vat

select * from desc  model_delivery.dwh_fact_journeys_v


