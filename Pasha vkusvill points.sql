select
distinct vv.adress ,
case when vv.store_number = d.store_number then 'NF обслуживание'
when vv.adress = fo.store_number then 'OF обслуживание' end service,

case when vv.adress = fo.store_number then fo.last_2weeks_active
when vv.store_number = d.store_number then d.last_2weeks_active end last_2weeks_active

from sheets."default".addresses vv
left join (
        select distinct adress, store_number,
        case when max(fo.date_key) >= date'2020-10-01' then 'yes' end last_2weeks_active

        from sheets."default".addresses vv
        join emilia_gettdwh.dwh_fact_orders_v fo
        on vv.adress = fo.origin_full_address
        and ordering_corporate_account_gk in (200023153, 200025199, 200023861)
        and country_key = 2
        and lob_key = 5
        group by 1,2
        ) fo on vv.store_number = fo.store_number --930 1341 1256 1591 1005
left join (
        select
        distinct adress, store_number,
        case when max(d.created_at) >= timestamp'2020-10-01 00:00:00' then 'yes' end last_2weeks_active
        from sheets."default".addresses vv
        join delivery."public".deliveries d
        on vv.store_number = json_extract_scalar(d.vendor, '$.name')
        and company_id = '23861'
        group by 1,2
        ) d on d.store_number = vv.store_number



select distinct origin_full_address,
        case when max(fo.date_key) >= date'2020-10-01' then 'yes' end last_2weeks_active

        from  emilia_gettdwh.dwh_fact_orders_v fo

        where ordering_corporate_account_gk in (200023153, 200025199, 200023861)
        and country_key = 2
        and lob_key = 5
        and origin_location_key = 245
        group by 1

-- VV list + our ticks
 select distinct adress, store_number,
        case when vv.adress = fo.origin_full_address then 'yes' end we_work_with,
        origin_location_key,
        case when max(fo.date_key) >= date'2020-10-01' then 'yes' end last_2weeks_active

        from sheets."default".addresses vv
        left join emilia_gettdwh.dwh_fact_orders_v fo
        on vv.adress = fo.origin_full_address
        and ordering_corporate_account_gk in (200023153, 200025199, 200023861)
        and country_key = 2
        and lob_key = 5
        group by 1,2,3,4

-- Our addresses that we have but VV does not --addresses in the region
select distinct origin_full_address,
        case when vv.adress = fo.origin_full_address then 'yes' end in_vv_list,
        array_agg(distinct ca.corporate_account_name) company,
        array_agg(distinct order_gk) orders,
        min(date_key) worked_since,
        max(date_key) worked_till

        from emilia_gettdwh.dwh_fact_orders_v fo
        left join sheets."default".addresses vv
        on vv.adress = fo.origin_full_address
        LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
          ON ca.corporate_account_gk = fo.ordering_corporate_account_gk

        where 1=1
        and ordering_corporate_account_gk in (200023153, 200025199, 200023861)
        and fo.country_key = 2
        and lob_key = 5
        and origin_location_key = 245
        group by 1,2


-- two week inactive
select distinct adress, store_number,
        case when vv.adress = fo.origin_full_address then 'yes' end we_work_with,
        origin_location_key, fo.origin_latitude, fo.origin_longitude,
        case when max(fo.date_key) <= date'2020-10-02' then 'no' end last_2weeks_active,
        array_agg(distinct ca.corporate_account_name) company

        from sheets."default".addresses vv
        left join emilia_gettdwh.dwh_fact_orders_v fo
        on vv.adress = fo.origin_full_address
        and ordering_corporate_account_gk in (200023153, 200025199, 200023861)
        and country_key = 2
        and lob_key = 5
        LEFT JOIN emilia_gettdwh.dwh_dim_corporate_accounts_v AS ca
          ON ca.corporate_account_gk = fo.ordering_corporate_account_gk
        group by 1,2,3,4,5,6


