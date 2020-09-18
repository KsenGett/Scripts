--main
select count(free_text), d.created_at, company_id, ci.cancellation_stage
from delivery."public".cancellation_infos ci
join delivery."public".deliveries d
on ci.cancellable_id = d.id
and d.env = 'RU'
where cancellable_type = 'deliveries'
and cancelled_by = 'supplier'
and reason_type is null
and d.env = 'RU'
--and ci.cancellation_stage = 'not_delivered'

-- how many comments by categories
select
count(case when free_text = 'Заказ не выдан'then free_text end) zakaz_ne_vidan,
count(case when free_text = 'Курьер не успел' then free_text end) curier_ne_uspel,
count(case when free_text = 'Не удалось связаться' then free_text end) ne_udalos_svazatca,
count(case when free_text = 'Неверный адрес' then free_text end) neverniy_adress,
count(case when free_text = 'Отказ при звонке' then free_text end) otkaz_pri_zvonke,
count(case when free_text = 'Перенос по просьбе клиента' then free_text end) perenos_po_prosbe_clienta,
count(case when free_text = 'Претензия к качеству товара' then free_text end) preteziya_k_kachestvu

--distinct free_text, d.created_at,
--company_id, ci.cancellation_stage
from delivery."public".cancellation_infos ci
join delivery."public".deliveries d
on ci.cancellable_id = d.id
and d.env = 'RU'
where cancellable_type = 'deliveries'
and cancelled_by = 'supplier'
and reason_type is not null
and d.env = 'RU'
and ci.cancellation_stage = 'not_delivered'

--earliest comment was
select min(d.created_at)
from delivery."public".cancellation_infos ci
join delivery."public".deliveries d
on ci.cancellable_id = d.id
and d.env = 'RU'
where cancellable_type = 'deliveries'
and cancelled_by = 'supplier'
and reason_type is null
and d.env = 'RU'



-- without OZON
select distinct free_text, d.created_at, ci.cancellation_stage
from delivery."public".cancellation_infos ci
join delivery."public".deliveries d
on ci.cancellable_id = d.id
and d.env = 'RU'
where cancellable_type = 'deliveries'
and cancelled_by = 'supplier'
and reason_type is null
and d.env = 'RU'
and d.company_id <> cast(12721 as varchar)
and ci.cancellation_stage = 'not_delivered'

select --count(free_text)
--count(case when free_text = 'Отказ при звонке' then free_text end) --1166
--count(case when free_text = 'Курьер не успел' then free_text end) --6862
--count(case when free_text = 'Неверный адрес' then free_text end) --3309
--count(case when free_text = 'Перенос по просьбе клиента' then free_text end) --21702
--count(case when free_text = 'Претензия к качеству товара' then free_text end) -344
--count(case when free_text = 'Заказ не выдан' then free_text end) --51911
--count(case when free_text = 'Не удалось связаться' then free_text end) --19992/105346

/*
*1.00/105308 ne_udalos_svazatca

count(case when free_text = 'Заказ не выдан' then free_text end)*1.00/105308 zakaz_ne_vidan,
count(case when free_text = 'Претензия к качеству товара' then free_text end)*1.00/105308 pretenziya_k_kachestvu_tovara,
count(case when free_text = 'Перенос по просьбе клиента' then free_text end)*1.00/105308 perenos_po_prosbe_client,
count(case when free_text = 'Неверный адрес' then free_text end)*1.00/105308 neverniy_adres,
count(case when free_text = 'Курьер не успел' then free_text end)*1.00/105308 curier_ne_uspel,
count(case when free_text = 'Отказ при звонке' then free_text end)*1.00/105308 otkaz_pri_zvonke

 */

from delivery."public".cancellation_infos ci
join delivery."public".deliveries d
on ci.cancellable_id = d.id
and d.env = 'RU'
where cancellable_type = 'deliveries'
and cancelled_by = 'supplier'
and reason_type is not null
and d.env = 'RU'

--and d.company_id <> cast(12721 as varchar)