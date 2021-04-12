select
 dr.driver_id ,
 --ar.order_id,
 dr."data" ,
 dr.succeeded_at ,
 --qi.quest_name,
 qi.feature_name,
 date(qi."start") as start_date,
  date(qi."end") as end_date,
  qi.quest_id,
     --qi.feature_name,
     split_part(qi.feature_name, '_', 5) as city,
     dm.driver_id,
     --succeeded_at,
     --ar.order_id ,
     --dr.data,
     dm.quest_iteration_id,
     (dm.created_at) as reward_datetime--,
     --cast((json_extract((dr.data), '$.reward_amount') ) as bigint) reward_value
from desc quest.public.driver_rewards dr

 left join  quest."public".driver_milestones dm on dr.driver_id = dm.driver_id and dr.driver_milestones_id = dm.id
 -- qwest info
 left join quest."public".quest_iterations qi on dm.quest_iteration_id = qi.id

where 1=1 --dr.driver_id = 979

 and date(dr.updated_at ) between date'2020-12-29' and date'2020-12-29'
 and (feature_name like '%20201228%' or feature_name like '%20201224%')
 --and cast((json_extract((dr.data), '$.reward_amount') ) as bigint) is not null



 select dr.*
 from  quest.public.driver_rewards dr
  left join  quest."public".driver_milestones dm on dr.driver_id = dm.driver_id and dr.driver_milestones_id = dm.id
 -- qwest info
 left join quest."public".quest_iterations qi on dm.quest_iteration_id = qi.id
 where 1=1

 and date(dr.created_at) between date'2020-12-29' and date'2020-12-29'
 --and (feature_name like '%20201228%' or feature_name like '%20201224%')
 and cast((json_extract((dr.data), '$.reward_amount') ) as bigint) is null

select * from quest.public.