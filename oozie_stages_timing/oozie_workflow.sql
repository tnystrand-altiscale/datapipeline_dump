select 
    id as workflow_id, 
    app_name as workflow_name,
    substring(parent_id, 1, 36) as coordinator_id, 
    substring(parent_id, 38, 2) as action,
    run,
    user_name, 
    timestampdiff(SECOND, start_time, end_time)/3600 as duration, 
    created_time, 
    start_time, 
    end_time, 
    status,
    1 as count
from 
    oozie.WF_JOBS 
where
    start_time is not NULL
order by 
    start_time;
