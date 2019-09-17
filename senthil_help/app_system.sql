select
    app, 
    count(*) as num_jobs, 
    count(distinct job_system) as num_customers
from 
    dp_views.job_conv 
where 
    job_date >='2016-07-01' 
group by 
    app
