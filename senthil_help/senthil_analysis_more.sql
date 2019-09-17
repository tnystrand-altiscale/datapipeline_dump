select
    app, 
    count(*) as num_jobs, 
    count(distinct job_system) as num_customers, 
    max(year(job_date)) as year, 
    max(month(job_date)) as month, 
    job_date as date,
    job_system as system
from 
    dp_views.job_conv 
where 
    job_date >='2016-07-01' 
group by 
    app, 
    job_date,
    job_system
