select
    app, 
    count(*) as num_jobs, 
    count(distinct job_system) as num_customers, 
    year(job_date) as year, 
    month(job_date) as month, 
    weekofyear(job_date) as week
from 
    dp_views.job_conv 
where 
    job_date >='2016-07-01' 
group by 
    app, 
    year(job_date), 
    month(job_date), 
    weekofyear(job_date)
