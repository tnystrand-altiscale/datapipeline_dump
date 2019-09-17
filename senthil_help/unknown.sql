select
    *
from 
    dp_views.job_conv 
where 
    job_date >= '2017-06-01' 
    and app == 'unknown'
