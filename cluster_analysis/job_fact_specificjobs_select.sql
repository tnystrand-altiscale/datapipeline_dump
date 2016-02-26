set hiveconf:START_DATE='2015-12-01';
set hiveconf:END_DATE='2015-12-31';
set hiveconf:SYSTEM='iheartradio';

select
    *
from 
    cluster_metrics_prod_2.job_fact
where
    system=${hiveconf:SYSTEM}
    and date between ${hiveconf:START_DATE} and ${hiveconf:END_DATE}
    and (jobid='job_1446505469556_101984' or jobid='job_1446505469556_106189' or jobid='job_1446505469556_107493')
