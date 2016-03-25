with
tmp_cfact
as (
select
    jobid,
    system,
    count(*) as njobs
from
    cluster_metrics_prod_2.container_fact
where
    system='dogfood'
    and date between '2015-12-01' and '2015-12-31'
group by
    jobid,
    system
)

,tmp_tfact
as (
select
    jobid,
    system,
    count(*) as njobs
from
    cluster_metrics_prod_2.task_attempt_fact
where
    system='dogfood'
    and date between '2015-12-01' and '2015-12-31' 
group by
    jobid,
    system
)

select
    cf.njobs as cf_jobs,
    tf.njobs as tf_jobs,
    cf.jobid as cf_jobid,
    tf.jobid as tf_jobid,
    cf.system as cf_system,
    tf.system as tf_system
from
    tmp_cfact as cf
full outer join
    tmp_tfact as tf
on
    cf.system = tf.system
    and cf.jobid = tf.jobid
