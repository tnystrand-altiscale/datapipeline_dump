set hiveconf:cluster='dogfood';
set hiveconf:start_date='2015-12-01';
set hiveconf:end_date='2016-01-10';

with
    task_attempt_reduced
    as (
    select
        *
    from
        cluster_metrics_prod_2.task_attempt_fact
    where
        date between ${hiveconf:start_date} and ${hiveconf:end_date}
        --and system = ${hiveconf:cluster}
    )

    ,apps_in_taskattempt
    as (
    select
        tasktype,
        count(*) as num_tasks
    from
        task_attempt_reduced as ta
    group by
        tasktype
    )

select * from apps_in_taskattempt

