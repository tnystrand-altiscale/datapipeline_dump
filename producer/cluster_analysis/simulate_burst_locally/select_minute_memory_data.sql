set hiveconf:start_date='2016-03-15';
set hiveconf:end_date='2016-03-31';
set hiveconf:system='dogfood';
set hiveconf:cts=cluster_metrics_prod_2.container_time_series;
set hiveconf:cf=cluster_metrics_prod_2.container_fact;

with
    -- Need container_fact for waiting 'slothours' which can be used to estimate requested memory
    cf_reduced
    as (
    select
        *
    from
        ${hiveconf:cf}
    where
       date between ${hiveconf:start_date} and ${hiveconf:end_date}
       and system like ${hiveconf:system}'%'
    )

    -- Memory and vcores per minute and system
    ,ctsgrouped
    as (
    select 
        cts.minute_start,
        sum(cts.container_wait_time) as container_wait_time,
        sum(cts.memory) as memory,
        sum(if(cts.memory>0,cts.vcores,0)) as vcores,
        sum(cts.container_wait_time/60000*cf.memory/1000) as memory_in_wait,
        cts.system,
        max(cts.cluster_memory) as cluster_memory, -- Use max here since reserved containers can have 0 cluster_memory
        max(cts.cluster_vcores) as cluster_vcores, -- Use max here since reserved containers can have 0 cluster_vcores
        min(cts.date) as date
    from
        ${hiveconf:cts} as cts
    join
        cf_reduced as cf
    on
        cf.containerid = cts.container_id
        and cf.system = cts.system
        and cf.date = cts.date
    where
       cf.date between ${hiveconf:start_date} and ${hiveconf:end_date}
       and cf.system like ${hiveconf:system}'%'
       and cts.date between ${hiveconf:start_date} and ${hiveconf:end_date}
       and cts.system like ${hiveconf:system}'%'
    group by
        cts.system,
        cts.minute_start
    )

select * from ctsgrouped order by system, minute_start
