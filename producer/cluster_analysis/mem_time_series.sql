-- Extending container time series with queue and memory limits
-- as well as filling in missing minutes (minutes when nothing happened)

set hiveconf:cluster='dogfood';
set hiveconf:start_date='2016-01-01';
set hiveconf:end_date='2016-02-07';
set hiveconf:queue_dim=cluster_metrics_prod_2.queue_dim;
set hiveconf:resource_dim=cluster_metrics_prod_2.cluster_resource_dim;
--set hiveconf:resource_dim=eric_test.cluster_resource_hours;


with
    -- Select the desired interval from container_time_series
    container_time_series_reduced
    as (
    select
        *
    from
        cluster_metrics_prod_2.container_time_series
    where
        system = ${hiveconf:cluster}
        and date between ${hiveconf:start_date} and ${hiveconf:end_date}
    )

    -- Select desierd interval from job_fact
    ,job_robbed_reduced
    as (
    select
        *
    from
        cluster_metrics_prod_2.robbed_jobs_report
    where
        system = ${hiveconf:cluster}
        and date between ${hiveconf:start_date} and ${hiveconf:end_date}
    )

    -- Select interval from contaier_fact for container_size
    ,container_fact_reduced
    as (
    select
        *
    from
        cluster_metrics_prod_2.container_fact
    where
        system = ${hiveconf:cluster}
        and date between ${hiveconf:start_date} and ${hiveconf:end_date}
    )

        

    -- All systems (needed to expand epochtminutes which are system independent)
    ,activesystems
    as (
    select
        system
    from
        container_time_series_reduced
    group by
        system
    )

    ,epochreduced
    as (
    select
        te.*,
        ast.system
    from
        dp_date_time.minute_start as te
    full outer join
        activesystems as ast
    where
        te.date between ${hiveconf:start_date} and ${hiveconf:end_date}
    )

    ,container_time_series_by_min
    as (
    select
        cts.minute_start,
        count(*) as num_containers,
        sum(if(cts.container_wait_time>0,1,0)) as num_waiting_containers,
        sum(cts.container_wait_time) as container_wait_time,
        sum(cts.container_wait_time*cf.memory) as container_wait_spacetime,
        sum(cts.memory) as memory,
        sum(if(cts.memory>0,cts.vcores,0)) as vcores,
        max(cts.cluster_memory) as cluster_memory,
        min(cts.user_key) as user_key,
        min(cts.date) as date,
        cts.system
    from
        container_time_series_reduced as cts
    join
        container_fact_reduced as cf
    on
        cts.container_id = cf.containerid
        and cts.system = cf.system
        and cts.date = cf.date
    group by
        cts.minute_start,
        cts.system
    )

    ,container_time_series_filledmins
    as (
    select
        er.minute_start,
        er.system,
        er.date,
        if(cts.minute_start is null,0,num_containers) as num_containers,
        if(cts.minute_start is null,0,num_waiting_containers) as num_waiting_containers,
        if(cts.minute_start is null,0,container_wait_time) as container_wait_time,
        if(cts.minute_start is null,0,container_wait_spacetime) as container_wait_spacetime,
        if(cts.minute_start is null,0,memory) as memory,
        if(cts.minute_start is null,0,vcores) as vcores,
        if(cts.minute_start is null,0,cluster_memory) as cluster_memory,
        if(cts.minute_start is null,'-',user_key) as user_key
    from
        epochreduced as er
    left outer join
        container_time_series_by_min as cts
    on
        er.system=cts.system
        and er.minute_start=cts.minute_start
    )

    ,capacity_combined
    as(
    select
        queue_date,
        queue_system,
        queue_name,
        int(timestamp/1000/3600)*3600 as hour_start,
        capacity,
        max_capacity,

        cluster_memory_capacity,
        cluster_vcore_capacity,
        cluster_hdfs_capacity,

        capacity*cluster_memory_capacity/100 as memory_capacity,
        max_capacity*cluster_memory_capacity/100 as memory_max_capacity,
        float(capacity)*cluster_vcore_capacity/100 as vcore_capacity,
        float(max_capacity)*cluster_vcore_capacity/100 as vcore_max_capacity
    from
        ${hiveconf:queue_dim} as qd
        ,${hiveconf:resource_dim} as rd
    where
        cluster_system=queue_system
        and cluster_date=queue_date
        --and cluster_system = ${hiveconf:cluster}
        and cluster_date between ${hiveconf:start_date} and ${hiveconf:end_date}
    )
    ,capacity_combined_avgd_hour
    as (
    select
        queue_date,
        queue_system,
        queue_name,
        hour_start as timestamp,

        avg(capacity) as capacity,
        avg(max_capacity) as max_capacity,
        avg(cluster_memory_capacity) as cluster_memory_capacity,
        avg(cluster_vcore_capacity) as cluster_vcore_capacity,
        --avg(cluster_hdfs_capacity) as cluster_hdfs_capacity,

        avg(memory_capacity) as memory_capacity,
        avg(memory_max_capacity) as memory_max_capacity,
        avg(vcore_capacity) as vcore_capacity,
        avg(vcore_max_capacity) as vcore_max_capacity
    from
        capacity_combined
    group by
        queue_date,
        queue_system,
        queue_name,
        hour_start
    )

select
    jmu.minute_start,
    jmu.system,
    jmu.date,
    jmu.num_containers,
    jmu.num_waiting_containers,
    jmu.container_wait_time/3600/1000 as container_wait_time,
    jmu.container_wait_spacetime/1000/3600/1000 as container_wait_spacetime,
    jmu.memory/1000 as memory,
    jmu.vcores,
    jmu.cluster_memory/1000 as cluster_memory
from
    container_time_series_filledmins as jmu
