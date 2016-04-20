set hiveconf:start_date='2016-04-01';
set hiveconf:end_date='2016-04-16';
set hiveconf:system='';
set hiveconf:cts=cluster_metrics_prod_2.container_time_series;
set hiveconf:cf=cluster_metrics_prod_2.container_fact;
set hiveconf:crd=cluster_metrics_prod_2.cluster_resource_dim;

-- Cannot trust the edges of time intervals due to partition and time mismatch

with
    -- Make sure to only select minutes bigger than the actual partition date (avoid half-days)
    crd_reduced
    as (
    select
        *
    from
        ${hiveconf:crd}
    where
       cluster_date between ${hiveconf:start_date} and ${hiveconf:end_date}
       and cluster_system like ${hiveconf:system}'%'
       and timestamp >= unix_timestamp( concat(${hiveconf:start_date}, ' 00:00:00') )*1000
    )

    -- Cannot fix the end tail (less containers will appear here, since these are still 'to-be' processed)
    -- Make sure to only select minutes bigger than the actual partition date (avoid half-days)
    -- Need this check here despite join since both crd and cts might miss the first minute of the partition date
    ,cts_reduced
    as (
    select
        *
    from
        ${hiveconf:cts}
    where
       date between ${hiveconf:start_date} and ${hiveconf:end_date}
       and system like ${hiveconf:system}'%'
       and minute_start >= unix_timestamp( concat(${hiveconf:start_date}, ' 00:00:00') )
    )

    -- Need container_fact for waiting 'slothours' which can be used to estimate requested memory
    -- Ignore minute_start - first partition date problems since we will join cf and cts anyways
    ,cf_reduced
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
    -- Join on cluster and date (partition date is dangerous, but assume to mismatch between container_fact and container_time_series
    -- is very small)
    ,ctsgrouped
    as (
    select 
        cts.minute_start,
        sum(cts.memory) as memory,
        sum(if(cts.memory>0,cts.vcores,0)) as vcores,
        sum(cts.container_wait_time) as container_wait_time,
        sum(cts.container_wait_time/60000*cf.memory/1000) as memory_in_wait,
        sum( if( cts.queue = 'production', cts.container_wait_time/60000*cf.memory/1000, 0) ) as production_memory_in_wait,
        sum( if( cts.state != 'RUNNING', 1, 0 ) ) as num_running_containers,
        sum( if( cts.state = 'REQUESTED', 1, 0 ) ) as num_waiting_containers, 
        cts.system,
        -- Do Not use these two for now. Gives misleading values
        --max(cts.cluster_memory) as cluster_memory, -- Use max here since reserved containers can have 0 cluster_memory
        --max(cts.cluster_vcores) as cluster_vcores, -- Use max here since reserved containers can have 0 cluster_vcores
        min(cts.date) as date
    from
        cts_reduced as cts
    join
        cf_reduced as cf
    on
        cf.containerid = cts.container_id
        and cf.system = cts.system
        and cf.date = cts.date
    group by
        cts.system,
        cts.minute_start
    )

    -- Cannot use cluster_memory and cluster_vcores from container_time_series
    -- Since these give inaccurate values for burst peridos
    -- Full outer join since there are no gurantees that timestamp and minute_start are the same
    ,cts_mem_extended
    as (
    select
        -- Every row should have a minute_start
        if( cts.minute_start is null, int(cdr.timestamp/1000), cts.minute_start) as minute_start,

        -- If any is null, it means container_time_series has no entry and should have 0's
        if( cts.memory is null, 0, cts.memory/1000 ) as memory,
        if( cts.vcores is null, 0, cts.vcores ) as vcores,
        if( cts.container_wait_time is null, 0, cts.container_wait_time ) as container_wait_time,
        if( cts.memory_in_wait is null, 0, cts.memory_in_wait ) as memory_in_wait,
        if( cts.production_memory_in_wait is null, 0, cts.production_memory_in_wait ) as production_memory_in_wait,
        if( cts.num_running_containers is null, 0, cts.num_running_containers ) as num_running_containers,
        if( cts.num_waiting_containers is null, 0, cts.num_waiting_containers ) as num_waiting_containers,

        -- Null value for these values means missing value and should be handled downstream
        cdr.cluster_memory_capacity/1000 as cluster_memory_capacity,
        cdr.cluster_vcore_capacity/1000 as cluster_vcore_capacity,

        -- Every row should have a date and system
        if( cts.system is null, cdr.cluster_system, cts.system) as system,
        if( cts.date is null, cdr.cluster_date, cast(cts.date as string) ) as date
    from
         ctsgrouped as cts
    full outer join
        crd_reduced as cdr
    on
        int(cdr.timestamp) = int(cts.minute_start*1000)
        and cdr.cluster_system = cts.system
    )

select * from cts_mem_extended order by system, minute_start
