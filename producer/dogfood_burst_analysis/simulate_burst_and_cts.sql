set hiveconf:start_date='2016-04-17';
set hiveconf:end_date='2016-04-24';
set hiveconf:queue_dim=cluster_metrics_prod_2.queue_dim;
set hiveconf:resource_dim=cluster_metrics_prod_2.cluster_resource_dim;
set hiveconf:system='';

with
    -- Need container_fact for waiting 'slothours' which can be used to estimate requested memory
    cf_reduced
    as (
    select
        *
    from
        cluster_metrics_prod_2.container_fact
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
        sum(cts.container_wait_time/60000*cf.memory) as memory_in_wait,
        cts.system,
        max(cts.cluster_memory) as cluster_memory,
        max(cts.cluster_vcores) as cluster_vcores,
        min(cts.date) as date
    from
        cluster_metrics_prod_2.container_time_series as cts
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


    -- All systems (needed to expand epochtminutes which are system independent)
    ,activesystems
    as (
    select
        distinct(system) as system
    from
        ctsgrouped
    )

    -- Systems and dates for current minute
    ,epochreduced
    as (
    select
        te.*,
        ast.system
    from
        dp_date_time.minute_start as te
    join
        activesystems as ast
    where
        date between ${hiveconf:start_date} and ${hiveconf:end_date}
        and ast.system like ${hiveconf:system}'%'
    )

    -- Fill in empty minutes in memory and vcores per minute table
    ,ctsfix
    as (
    select
        et.minute_start,
        if(container_wait_time is null,0,container_wait_time) as container_wait_time,
        if(memory is null,0,memory) as memory,
        if(vcores is null,0,vcores) as vcores,
        if(memory_in_wait is null,0,memory_in_wait) as memory_in_wait,
        if(cluster_memory is null,0,cluster_memory) as cluster_memory,
        if(cluster_vcores is null,0,cluster_vcores) as cluster_vcores,
        et.system,
        et.date
    from
        epochreduced as et
    left outer join
        ctsgrouped as cts
    on
        et.minute_start = cts.minute_start
        and et.system = cts.system
        and et.date = cts.date
    )

    -- Capacity and queue dims 
    ,capacity_combined
    as(
    select
        queue_date,
        queue_system,
        queue_name,
        floor(timestamp/3600/1000)*3600 as timestamp,
        avg(capacity) as capacity,
        avg(max_capacity) as max_capacity,
        avg(cluster_memory_capacity) as cluster_memory_capacity,
        avg(cluster_vcore_capacity) as cluster_vcore_capacity,
        avg(cluster_hdfs_capacity) as cluster_hdfs_capacity,

        avg(capacity)*avg(cluster_memory_capacity)/100 as memory_capacity,
        avg(max_capacity)*avg(cluster_memory_capacity)/100 as memory_max_capacity,
        floor(avg(capacity)*avg(cluster_vcore_capacity)/100) as vcore_capacity,
        avg(max_capacity)*avg(cluster_vcore_capacity)/100 as vcore_max_capacity
    from
        ${hiveconf:queue_dim} as qd
    left outer join
        ${hiveconf:resource_dim} as rd
    on
        cluster_system=queue_system
        and cluster_date=queue_date
        and cluster_date between ${hiveconf:start_date} and ${hiveconf:end_date}
    group by
        queue_date,
        queue_system,
        queue_name,
        timestamp
    )
    ,capacity_combined_avgd_hour
    as (
    select
        queue_date,
        queue_system,
        queue_name,
        timestamp,

        avg(capacity) as capacity,
        avg(max_capacity) as max_capacity,
        avg(cluster_memory_capacity) as cluster_memory_capacity,
        avg(cluster_vcore_capacity) as cluster_vcore_capacity,
        avg(cluster_hdfs_capacity) as cluster_hdfs_capacity,

        avg(memory_capacity) as memory_capacity,
        avg(memory_max_capacity) as memory_max_capacity,
        double(floor(avg(vcore_capacity))) as vcore_capacity,
        double(floor(avg(vcore_max_capacity))) as vcore_max_capacity
    from
        capacity_combined
    group by
        queue_date,
        queue_system,
        queue_name,
        timestamp
    )
    -- Removing the dim dependency...could avoid all this
    ,capacity_combined_avg_hour_cluster
    as (
    select
        queue_date as date,
        queue_system as system,
        timestamp,
        
        max(cluster_memory_capacity) as cluster_memory_capacity,
        max(cluster_vcore_capacity) as cluster_vcore_capacity,
        max(cluster_hdfs_capacity) as cluster_hdfs_capacity
    from
        capacity_combined_avgd_hour
    group by
        queue_date,
        queue_system,
        timestamp
    )

    -- Joining in cts with capacities
    ,cts_cc_patched
    as (
    select
        cts.*,
        cluster_memory_capacity,
        cluster_vcore_capacity,
        cluster_hdfs_capacity
    from
        ctsfix as cts
    left outer join
        capacity_combined_avg_hour_cluster as cc
    on
        cts.system=cc.system
        and floor(cts.minute_start/3600)=floor(cc.timestamp/3600)
    )

    -- Looking back 10min and 60min and checking how many of these 
    ,cts_with_avg_and_capacity 
    as (
    select
        cts.*,
        -- Slothour (2.5G) size for entire minute
        sum(if(cts.memory_in_wait>=2500*60,1,0)) over window_10 as shortcheck,

        sum(if(cts.memory_in_wait>=2500*60,1,0)) over window_60 as longcheck,
        
        -- Since memory is almost always small and there is always a little wait
        sum(if(cts.memory_in_wait<=2500*60 and
              cts.memory<0.75*cts.cluster_memory_capacity,1,0)) over window_10 as shortend,

        sum(if(cts.memory_in_wait<=2500*60 and
              cts.memory<0.75*cts.cluster_memory_capacity,1,0)) over window_60 as longend

        --cts.cluster_memory_capacity,
        --cts.cluster_vcore_capacity,
        --cts.cluster_hdfs_capacity
    from
        --ctsgrouped as cts
        cts_cc_patched as cts
    window window_60
        as (
            partition by cts.system
            order by cts.minute_start
            rows between 60 preceding and 0 following
        ),
    window_10
        as (
            partition by cts.system
            order by cts.minute_start
            rows between 10 preceding and 0 following
        )
    )

    -- Burst categories based on the 'bunsiness' of past 10 min or hour
    ,burst_switch
    as (
    select
        *,
        case
            when shortcheck>10 then
                'burst_shortbusy'
            when longcheck>60*0.7 then
                'burst_longbusy'
            when shortend>10 then
                'stop_burst_short'
            when longend>60*0.7 then
                'stop_burst_long'
            else
                'steady_state'
        end as burst_state
    from
        cts_with_avg_and_capacity
    )

    -- Need to find last minute that was bursted/unbursted
    -- Depending on whether unburst or burst is the closest to a certain minute,
    -- that minute will be marked accordingly
    ,cts_steady_state_fill
    as (
    select
        cts.*,  
        -- Should preferrably not have to bridgefix at all
        sum(if(cts.burst_state='burst_shortbusy',1,0)) over (
            partition by cts.system
            order by cts.minute_start
            rows between 10 preceding and 0 following
            ) as bridge,

        -- Widnow size should be as large as possible 
        max(if(cts.burst_state='burst_shortbusy' or cts.burst_state='burst_longbusy',minute_start,0)) over window_60 as last_burst_minute,
        max(if(cts.burst_state='stop_burst_short' or cts.burst_state='stop_burst_long',minute_start,0)) over window_60 as last_unburst_minute

    from
        burst_switch as cts
    window window_60
        as (
            partition by cts.system
            order by cts.minute_start
            rows between 60 preceding and 0 following
        )
    )

    ,marked_for_burst
    as (
    select
        *,
        case
            -- Reached a steady-state
            when burst_state='steady_state' and last_burst_minute>last_unburst_minute then
                'stay_burst'
            when burst_state='steady_state' and last_burst_minute<last_unburst_minute then
                'stay_unburst'
            when burst_state='steady_state' and last_burst_minute=last_unburst_minute and last_burst_minute!=0 then
                'burst_clash'
            when burst_state='steady_state' and last_burst_minute=0 then
                'unknown'
            when burst_state='steady_state' and bridge>0 then
                'bridged'
            else
                burst_state
        end as burst_complete
    from
        cts_steady_state_fill
    )


select * from marked_for_burst;
--select * from ctsfix

--select * from epochreduced;

--select * from capacity_combined_avg_hour_cluster
