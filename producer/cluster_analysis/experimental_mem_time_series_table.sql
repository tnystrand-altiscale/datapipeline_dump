-- Extending container time series with queue and memory limits
-- as well as filling in missing minutes (minutes when nothing happened)

set hiveconf:cluster=;
set hiveconf:start_date='2016-04-15';
set hiveconf:end_date='2016-04-25';
set hiveconf:queue_dim=cluster_metrics_prod_2.queue_dim;
set hiveconf:resource_dim=cluster_metrics_prod_2.cluster_resource_dim;
set hiveconf:container_fact=cluster_metrics_prod_2.container_fact;
set hiveconf:container_time_series=cluster_metrics_prod_2.container_time_series;
set hiveconf:job_fact=cluster_metrics_prod_2.job_fact;


set hiveconf:target_table=dp_derived.time_series_job_granularity;

-- To drop table on each run
-- drop table if exists ${hiveconf:target_table};

-- Drop table command
-- hive -e "drop table dp_derived.time_series_job_granularity"
-- hive -e "select * from dp_derived.time_series_job_granularity limit 20"

create table if not exists ${hiveconf:target_table}
    (
        -- Container time series related
        minute_start Int,
        job_id String,

        queue String,
        user_key String,

        memory_allocated_withreserved Double,
        memory_allocated Double,

        running_containers Int,
        waiting_containers Int,

        container_wait_time_sum Double,

        waiting_memory_sum Double,

        vcores_allocated_withreserved Int,
        vcores_allocated Int,

        cluster_memory_fromcts Double,
        cluster_vcores_fromcts Double,

        -- Per job stats from container_fact
        job_starttime Int,
        job_finishtime Int,
        job_duration Double,

        -- Capacities from metrics
        normal_queue_capacity Double,
        max_queue_capacity Double,
        total_memory_capacity Double,
        total_vcore_capacity Int,
        normal_memory_queue_capacity Double,
        max_memory_capacity_capacity Double,
        normal_vcore_queue_capacity Double,
        max_vcore_queue_capacity Double,
    
        -- Dimensions from job_fact
        jobname String,
        job_endstatus String,
        --hdfs_bytes_read_GB Double,

        application String,

        workflowid String,
        workflowname String,
        workflownodename String
    )
partitioned by
    (
        system  String,
        date    String
    )
stored as
    orc;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

with
    -- Select the desired interval from container_time_series
    container_time_series_reduced
    as (
    select
        *
    from
        ${hiveconf:container_time_series}
    where
        system like '%${hiveconf:cluster}%'
        and date between ${hiveconf:start_date} and ${hiveconf:end_date}
    )

    -- Job fact fields since robbed_jobs entires are often missing
    ,job_fact_reduced
    as (
    select
        *
    from
        ${hiveconf:job_fact}
    where
        system like '%${hiveconf:cluster}%'
        and date between ${hiveconf:start_date} and ${hiveconf:end_date}
    )

    -- Select interval from contaier_fact for container_size
    ,container_fact_reduced
    as (
    select
        *
    from
        ${hiveconf:container_fact}
    where
        system like '%${hiveconf:cluster}%'
        and date between ${hiveconf:start_date} and ${hiveconf:end_date}
    )


    -- All systems (needed to expand epochtminutes which are system independent) and queues
    ,activesystems
    as (
    select
        system,
        queue
    from
        container_time_series_reduced
    where 
        queue is not null
    group by
        system,
        queue
    )

    -- Combine the active systems and queues with epochreduced
    ,epochreduced
    as (
    select
        te.*,
        ast.system,
        ast.queue
    from
        dp_date_time.minute_start as te
    full outer join
        activesystems as ast
    where
        te.date between ${hiveconf:start_date} and ${hiveconf:end_date}
    )


    -- Group container_time_series by job
    ,container_time_series_by_job
    as (
    select
        -- Primary key
        cts.minute_start,
        cts.job_id,
        cts.system,

        -- Dimensions
        min(cts.queue) as                          queue,
        min(cts.user_key) as                       user_key,
        min(cts.date) as                           date,

        -- Running memory
        sum(cts.memory) as                              memory_allocated_withreserved,
        sum(if(cts.state='RESERVED',0,cts.memory)) as   memory_allocated,

        sum(if(cts.state='RUNNING',1,0)) as             running_containers,
        sum(if(cts.container_wait_time>0,1,0)) as       waiting_containers,
        sum(cts.container_wait_time) as                 waiting_time_container_sum,
        -- This is an approximation. Memory is the actual waiting memory,
        -- but this is 'smeared' over the minute 
        sum(cts.container_wait_time*cf.memory) as       waiting_memory,
        
        sum(if(cts.memory>0,cts.vcores,0)) as           vcores_allocated_withreserved,
        sum(if(cts.state='RESERVED',0,cts.vcores)) as   vcores_allocated,

        -- These should be treated with care
        -- cluster_memory/vcores is still spaced out over container_life_time
        -- These can also be 0 for reserved containers
        max(cts.cluster_memory) as                      cluster_memory,
        max(cts.cluster_vcores) as                      cluster_vcores,

        -- From container_fact
        -- This will indeed be the starttime and endtime since
        -- the app master runs at every minute step
        min(if(allocatedtime>0,allocatedtime,null)) as job_starttime,
        max(
            case
                when completedtime>0 then
                    completedtime
                when releasedtime>0 then
                    releasedtime
                when killedtime>0 then
                    killedtime
                when expiredtime>0 then
                    expiredtime
                else
                    null
            end
            ) as job_finishtime
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
        cts.system,
        cts.job_id
    )

    -- Pad container time series with epoch times to remove 'holes' in data
    ,container_time_series_filledmins_pre_mem_fix
    as (
    select
        er.minute_start,
        if(cts.minute_start is null,'-',job_id) as job_id,
        er.system,

        er.queue,
        if(cts.minute_start is null,'-',user_key) as user_key,
        er.date,


        if(cts.minute_start is null,0,memory_allocated_withreserved) as memory_allocated_withreserved,
        if(cts.minute_start is null,0,memory_allocated) as              memory_allocated,

        if(cts.minute_start is null,0,running_containers) as            running_containers,
        if(cts.minute_start is null,0,waiting_containers) as            waiting_containers,

        if(cts.minute_start is null,0,waiting_time_container_sum) as    waiting_time_container_sum,

        if(cts.minute_start is null,0,waiting_memory) as                waiting_memory,

        if(cts.minute_start is null,0,vcores_allocated_withreserved) as vcores_allocated_withreserved,
        if(cts.minute_start is null,0,vcores_allocated) as              vcores_allocated,

        if(cts.minute_start is null,null,cluster_memory) as             cluster_memory,
        if(cts.minute_start is null,null,cluster_vcores) as             cluster_vcores,

        if(cts.minute_start is null,null,job_starttime) as              job_starttime,
        if(cts.minute_start is null,null,job_finishtime) as             job_finishtime
    from
        epochreduced as er
    left outer join
        container_time_series_by_job as cts
    on
        er.system=cts.system
        and er.queue=cts.queue
        and er.minute_start=cts.minute_start
    )

    -- Combining queue dims with resourcedims
    ,capacity_combined
    as(
    select
        queue_date,
        queue_system,
        queue_name,

        int(timestamp/1000) as timestamp,
        int(timestamp/1000/60)*60 as minute_start,
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
        and cluster_date between ${hiveconf:start_date} and ${hiveconf:end_date}
    )

    -- Join the epoch time series padded container time series with
    -- queue and cluster resource dim info
    ,final_table
    as (
    select
        jmu.minute_start,
        jmu.job_id,

        jmu.queue,
        jfr.user_key,

        -- Memory in GBs
        jmu.memory_allocated_withreserved/1000 as memory_allocated_withreserved,
        jmu.memory_allocated/1000 as memory_allocated,

        jmu.running_containers,
        jmu.waiting_containers,

        -- Container wait time in hours
        jmu.waiting_time_container_sum/3600/1000 as container_wait_time_sum,

        -- Wainting cotainer memory in GB
        jmu.waiting_memory/1000 as waiting_memory_sum,

        -- Vcores
        jmu.vcores_allocated_withreserved/1000 as vcores_allocated_withreserved,
        jmu.vcores_allocated/1000 as vcores_allocated,

        jmu.cluster_memory/1000 as cluster_memory_fromcts,
        jmu.cluster_vcores/1000 as cluster_vcores_fromcts,

        -- In epoch milliseconds
        jmu.job_starttime,
        jmu.job_finishtime,
        -- In hours
        (jmu.job_finishtime - jmu.job_starttime)/3600/1000 as job_duration,
   
        -- Cluster limits and capacities
        cc.capacity as normal_queue_capacity,
        cc.max_capacity as max_queue_capcity,
        cc.cluster_memory_capacity/1000 as total_memory_capacity,
        cc.cluster_vcore_capacity as total_vcore_capacity,
        cc.memory_capacity/1000 as normal_memory_queue_capacity,
        cc.memory_max_capacity/1000 as max_memory_queue_capacity,
        cc.vcore_capacity as normal_vcore_queue_capacity,
        cc.vcore_max_capacity as max_vcore_queue_capacity,


        -- Dimensions from job_fact
        -- These are used to find reptetive jobs
        jfr.jobname,
        jfr.jobstatus,
        --jfr.totalcounters['HDFS_BYTES_READ']/1000000000 as hdfs_bytes_read_GB,

        (CASE when app = 'scalding' THEN 'scalding' --scalding detected by processing XML conf. in rm-jhist
                    when app = 'cascalog' THEN 'cascalog' --cascalog detected by processing XML conf. in rm-jhist
                    when jobname RLIKE "^[sS][eE][lL][eE][cC][tT] " THEN "hive_map_reduce"
                    when jobname RLIKE "^[cC][rR][eE][aA][tT][eE] " THEN "hive_map_reduce"
                    when jobname RLIKE "^[iI][nN][sS][eE][rR][tT] " THEN "hive_map_reduce"
                    when jobname RLIKE "\(Stage-[0-9]\)" THEN "hive_map_reduce"
                    when jobname LIKE "HIVE-%" THEN "hive_tez"
                    when jobname LIKE "H2O%" THEN "H2O"
                    when jobname LIKE "PigLatin%" THEN "pig"
                    when jobname = "distcp" THEN "distcp"
                    when jobname RLIKE "^[Ss]park" THEN "spark"
                    when jobname RLIKE "^[Pp]y[Ss]park" THEN "pyspark"
                    when jobname LIKE "MSMP%" THEN "marketshare_mp"
                    when jobname LIKE "streamjob%" THEN "streaming_map_reduce"
                    when jobname LIKE 'vmc-camus.jar%' THEN 'camus'
                    when jobname LIKE 'Camus Job%' THEN 'camus'
                    when jobname LIKE 'oozie:launcher%' THEN 'oozie_launcher'
                    when jobname RLIKE '^[Ss]qoop' THEN 'sqoop'
                    when jobname LIKE 'oozie:action:T=map-reduce%' THEN 'other_map_reduce'
                    when jobname LIKE 'oozie:action:T\\=map-reduce%' THEN 'other_map_reduce'
                    when jobname LIKE 'oozie:action:T=sqoop%' THEN 'sqoop'
                    when jobname LIKE 'oozie:action:T\\=sqoop%' THEN 'sqoop'
                    when jobname RLIKE '^\\[[A-F0-9]+/' THEN 'cascading'
                    when totalmaps IS NOT NULL THEN 'other_map_reduce'
                    ELSE 'unknown' END
            ) as application,

        jfr.workflowid,
        jfr.workflowname,
        jfr.workflownodename,
  
        -- Partitions in hive must appear last
        jmu.system,
        jmu.date
    from
        container_time_series_filledmins_pre_mem_fix as jmu
    -- container_time_series is copmlete due to epoch time series join
    left outer join
        capacity_combined as cc
    on
        jmu.minute_start=cc.minute_start
        and jmu.system=cc.queue_system
        and jmu.queue=cc.queue_name
    left outer join
        job_fact_reduced jfr
    on
        jfr.jobid = jmu.job_id
        and jfr.system = jmu.system
    )

insert overwrite table ${hiveconf:target_table}
partition(system, date)
select * from final_table
