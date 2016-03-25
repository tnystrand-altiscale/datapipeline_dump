-- Extending container time series with queue and memory limits
-- as well as filling in missing minutes (minutes when nothing happened)

set hiveconf:cluster='dogfood';
set hiveconf:start_date='2016-03-10';
set hiveconf:end_date='2016-03-15';


with
    -- Select the desired interval from container_time_series
    container_time_series_reduced
    as (
    select
        *
    from
        cluster_metrics_prod_2.container_time_series
    where
        date between ${hiveconf:start_date} and ${hiveconf:end_date}
        --and system = ${hiveconf:cluster}
    )

    -- Job fact fields since robbed_jobs entires are often missing
    ,job_fact_reduced
    as (
    select
        *
    from
        cluster_metrics_prod_2.job_fact
    where
        date between ${hiveconf:start_date} and ${hiveconf:end_date}
        --and system = ${hiveconf:cluster}
    )

    -- Select interval from contaier_fact for container_size
    ,container_fact_reduced
    as (
    select
        *
    from
        cluster_metrics_prod_2.container_fact
    where
        date between ${hiveconf:start_date} and ${hiveconf:end_date}
        --and system = ${hiveconf:cluster}
    )


    -- All systems (needed to expand epochtminutes which are system independent)
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

    ,container_time_series_by_job
    as (
    select
        cts.minute_start,
        cts.job_id,
        count(*) as num_containers,
        sum(if(cts.container_wait_time>0,1,0)) as num_waiting_containers,
        sum(cts.container_wait_time) as container_wait_time,
        sum(cts.container_wait_time*cf.memory) as container_wait_spacetime,
        sum(cts.container_wait_time/60000*cf.memory) as container_wait_memory_v1,
        sum(if(cts.state='REQUESTED',cf.memory,0)) as container_wait_memory_v2,
        max(cts.memory) as max_container_size,
        sum(cts.memory) as memory,
        sum(if(cts.state='RESERVED',0,cts.memory)) as memory_noreserved,
        sum(if(cts.memory>0,cts.vcores,0)) as vcores,
        min(cts.queue) as queue,
        max(cts.cluster_memory) as cluster_memory,
        max(cts.cluster_vcores) as cluster_vcores,
        min(cts.user_key) as user_key,
        min(cts.date) as date,
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
            ) as job_finishtime,
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
        cts.system,
        cts.job_id
    )

    ,container_time_series_filledmins_pre_mem_fix
    as (
    select
        er.minute_start,
        er.system,
        er.queue,
        er.date,
        if(cts.minute_start is null,'-',job_id) as job_id,
        if(cts.minute_start is null,0,num_containers) as num_containers,
        if(cts.minute_start is null,0,num_waiting_containers) as num_waiting_containers,
        if(cts.minute_start is null,0,container_wait_time) as container_wait_time,
        if(cts.minute_start is null,0,container_wait_spacetime) as container_wait_spacetime,
        if(cts.minute_start is null,0,container_wait_memory_v1) as container_wait_memory_v1,
        if(cts.minute_start is null,0,container_wait_memory_v2) as container_wait_memory_v2,
        if(cts.minute_start is null,0,memory) as memory,
        if(cts.minute_start is null,0,max_container_size) as max_container_size,
        if(cts.minute_start is null,0,memory_noreserved) as memory_noreserved,
        if(cts.minute_start is null,0,vcores) as vcores,
        if(cts.minute_start is null,null,job_starttime) as job_starttime,
        if(cts.minute_start is null,null,job_finishtime) as job_finishtime,
        if(cts.minute_start is null,null,cluster_memory) as cluster_memory,
        if(cts.minute_start is null,null,cluster_vcores) as cluster_vcores,
        if(cts.minute_start is null,'-',user_key) as user_key
    from
        epochreduced as er
    left outer join
        container_time_series_by_job as cts
    on
        er.system=cts.system
        and er.queue=cts.queue
        and er.minute_start=cts.minute_start
    )

    ,final_table
    as (
    select
        jmu.minute_start,
        jmu.system,
        jmu.queue,
        jmu.date,
        jmu.job_id,
        jmu.num_containers,
        jmu.num_waiting_containers,
        jmu.container_wait_time/3600/1000 as container_wait_time,
        jmu.container_wait_memory_v1/1000 as container_wait_memory_v1,
        jmu.container_wait_memory_v2/1000 as container_wait_memory_v2,
        jmu.container_wait_spacetime/1000/3600/1000 as container_wait_spacetime,
        jmu.memory/1000 as memory,
        jmu.max_container_size/1000 as max_container_size,
        jmu.memory_noreserved/1000 as memory_noreserved,
        jmu.vcores,
        jmu.cluster_memory/1000 as cluster_memory,
        jmu.cluster_vcores,
        jmu.job_starttime,
        jmu.job_finishtime,
        (jmu.job_finishtime - jmu.job_starttime)/3600/1000 as job_duration,
        --jmu.cluster_memory_patched/1000 as cluster_memory_patched,
   
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

        jfr.user_key
    from
        container_time_series_filledmins_pre_mem_fix as jmu
    left outer join
        job_fact_reduced jfr
    on
        jfr.jobid = jmu.job_id
        and jfr.system = jmu.system
    )

select * from final_table
