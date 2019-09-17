-- Extending container time series with queue and memory limits
-- as well as filling in missing minutes (minutes when nothing happened)

set hiveconf:cluster='firstdata';
set hiveconf:start_date='2016-10-01';
set hiveconf:end_date='2017-03-14';
set hiveconf:queue_dim=cluster_metrics_prod_2.queue_dim;
set hiveconf:resource_dim=cluster_metrics_prod_2.cluster_resource_dim;


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

    -- Job fact fields since robbed_jobs entires are often missing
    ,job_fact_reduced
    as (
    select
        *
    from
        cluster_metrics_prod_2.job_fact
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

    ,capacity_combined
    as(
    select
        queue_date,
        queue_system,
        queue_name,
        int(timestamp/1000/3600)*3600 as hour_start,
        int(timestamp/1000/60)*60 as minute_start,
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
        --hour_start as timestamp,
        minute_start as timestamp,

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
        --hour_start
        minute_start
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

        jfr.user_key,
        jfr.workflowid,
        jfr.workflowname,
        jfr.workflownodename,


        --jf.duration,
        --jf.wait_on_jobstart,
        --jf.num_containers robbed_num_containers,
        --jf.job_waittime,
        --jf.application,
        --jf.user_key,
        --jf.singlejob_max_capacity_memory_delayed,
        --jf.multijob_max_capacity_memory_delayed,
        --jf.userlimit_memory_delayed,
        --jf.elastic_unfariness_memory_delayed,
        --jf.competing_jobs_memory_delayed,
        --jf.singlejob_max_capacity_vcores_delayed,
        --jf.multijob_max_capacity_vcores_delayed,
        --jf.user_limit_vcores_delayed,
        --jf.elastic_unfariness_vcores_delayed,
        --jf.competing_jobs_vcores_delayed,
        --jf.optimal_mr_time,
    
        cc.capacity,
        cc.max_capacity,
        cc.cluster_memory_capacity/1000 as cluster_memory_capacity,
        cc.cluster_vcore_capacity,
        --cc.cluster_hdfs_capacity,
        cc.memory_capacity/1000 as memory_capacity,
        cc.memory_max_capacity/1000 as memory_max_capacity,
        cc.vcore_capacity,
        cc.vcore_max_capacity
    from
        container_time_series_filledmins_pre_mem_fix as jmu
    left outer join
        capacity_combined_avgd_hour as cc
    on
        --int(jmu.minute_start/3600)=int(cc.timestamp/3600)
        int(jmu.minute_start/60)=int(cc.timestamp/60)
        and jmu.system=cc.queue_system
        and jmu.queue=cc.queue_name
    --left outer join
    --    job_robbed_reduced as jf
    --on
    --    jf.job_id = jmu.job_id
    --    and jf.system = jmu.system
    left outer join
        job_fact_reduced jfr
    on
        jfr.jobid = jmu.job_id
        and jfr.system = jmu.system
    )

select * from final_table
--select count(*),queue_system,queue_date,queue_name from capacity_combined_avgd_hour group by queue_date, queue_system,queue_name
--select * from container_time_series_by_job
--select * from container_time_series_filledmins_pre_mem_fix
