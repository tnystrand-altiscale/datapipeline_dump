set hiveconf:cluster='dogfood';
set hiveconf:start_date='2016-02-01';
set hiveconf:end_date='2016-02-09';

with
    container_fact_reduced
    as (
    select
        *,
        if(requestedtime>0,requestedtime,null) as container_borntime,
        if(allocatedtime>0,allocatedtime,null) as container_starttime,
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
        end as container_finishtime
    from
        cluster_metrics_prod_2.container_fact
    where
        system = ${hiveconf:cluster}
        and date between ${hiveconf:start_date} and ${hiveconf:end_date}
    )


    ,job_fact_reduced
    as (
    select
        *,
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
            ) as application
    from
        cluster_metrics_prod_2.job_fact
    where
        system = ${hiveconf:cluster}
        and date between ${hiveconf:start_date} and ${hiveconf:end_date}
    )

    ,times_by_map_reducer
    as (
    select
        jobid,
        priority,
        system,
        min(container_borntime) as type_borntime,
        min(container_starttime) as type_starttime,
        max(container_finishtime) as type_finishtime,
        avg(memory) as memory
    from
        container_fact_reduced
    group by
        priority,
        jobid,
        system
    )

    ,optimal_times
    as (
    select
        jobid,
        system,
        count(*) as num_diff_priorities,
        min(type_borntime) as job_borntime,
        min(type_starttime) as job_waketime,
        max(type_finishtime) as job_deathtime,

        sum(if(priority > 0 and type_finishtime is not null and type_starttime is not null,
            type_finishtime - type_starttime, null)) as optimal_execution_time,

        max(type_finishtime - type_borntime) as job_duration_time,
        max(type_finishtime - type_starttime) as job_execution_time,

        max(if(priority == 20 and type_finishtime is not null and type_starttime is not null,
            type_finishtime - type_starttime, null)) as longest_map_execution_time,
        max(if(priority == 10 and type_finishtime is not null and type_starttime is not null,
            type_finishtime - type_starttime, null)) as longest_reducer_execution_time,

        max(if(priority == 20 and type_finishtime is not null and type_borntime is not null,
            type_finishtime - type_borntime, null)) as longest_map_duration_time,
        max(if(priority == 10 and type_finishtime is not null and type_borntime is not null,
            type_finishtime - type_borntime, null)) as longest_reducer_duration_time,

        max(if(priority == 0, memory, null)) as app_master_size,
        max(if(priority == 10, memory, null)) as reducer_size,
        max(if(priority == 20, memory, null)) as mapper_size,

        collect_set(priority) as all_priorities
    from
        times_by_map_reducer 
    group by
        jobid,
        system
    )

    ,optimal_time_extended
    as (
    select
        opt.*,
        jf.application
    from
        optimal_times as opt
    join
        job_fact_reduced as jf
    on
        opt.jobid = jf.jobid
        and opt.system = jf.system
    )


select * from optimal_time_extended
--select * from times_by_map_reducer

