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
        date between ${hiveconf:start_date} and ${hiveconf:end_date}
        --and system = ${hiveconf:cluster}
    )

    ,apps_in_taskattempt
    as (
    select
        jf.application,
        count(*) as num_tasks
    from
        task_attempt_reduced as ta
    join
        job_fact_reduced as jf
    on
        ta.jobid = jf.jobid
        and ta.system = jf.system
    group by
        jf.application
    )

select * from apps_in_taskattempt

