ADD JAR ../udf_altiscale/target/HiveUDF-1.0-jar-with-dependencies.jar;
create temporary function sha1 as 'com.altiscale.pipeline.hive.udf.Sha1';

set hiveconf:start_date='2015-06-01';
set hiveconf:end_date='2016-02-01';
set hiveconf:system_dog='dogfood';
set hiveconf:system_jum='jumbo';
set hiveconf:system_jun='jungledata';

use thomas_tostratos;

drop table if exists stratos_1;

create table stratos_1
stored as orc
as
with
    job_fact_reduced
    as (
    select
        *
    from
        cluster_metrics_prod_2.job_fact
    where
        date between ${hiveconf:start_date} and ${hiveconf:end_date}
        and system != ${hiveconf:system_dog}
        and system != ${hiveconf:system_jun}
        and system != ${hiveconf:system_jum}
    )

    ,num_wait_perminute
    as (
    SELECT
        minute_start,
        container_system,
        job_id,
        container_state,
        container_host,
        count(container_wait_time) as containers_number,
        measure_date
    FROM
        dp_views.container_time_series_fact
    WHERE
        partition_date between ${hiveconf:start_date} and ${hiveconf:end_date}
        and container_system != ${hiveconf:system_dog}
        and container_system != ${hiveconf:system_jun}
        and container_system != ${hiveconf:system_jum}
        and container_wait_time >= 10
    GROUP BY
        minute_start, container_system, job_id, container_state, container_host, measure_date

    UNION ALL

    SELECT
        minute_start,
        container_system,
        job_id,
        container_state,
        container_host,
        count(container_wait_time) as containers_number,
        measure_date
    FROM
        dp_views.container_time_series_fact
    WHERE
        partition_date between ${hiveconf:start_date} and ${hiveconf:end_date}
        and container_system != ${hiveconf:system_dog}
        and container_system != ${hiveconf:system_jun}
        and container_system != ${hiveconf:system_jum}
        and container_state != 'EXPIRED'
    GROUP BY
        minute_start, container_system, job_id, container_state, container_host, measure_date
    ) 

    ,num_wait_perminute_with_app
    as (
    select
        nwp.minute_start,
        nwp.job_id,
        nwp.container_state,
        nwp.containers_number,
        nwp.container_host,
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
        sha1(container_system) as container_system,
        nwp.measure_date
    from
        num_wait_perminute as nwp
    join
        job_fact_reduced as jf
    on
        nwp.container_system = jf.system
        and nwp.job_id = jf.jobid
    )

select * from num_wait_perminute_with_app order by minute_start



