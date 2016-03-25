ADD JAR ../../udf_altiscale/target/HiveUDF-1.0-jar-with-dependencies.jar;
create temporary function sha1 as 'com.altiscale.pipeline.hive.udf.Sha1';

set hiveconf:start_date='2015-06-01';
set hiveconf:end_date='2016-02-01';
set hiveconf:system_dog='dogfood';
set hiveconf:system_jum='jumbo';
set hiveconf:system_jun='jungledata';

use thomas_tostratos;

drop table if exists stratos_2a_perjob;

create table stratos_2a_perjob
stored as orc
as
with
    job_fact_reduced
    as (
        select
            *,
            totalcounters['FILE_BYTES_READ'] as FILE_BYTES_READ,
            totalcounters['FILE_BYTES_WRITTEN'] as FILE_BYTES_WRITTEN,
            totalcounters['HDFS_BYTES_READ'] as HDFS_BYTES_READ,
            totalcounters['HDFS_BYTES_WRITTEN'] as HDFS_BYTES_WRITTEN
        from
            cluster_metrics_prod_2.job_fact
        where
            date between ${hiveconf:start_date} and ${hiveconf:end_date}
            and system != ${hiveconf:system_dog}
            and system != ${hiveconf:system_jun}
            and system != ${hiveconf:system_jum}
    )

    ,container_fact_reduced
    as (
    select
        *,
        -- Waiting time. (Excluding scheduling time)
        case when allocatedtime>0 and requestedtime>0 then
            (allocatedtime-requestedtime)/1000
        else
            0
        end as waitingtime,
        -- Running time
        case
            when completedtime>0 and acquiredtime>0 then
                (completedtime-acquiredtime)/1000
            when releasedtime>0 and acquiredtime>0 then
                (releasedtime-acquiredtime)/1000
            when killedtime>0 and acquiredtime>0 then
                (killedtime-acquiredtime)/1000
            when expiredtime>0 and acquiredtime>0 then
                (expiredtime-acquiredtime)/1000
            else
                0
        end as full_runningtime,

        -- The max end time
        case
            when completedtime>0  then
                completedtime/1000
            when releasedtime>0  then
                releasedtime/1000
            when killedtime>0  then
                killedtime/1000
            when expiredtime>0 then
                expiredtime
            else
                null
        end as finishtime,

        if(requestedtime>0,requestedtime/1000,NULL) as starttime
    from
        cluster_metrics_prod_2.container_fact
    where
        date between ${hiveconf:start_date} and ${hiveconf:end_date}
        and system != ${hiveconf:system_dog}
        and system != ${hiveconf:system_jun}
        and system != ${hiveconf:system_jum}
    )


    ,mem_and_vcore_per_job
    as (
    select
        jobid as job_id,
        count(*) as number_of_containers,
        sum(memory) as requested_memory,
        sum(vcores) as requested_vcores,
        system,
        max(finishtime)-min(starttime) as job_duration,
        min(starttime) as starttime,
        max(finishtime) as finishtime
    from
        container_fact_reduced
    group by
        jobid,
        system
    )

    ,perjob_with_counters
    as (
    select
        mv.job_id,
        mv.number_of_containers,
        mv.requested_memory,
        mv.requested_vcores,
        mv.job_duration,
        sha1(mv.system) as system,
        mv.starttime,
        mv.finishtime,
        jf.FILE_BYTES_READ,
        jf.FILE_BYTES_WRITTEN,
        jf.HDFS_BYTES_READ,
        jf.HDFS_BYTES_WRITTEN
    from
        mem_and_vcore_per_job as mv
    join
        job_fact_reduced as jf
    on
        jf.jobid = mv.job_id
        and jf.system = mv.system
    )     


select * from perjob_with_counters

