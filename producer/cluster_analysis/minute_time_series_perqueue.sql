-- Grouping extended container time series with job info
-- by queue. This by queue level info is easier to visaulize
-- and process for burst prediction algorithms

set hiveconf:cluster=;
set hiveconf:start_date='2016-04-15';
set hiveconf:end_date='2016-04-25';

set hiveconf:minute_time_series_perjob=dp_derived.time_series_job_granularity;
set hiveconf:target_table=dp_derived.time_series_queue_granularity;

-- To drop table on each run
-- drop table if exists ${hiveconf:target_table};

-- Drop table command
-- hive -e "drop table dp_derived.time_series_queue_granularity"
-- Sample table command
-- hive -e "select * from dp_derived.time_series_queue_granularity limit 20"

create table if not exists ${hiveconf:target_table}
    (
        -- Container time series related
        minute_start Int,
        queue String,

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

        -- Capacities from metrics
        normal_queue_capacity Double,
        max_queue_capacity Double,
        total_memory_capacity Double,
        total_vcore_capacity Int,
        normal_memory_queue_capacity Double,
        max_memory_capacity_capacity Double,
        normal_vcore_queue_capacity Double,
        max_vcore_queue_capacity Double
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
    final_table
    as (
    select
        minute_start,
        queue,
        
        sum(memory_allocated_withreserved) as memory_allocated_withreserved,
        sum(memory_allocated) asmemory_allocated,

        sum(running_containers) as running_containers,
        sum(waiting_containers) as waiting_containers,

        sum(container_wait_time_sum) as container_wait_time_sum,

        sum(waiting_memory_sum) as waiting_memory_sum,

        sum(vcores_allocated_withreserved) as vcores_allocated_withreserved,
        sum(vcores_allocated) as vcores_allocated,

        max(cluster_memory_fromcts) as cluster_memory_fromcts,
        max(cluster_vcores_fromcts) as cluster_vcores_fromcts,

        -- Capacities from metric--s
        avg(normal_queue_capacity) as normal_queue_capacity,
        avg(max_queue_capacity) as max_queue_capacity,
        avg(total_memory_capacity) as total_memory_capacity,
        avg(total_vcore_capacity) as total_vcore_capacity,
        avg(normal_memory_queue_capacity) as normal_memory_queue_capacity,
        avg(max_memory_capacity_capacity) as max_memory_capacity_capacity,
        avg(normal_vcore_queue_capacity) as normal_vcore_queue_capacity,
        avg(max_vcore_queue_capacity) as max_vcore_queue_capacity,
    
        system,
        min(date) as date
    from
        ${hiveconf:minute_time_series_perjob}
    where
        system like '%${hiveconf:cluster}%'
        and date between ${hiveconf:start_date} and ${hiveconf:end_date}
    group by
        minute_start,
        system,
        queue
    )

insert overwrite table ${hiveconf:target_table}
partition(system, date)
select * from final_table

