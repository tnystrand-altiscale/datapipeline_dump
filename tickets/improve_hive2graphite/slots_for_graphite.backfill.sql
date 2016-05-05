-- incremental query for slot-hours metrics from the DW
-- parameters:
--   ${DB_NAME} - database containing container_time_series
--   ${START_DATE} - YYYY-MM-DD date from which to update
--   ${END_DATE} -- end of interval to update
--   ${MAX_LOOKBACK} -- longer than longest running container (estimate 45 days)
--   ${MAX_LOOKAHEAD} -- containers finishing on day1 might be recorded on day1+1
--                      this should happen very seldomly, and are most likely to be
--                      caught by subsquent days script runs
--   ${EDB_DB_NAME} - database name for edb principal_dim
--   ${OUT_DIR} - hdfs directory where output goes (ctrl-a separated)
--   ${SYSTEM} - if left blank it will take all systems

-- Run from command line:
--nohup hive \
--   -hiveconf DB_NAME=dp_views \
--   -hiveconf START_DATE=2016-02-13 \
--   -hiveconf END_DATE=2016-02-20 \
--   -hiveconf EDB_DB_NAME=rollup_edb_sdb_dims_prod_1 \
--   -hiveconf OUT_DIR=/tmp/tmp_slot_dir \
--   -hiveconf JOB_TABLE=job_fact \
--   -hiveconf MAX_LOOKBACK=45 \
--   -hiveconf MAX_LOOKAHEAD=2 \
--   -hiveconf SYSTEM= \
--   -hiveconf mapreduce.map.memory.mb=5120 \
--   -hiveconf mapreduce.reduce.memory.mb=5120 \
--   -hiveconf mapred.job.queue.name=production \
--   -f ./slots_for_graphite.backfill.q > nohup.out 2>&1 &

-- Inside a script:
--set DB_NAME=dp_views;
--set START_DATE=2016-02-20;
--set END_DATE=2016-02-20;
--set EDB_DB_NAME=rollup_edb_sdb_dims_prod_1;
--set OUT_DIR=/tmp/tmp_slot_dir;
--set JOB_TABLE=job_fact;
--set MAX_LOOKBACK=45;
--set MAX_LOOKAHEAD=2;
--set SYSTEM=dogfood;
--set mapreduce.map.memory.mb=5120;
--set mapreduce.reduce.memory.mb=5120;
--set mapred.job.queue.name=production;


WITH 

    -- Make a historical list of uuid -> system_user_name
    -- since container_time_series only has uuid, not user names
    user_map 
    AS (
    SELECT
        distinct uniqueidentifier, system_user_name
    FROM
        ${hiveconf:EDB_DB_NAME}.principal_dim
    )
    
    
    -- Distinct (day, system) pairs that were mentioned in the recent container_time_series partitions
    -- Used to figure out which days will have to be 'restated' due to late arriving containers
    ,updated_days_by_system
    AS (
    SELECT DISTINCT
        (minute_start - (minute_start % (24 * 3600))) as day_start,
        container_system as system
    FROM
        ${hiveconf:DB_NAME}.container_time_series_fact
    WHERE
    -- restore this constraint to focus on a single system
        container_system LIKE '%${hiveconf:SYSTEM}%' AND
        partition_date between '${hiveconf:START_DATE}' and '{END_DATE}'
    )
    
    -- All the entries for those (day, system) pairs
    -- Pick out all containers so we can recalculate
    --   slot hours for the portal for all days affect by container time series
    ,slots_with_hour_and_day
    AS (
    SELECT
        (minute_start - (minute_start % (24 * 3600))) as day_start,
        (minute_start - (minute_start % 3600)) as hour_start,
        container_system as system,
        COALESCE(principal_uuid, 'UNKNOWN') as uuid,
        slot_hours_charged as slot_hours,
        job_id
    FROM
         ${hiveconf:DB_NAME}.container_time_series_fact container_time_series
    LEFT SEMI JOIN updated_days_by_system ON
         (updated_days_by_system.day_start = 
          (container_time_series.minute_start - (container_time_series.minute_start % (24 * 3600)))) AND 
         (updated_days_by_system.system = container_time_series.container_system)
    WHERE
        container_system LIKE '%${hiveconf:SYSTEM}%' AND
        container_state != 'RESERVED' AND
        partition_date >= date_sub('${hiveconf:START_DATE}', ${hiveconf:MAX_LOOKBACK})
    )
    
    
    -- Filtered by jobs table
    -- Remove containers (slot hours) for jobs that has not finished yet
    ,job_slots_with_hour_and_day
    AS (
    SELECT 
        day_start, 
        hour_start, 
        slots_with_hour_and_day.system, 
        uuid, 
        slot_hours 
    FROM
        slots_with_hour_and_day
    LEFT JOIN 
        ${hiveconf:DB_NAME}.${hiveconf:JOB_TABLE} ON
        (slots_with_hour_and_day.system = ${hiveconf:JOB_TABLE}.job_system) AND (slots_with_hour_and_day.job_id = ${hiveconf:JOB_TABLE}.jobid)
    WHERE
    	${hiveconf:JOB_TABLE}.job_date >= date_sub('${hiveconf:START_DATE}', ${hiveconf:MAX_LOOKBACK})
    )
    
    -- Caculating daily and hourly totals 
    ,slots_runup AS (
    SELECT
        system,
        day_start,
        hour_start,
        uuid,
        sum(slot_hours) as slot_hours_sum
    FROM
        job_slots_with_hour_and_day
    GROUP BY
        system, day_start, hour_start, uuid
    GROUPING SETS (
        (system, day_start),
        (system, hour_start),
        (system, hour_start, uuid)
        )
    )

-- and finally join on the username for the uuid
INSERT OVERWRITE DIRECTORY '${hiveconf:OUT_DIR}'
SELECT
    slots_runup.system as system,
    slots_runup.day_start as day_start,
    slots_runup.hour_start as hour_start,
    COALESCE(user_map.system_user_name, slots_runup.uuid) as user,
    slots_runup.slot_hours_sum as slot_hours_sum
FROM
    slots_runup
LEFT OUTER JOIN 
    user_map
ON 
    slots_runup.uuid = user_map.uniqueidentifier
