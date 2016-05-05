-- incremental query for slot-hours metrics from the DW
-- parameters:
--   ${DB_NAME} - database containing container_time_series
--   ${START_DATE} - YYYY-MM-DD date from which to update (no end date is needed)
--   ${EDB_DB_NAME} - database name for edb principal_dim
--   ${OUT_DIR} - hdfs directory where output goes (ctrl-a separated)
-- e.g. command line:
-- nohup hive --hivevar START_DATE=2015-01-01 --hivevar OUT_DIR=/tmp/max-slots-2 \
--    --hivevar EDB_DB_NAME=edb_sdb_dims_prod_1 --hivevar DB_NAME=cluster_metrics_prod_1 \
--    -f ./slots_for_graphite.q > nohup.out 2>&1 &
-- inside a script:
--set hivevar:DB_NAME=dp_views;
--set hivevar:START_DATE=2016-02-13;
--set hivevar:EDB_DB_NAME=rollup_edb_sdb_dims_prod_1;
-- set hivevar:OUT_DIR=/tmp/max-slots;
--set hivevar:JOB_TABLE=job_fact;

-- could be overridden with some hand-editing:
set hivevar:NORMAL_LOOKBACK=7;
set hivevar:MAX_LOOKBACK=45;
set hivevar:MAX_LOOKAHEAD=2;
set hivevar:SYSTEM=dogfood;
set mapreduce.map.memory.mb=5120;
set mapreduce.reduce.memory.mb=5120;

WITH 
    -- Make a historical list of uuid -> system_user_name
    user_map AS (
    SELECT distinct 
        uniqueidentifier, 
        system_user_name
    FROM
        ${EDB_DB_NAME}.principal_dim
    )
    
    -- Distinct (day, system) pairs that were mentioned in the recent container_time_series partitions
    ,updated_days_by_system AS (
    SELECT DISTINCT
        (minute_start - (minute_start % (24 * 3600))) as day_start,
        --container_system as system
        system
    FROM
        --${DB_NAME}.container_time_series_fact
         cluster_metrics_prod_2.container_time_series container_time_series
    WHERE
    -- restore this constraint to focus on a single system
        --container_system LIKE '%${SYSTEM}%' AND
        --container_state != 'RESERVED' AND
        --AND partition_date >= date_sub('${START_DATE}', ${NORMAL_LOOKBACK})
        --AND partition_date < date_add('${START_DATE}', ${MAX_LOOKAHEAD})
        system LIKE '%${SYSTEM}%'
        AND state != 'RESERVED'
        AND date >= date_sub('${START_DATE}', ${NORMAL_LOOKBACK})
        AND date < date_add('${START_DATE}', ${MAX_LOOKAHEAD})
    )


    -- some useful queries using you might want to run here:
    -- select count(*) from updated_days_by_system
    -- select from_unixtime(day_start), system from updated_days_by_system
    
    -- All the entries for those (day, system) pairs
    ,slots_with_hour_and_day AS (
    SELECT
        (minute_start - (minute_start % (24 * 3600))) as day_start,
        (minute_start - (minute_start % 3600)) as hour_start,
        --container_system as system,
        system,
        COALESCE(principal_uuid, 'UNKNOWN') as uuid,
        --slot_hours_charged as slot_hours,
        memory/1000/2.5 as slot_hours,
        job_id
    FROM
         --${DB_NAME}.container_time_series_fact container_time_series
         cluster_metrics_prod_2.container_time_series container_time_series
    LEFT SEMI JOIN updated_days_by_system ON
         (updated_days_by_system.day_start = 
          (container_time_series.minute_start - (container_time_series.minute_start % (24 * 3600))))
         --AND (updated_days_by_system.system = container_time_series.container_system)
         AND (updated_days_by_system.system = container_time_series.system)
    WHERE
        --container_system LIKE '%${SYSTEM}%' AND
        --container_state != 'RESERVED' AND
        --partition_date >= date_sub('${START_DATE}', ${MAX_LOOKBACK})
        system LIKE '%${SYSTEM}%'
        AND state != 'RESERVED'
        AND date >= date_sub('${START_DATE}', ${MAX_LOOKBACK})
    )
    
    --filtered by jobs table
    ,job_slots_with_hour_and_day AS (
    SELECT 
        day_start, 
        hour_start, 
        slots_with_hour_and_day.system, 
        uuid, 
        slot_hours 
    FROM 
        slots_with_hour_and_day
    LEFT JOIN 
        ${DB_NAME}.${JOB_TABLE} 
    ON
    	(slots_with_hour_and_day.system = ${JOB_TABLE}.job_system) 
        AND (slots_with_hour_and_day.job_id = ${JOB_TABLE}.jobid)
    WHERE
    	${JOB_TABLE}.job_date >= date_sub('${START_DATE}', ${MAX_LOOKBACK})
    )
    
    -- summarize them into groups
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
INSERT OVERWRITE DIRECTORY '${OUT_DIR}'
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
