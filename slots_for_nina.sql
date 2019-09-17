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
set hivevar:DB_NAME=cluster_metrics_prod_2;
set hivevar:START_DATE=2016-07-20;
set hivevar:EDB_DB_NAME=rollup_edb_sdb_dims_prod_1;
set hivevar:OUT_DIR=/tmp/very_tmp;
set hivevar:JOB_TABLE=job_fact;

-- could be overridden with some hand-editing:
set hivevar:NORMAL_LOOKBACK=45;
set hivevar:MAX_LOOKBACK=60;
set hivevar:MAX_LOOKAHEAD=2;
set hivevar:SYSTEM=;

-- Settings for current run
set hive.tez.java.opts=-Xmx7800m;
set hive.execution.engine=tez;
set hive.tez.container.size=8000;
set tez.cpu.vcores=4;
set tez.session.client.timeout.secs=10000000;
set tez.session.am.dag.submit.timeout.secs=10000000;
set tez.queue.name=production;

WITH 
    -- make a historical list of uuid -> system_user_name
    user_map AS (
    SELECT
        distinct uniqueidentifier, system_user_name
    FROM
        ${EDB_DB_NAME}.principal_dim
    )
    ,
    -- distinct (day, system) pairs that were mentioned in the recent container_time_series partitions
    updated_days_by_system AS (
    SELECT DISTINCT
        (minute_start - (minute_start % (24 * 3600))) as day_start,
        system as system
    FROM
        ${DB_NAME}.container_time_series
    WHERE
    -- restore this constraint to focus on a single system
        system LIKE '${SYSTEM}%' AND
        date >= date_sub('${START_DATE}', ${NORMAL_LOOKBACK}) AND
        date < date_add('${START_DATE}', ${MAX_LOOKAHEAD})
    )
    -- some useful queries using you might want to run here:
    -- select count(*) from updated_days_by_system
    -- select from_unixtime(day_start), system from updated_days_by_system
    ,
    -- all the entries for those (day, system) pairs
    slots_with_hour_and_day AS (
    SELECT
        (minute_start - (minute_start % (24 * 3600))) as day_start,
        (minute_start - (minute_start % 3600)) as hour_start,
        system as system,
        COALESCE(principal_uuid, 'UNKNOWN') as uuid,
        IF(
          container_time_series.user_key LIKE 'EMPLOYEE%'
          OR state = 'RESERVED'
          , 0
          , memory/2500/60
        ) as slot_hours,
        job_id
    FROM
         ${DB_NAME}.container_time_series container_time_series
    LEFT SEMI JOIN updated_days_by_system ON
         (updated_days_by_system.day_start = 
          (container_time_series.minute_start - (container_time_series.minute_start % (24 * 3600)))) AND 
         (updated_days_by_system.system = container_time_series.system)
    WHERE
        system LIKE '${SYSTEM}%' AND
        state != 'RESERVED' AND
        date >= date_sub('${START_DATE}', ${MAX_LOOKBACK})
    )
    ,
    --filtered by jobs table
    job_slots_with_hour_and_day AS (
    SELECT day_start, hour_start, slots_with_hour_and_day.system, uuid, slot_hours FROM slots_with_hour_and_day
    LEFT JOIN ${DB_NAME}.${JOB_TABLE} ON
        (slots_with_hour_and_day.system = ${JOB_TABLE}.system) AND (slots_with_hour_and_day.job_id = ${JOB_TABLE}.jobid)
    WHERE
    	${JOB_TABLE}.date >= date_sub( date_sub('${START_DATE}', ${NORMAL_LOOKBACK}), ${MAX_LOOKBACK})
    ),
    -- summarize them into groups
    slots_runup AS (
    SELECT
        system,
        day_start,
        sum(slot_hours) as slot_hours_sum
    FROM
        job_slots_with_hour_and_day
    GROUP BY
        system, day_start
    )
-- and finally join on the username for the uuid
SELECT
    system as system,
    day_start as day_start,
    slot_hours_sum,
    sum(slot_hours_sum) over (partition by system rows between 29 preceding and 0 following) as slot_hours_sum_30_day_average
FROM
    slots_runup
