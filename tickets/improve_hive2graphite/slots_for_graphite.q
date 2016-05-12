-- incremental query for slot-hours metrics from the DW
-- parameters:
--   ${hiveconf:DB_NAME} - database containing container_time_series
--   ${hiveconf:START_DATE} - YYYY-MM-DD date from which to update (no end date is needed)
--   ${hiveconf:EDB_DB_NAME} - database name for edb principal_dim
--   ${hiveconf:OUT_DIR} - hdfs directory where output goes (ctrl-a separated)
-- e.g. command line:
-- nohup hive --hivevar START_DATE=2015-01-01 --hivevar OUT_DIR=/tmp/max-slots-2 \
--    --hivevar EDB_DB_NAME=edb_sdb_dims_prod_1 --hivevar DB_NAME=cluster_metrics_prod_1 \
--    -f ./slots_for_graphite.q > nohup.out 2>&1 &
-- inside a script:
--set hivevar:DB_NAME=dp_views;
--set hivevar:START_DATE=2016-05-01;
--set hivevar:EDB_DB_NAME=rollup_edb_sdb_dims_prod_1;
--set hivevar:OUT_DIR=/tmp/very_tmp;
--set hivevar:JOB_TABLE=job_fact;

-- could be overridden with some hand-editing:
--set hivevar:NORMAL_LOOKBACK=7;
--set hivevar:MAX_LOOKBACK=45;
--set hivevar:MAX_LOOKAHEAD=2;
--set hivevar:SYSTEM=dogfood;

-- Settings for current run
--set hive.execution.engine=tez;
--set hive.tez.java.opts=-Xmx7800m;
--set hive.tez.container.size=8000;
--set tez.cpu.vcores=4;
--set tez.session.client.timeout.secs=10000000;
--set tez.session.am.dag.submit.timeout.secs=10000000;
--set tez.queue.name=production;

WITH 
-- make a historical list of uuid -> system_user_name
user_map AS (
SELECT
    distinct uniqueidentifier, system_user_name
FROM
    ${hiveconf:EDB_DB_NAME}.principal_dim
)
,
-- distinct (day, system) pairs that were mentioned in the recent container_time_series partitions
updated_days_by_system AS (
SELECT DISTINCT
    (minute_start - (minute_start % (24 * 3600))) as day_start,
    container_system as system
FROM
    ${hiveconf:DB_NAME}.container_time_series_fact
WHERE
-- restore this constraint to focus on a single system
--  container_system = '${hiveconf:SYSTEM}' AND
    partition_date >= date_sub('${hiveconf:START_DATE}', ${hiveconf:NORMAL_LOOKBACK}) AND
    partition_date < date_add('${hiveconf:START_DATE}', ${hiveconf:MAX_LOOKAHEAD})
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
--  container_system = '${hiveconf:SYSTEM}' AND
    container_state != 'RESERVED' AND
    partition_date >= date_sub('${hiveconf:START_DATE}', ${hiveconf:MAX_LOOKBACK})
)
,
--filtered by jobs table
job_slots_with_hour_and_day AS (
SELECT day_start, hour_start, slots_with_hour_and_day.system, uuid, slot_hours FROM slots_with_hour_and_day
LEFT JOIN ${hiveconf:DB_NAME}.${hiveconf:JOB_TABLE} ON
    (slots_with_hour_and_day.system = ${hiveconf:JOB_TABLE}.job_system) AND (slots_with_hour_and_day.job_id = ${hiveconf:JOB_TABLE}.jobid)
WHERE
	${hiveconf:JOB_TABLE}.job_date >= date_sub( date_sub('${hiveconf:START_DATE}', ${hiveconf:NORMAL_LOOKBACK}), ${hiveconf:MAX_LOOKBACK})
),
-- summarize them into groups
slots_runup AS (
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
LEFT OUTER JOIN user_map
ON slots_runup.uuid = user_map.uniqueidentifier
