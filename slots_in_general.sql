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


--set hivevar:SYSTEM=ms3;
set hivevar:SYSTEM=marketshare;

set hivevar:DB_NAME=cluster_metrics_prod_2;
--set hivevar:START_DATE=2016-07-18;
set hivevar:START_DATE=2016-07-19;
set hivevar:JOB_TABLE=job_fact;

-- could be overridden with some hand-editing:
set hivevar:MAX_LOOKBACK=0;
set hivevar:MAX_LOOKAHEAD=0;

-- Settings for current run
set hive.tez.java.opts=-Xmx7800m;
set hive.execution.engine=tez;
set hive.tez.container.size=8000;
set tez.cpu.vcores=4;
set tez.session.client.timeout.secs=10000000;
set tez.session.am.dag.submit.timeout.secs=10000000;
set tez.queue.name=production;

WITH 
    slots_with_hour_and_day AS (
    SELECT
        (minute_start - (minute_start % (24 * 3600))) as day_start,
        (minute_start - (minute_start % 3600)) as hour_start,
        system,
        date,
        IF(
          container_time_series.user_key LIKE 'EMPLOYEE%'
          OR state = 'RESERVED'
          , 0
          , memory/2500/60
        ) as slot_hours,
        job_id
    FROM
         ${DB_NAME}.container_time_series container_time_series
    WHERE
        system LIKE '${SYSTEM}%'
        and state != 'RESERVED'
        and date between '${START_DATE}' and date_sub('${START_DATE}', ${MAX_LOOKBACK})
        and (
        --job_id='job_1466186871487_21781' or
        --job_id='job_1466186871487_21782' or
        --job_id='job_1466186871487_21781' or
        --job_id='job_1466186871487_21782' or
        --job_id='job_1466186871487_21784' or
        --job_id='job_1466186871487_21787' or
        --job_id='job_1466186871487_21788' or
        --job_id='job_1466186871487_21791' or
        --job_id='job_1466186871487_21793' or
        --job_id='job_1466186871487_21794' or
        --job_id='job_1466186871487_21809' or
        --job_id='job_1466186871487_21823'
    
        job_id='job_1462170014413_60321' or
        job_id='job_1462170014413_60322' or
        job_id='job_1462170014413_60323' or
        job_id='job_1462170014413_60325' or
        job_id='job_1462170014413_60328' or
        job_id='job_1462170014413_60329' or
        job_id='job_1462170014413_60330'
        )
    )
-- and finally join on the username for the uuid
SELECT
    system,
    sum(slot_hours) as slot_hours_sum,
    job_id
FROM
    slots_with_hour_and_day
group by
    job_id,
    system
