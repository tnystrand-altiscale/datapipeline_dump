SET hive.cli.print.header=false;
SET hiveconf:system=marketshare;
SET hiveconf:START_DATE=2016-05-15;
SET hiveconf:END_DATE=2016-05-18;
SET hiveconf:FIRST_DATE=2016-04-16;

USE cluster_metrics_prod_2;

WITH
    job_fact_reduced
    AS (
    SELECT
        jobid,
        slothours,
        system,
        date
    FROM
        job_fact
    WHERE
        date between '${hiveconf:START_DATE}' AND '${hiveconf:END_DATE}'
        and system like '%${hiveconf:system}%'
    )

    ,job_slot_hours_table
    AS (
    SELECT
        SUM(slothours) AS job_slot_hours,
        system,
        date,
        jobid
    FROM
        job_fact_reduced
    GROUP BY
        system,
        date,
        jobid
    )

    ,container_slot_hours_table
    AS (
    SELECT
        sum(memory/2500/60) AS container_slot_hours,
        cts.system,
        jf.date,
        cts.jobid
    FROM
        container_time_series as cts,
        job_fact_reduced as jf
    WHERE
        cts.job_id = jf.jobid
        AND cts.system = jf.system
        AND cts.state != 'RESERVED'
        AND cts.date between '${hiveconf:FIRST_DATE}' AND '${hiveconf:END_DATE}'
        AND cts.system like '%${hiveconf:system}%'
    GROUP BY
        cts.system,
        cts.jobid,
        jf.date
    )

SELECT
    (job_slot_hours - container_slot_hours)/container_slot_hours*100 AS percent_diff,
    jsh.system AS system,
    jsh.date AS date
FROM
    job_slot_hours_table as jsh,
    container_slot_hours_table as csh
WHERE
    jsh.system = csh.system
    AND jsh.jobid = csh.jobid
    AND jsh.date = csh.date
ORDER BY
    system,
    date,
    percent_diff;

