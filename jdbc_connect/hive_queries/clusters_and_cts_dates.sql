set hiveconf:SYSTEM=zudlylabs;
set hiveconf:START_DATE='2016-04-10';
set hiveconf:END_DATE='2016-04-10';

with
    updated_days_by_system
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
select * from updated_days_by_system;
