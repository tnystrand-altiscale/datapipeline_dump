with
    updated_days_by_system
    AS (
    SELECT DISTINCT
        (minute_start - (minute_start % (24 * 3600))) as day_start,
        container_system as system
    FROM
        dp_views.container_time_series_fact
    WHERE
    -- restore this constraint to focus on a single system
        container_system = 'owneriq'
        and partition_date between '2016-04-10' and '2016-04-10'
    )
select * from updated_days_by_system;
