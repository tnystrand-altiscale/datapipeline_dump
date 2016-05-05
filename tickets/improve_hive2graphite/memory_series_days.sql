set hiveconf:start_date=2016-02-20;
set hiveconf:end_date=2016-02-20;
set hiveconf:system=dogfood;
set hiveconf:target_table=thomastest.slot_hours_per_day;
set hiveconf:container_facts=cluster_metrics_prod_2.container_fact;
set hiveconf:edb_db_name=rollup_edb_sdb_dims_prod_1;
set hiveconf:db_name=cluster_metrics_prod_2;

create table if not exists ${hvieconf:target_table}
    (
        slot_hours,
        system,
        user,
        queue
    )
stored as
    orc
partition by
    (
    system,
    date
    )

with
    user_map 
    AS (
    SELECT
        distinct uniqueidentifier, system_user_name
    FROM
        ${hiveconf:edb_db_name}.principal_dim
    )

    ,updated_days_by_system
    AS (
    SELECT DISTINCT
        (minute_start - (minute_start % (24 * 3600))) as day_start,
        system
    FROM
        ${hiveconf:db_name}.container_time_series
    WHERE
        system LIKE '%${hiveconf:SYSTEM}%' AND
        date between '${hiveconf:START_DATE}' and '{END_DATE}'
    )

    ,
    as (
    select
         

    from
        ${hiveconf:container_facts} as cf
    where
        date between ${hiveconf:start_date} and ${hiveconf:end_date}
        and system like '${hiveconf:system}'
    )


insert overwrite table ${hvieconf:target_table}
partition
    (
    system,
    date
    )
select
    *
from
    



