use tnystrand_metrics_archive;
with
    grouped_by_minute as (
        select
            timestamp/60*60 as minute_start,
            system,
            partition_date
        from
            mt_burst_flow
        group by
            timestamp/60*60,
            system,
            partition_date
    ),
    ranked as (
        select
            *,
            rank() over (order by minute_start) as row_num
        from
            grouped_by_minute
    ),
    ranked_self_joined as (
        select
            r1.minute_start as minute_start,
            r2.minute_start - r1.minute_start as minute_diff,
            r1.system as system,
            r1.partition_date as partition_date,
            r1.row_num as row_num
        from
            ranked as r1
        left outer join
            ranked as r2
        on
            r1.row_num = r2.row_num-1
            and r1.system = r2.system
            and r1.partition_date = r2.partition_date
    )
select * from ranked_self_joined;
