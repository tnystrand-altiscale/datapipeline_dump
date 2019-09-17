use tnystrand_metrics_archive;
with
    grouped_by_minute as (
        select
            int(timestamp/60000)*60 as minute_start,
            system,
            partition_date
        from
            mt_burst
        where
            (system='dogfood' or system='playfirst')
            and partition_date > '2017-07-22'
        group by
            int(timestamp/60000)*60,
            system,
            partition_date
    ),
    ranked as (
        select
            *,
            rank() over (partition by system order by minute_start) as row_num
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
    )
select * from ranked_self_joined;
