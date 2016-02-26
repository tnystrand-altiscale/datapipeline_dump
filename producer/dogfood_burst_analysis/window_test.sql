with
    tmp
    as (
    select
        minute_start,
        sum(memory) as memory,
        system,
        date
    from
        cluster_metrics_prod_2.container_time_series
    where
        date between '2016-01-01' and '2016-02-17'
        --and system='dogfood'
    group by
        minute_start,
        system,
        date
    )

select
    minute_start,
    memory,
    max(memory) over (partition by system order by minute_start rows between 10 preceding and 0 following) as memory_ext,
    system,
    date
from
    tmp
