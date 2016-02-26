select
    count(*) as num_records,
    system,
    date
from
    cluster_metrics_prod_2.container_time_series
where
    date between '2016-01-01' and '2016-02-11'
    and system = 'dogfood'
group by
    system,
    date
order by
    date
