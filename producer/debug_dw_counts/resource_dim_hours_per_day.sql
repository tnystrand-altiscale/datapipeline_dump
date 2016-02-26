with
    records_per_hour
    as (
    select
        count(*) as num_records,
        cluster_system,
        cluster_date,
        avg(int(timestamp/1000/3600)*3600) as hour_start
    from
        cluster_metrics_prod_2.cluster_resource_dim
    where
        cluster_date between '2015-12-01' and '2015-12-31'
        and cluster_system = 'iheartradio'
    group by
        cluster_system,
        cluster_date,
        int(timestamp/1000/3600)*3600
    )
select
    count(*) as number_of_covered_hours,
    cluster_system,
    cluster_date
from 
    records_per_hour
group by
    cluster_system,
    cluster_date


