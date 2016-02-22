select
    *,
    int(timestamp/1000/3600) as hour_start
from
    cluster_metrics_prod_2.cluster_resource_dim
where
    cluster_date between '2015-12-01' and '2015-12-31'
    and cluster_system = 'iheartradio'
order by
    cluster_system,
    cluster_date,
    timestamp
