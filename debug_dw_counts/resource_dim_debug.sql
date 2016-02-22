select
    count(*) as num_records,
    cluster_system,
    cluster_date
from
    cluster_metrics_prod_2.cluster_resource_dim
where
    cluster_date between '2016-01-01' and '2016-01-28'
    and cluster_system = 'firstdata'
group by
    cluster_system,
    cluster_date
