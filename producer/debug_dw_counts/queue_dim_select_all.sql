select
    *
from
    cluster_metrics_prod_2.queue_dim
where
    queue_date between '2015-12-01' and '2015-12-31'
    and queue_system = 'iheartradio'
