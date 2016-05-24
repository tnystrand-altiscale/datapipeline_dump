select
    count(*) as numhits,
    date,
    system
from
    cluster_metrics_prod_2.container_fact
where
    date>='2016-01-01'
    and requestedtime>0
    and completedtime>0
    and completedtime-requestedtime>1000*60*60*24*2
group by
    system,
    date
