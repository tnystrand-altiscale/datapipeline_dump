str="select * from cluster_metrics_prod_2.container_fact where date between '$1' and '$2' and system='$3'"

hive -hiveconf tez.queue.name=research -e "$str"
