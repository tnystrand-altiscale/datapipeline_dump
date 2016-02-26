str="select * from cluster_metrics_prod_2.robbed_jobs_report where date between '$1' and '$2' and system='firstdata';"

hive -hiveconf tez.queue.name=research -e "$str"
