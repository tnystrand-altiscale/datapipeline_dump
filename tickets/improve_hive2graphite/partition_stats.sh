hive -e "ANALYZE TABLE cluster_metrics_prod_2.container_time_series partition(date,system) COMPUTE STATISTICS noscan;"
