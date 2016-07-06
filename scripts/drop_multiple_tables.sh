 hive -e "use cluster_metrics_prod_2; show tables 'job_fact_2016_05*'" \
    | xargs -n 1 -I {} hive -e "drop table cluster_metrics_prod_2.{}"
