
hiveq -e "select * from cluster_metrics_prod_2.container_fact where jobid='job_1462231857641_7091' and system='ms22' and date between '2016-04-01' and '2016-05-18'" > badjob_container_ms22.tsv
hiveq -e "select * from cluster_metrics_prod_2.job_fact where jobid='job_1462231857641_7091' and system='ms22' and date between '2016-05-10' and '2016-05-18'"
