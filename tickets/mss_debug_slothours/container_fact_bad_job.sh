 hiveq -e "select * from cluster_metrics_prod_2.container_fact where jobid='job_1462170014413_9097' and system='marketshare' and date between '2016-04-01' and '2016-05-18'" > 041616_badjob.tsv
