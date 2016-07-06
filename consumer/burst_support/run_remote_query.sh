set -e

ssh dfs << EOF
mkdir /tmp/thomas
rm /tmp/thomas/*
hive \
--hiveconf hive.execution.engine=tez \
--hiveconf hive.tez.java.opts=-Xmx7096m \
--hiveconf hive.tez.container.size=8000 \
--hiveconf tez.cpu.vcores=4 \
--hiveconf tez.session.client.timeout.secs=10000000 \
--hiveconf tez.session.am.dag.submit.timeout.secs=10000000 \
--hiveconf tez.queue.name=research \
-e "select * from cluster_metrics_prod_2.burst_time_series where partition_date between '2016-06-22' and '2016-07-05'" > /tmp/thomas/burst_time_series.tsv
EOF

python run_etl_procedure.py -f burst_time_series.tsv
