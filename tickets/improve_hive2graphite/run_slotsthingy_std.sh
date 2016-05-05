hive  \
    --hivevar START_DATE=2016-02-01 \
    --hivevar OUT_DIR=/tmp/tnystrand-slots-2 \
    --hivevar EDB_DB_NAME=edb_sdb_dims_prod_1 \
    --hivevar DB_NAME=cluster_metrics_prod_2 \
    --hiveconf hive.execution.engine=tez \
    --hiveconf hive.tez.java.opts=-Xmx7096m \
    --hiveconf hive.tez.container.size=5000 \
    --hiveconf tez.cpu.vcores=4 \
    --hiveconf tez.session.client.timeout.secs=10000000 \
    --hiveconf tez.session.am.dag.submit.timeout.secs=10000000 \
    --hiveconf tez.queue.name=research \
    "-f" "tmp_slots_for_graphite.q"
