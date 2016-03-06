hive \
    --hiveconf hive.execution.engine=tez \
    --hiveconf hive.tez.java.opts=-Xmx7096m \
    --hiveconf hive.tez.container.size=8000 \
    --hiveconf tez.cpu.vcores=4 \
    --hiveconf tez.session.client.timeout.secs=10000000 \
    --hiveconf tez.session.am.dag.submit.timeout.secs=10000000 \
    --hiveconf tez.queue.name=production \
    "-e" "select distinct name from edb_sdb_dims_prod_1.cluster_dim"
