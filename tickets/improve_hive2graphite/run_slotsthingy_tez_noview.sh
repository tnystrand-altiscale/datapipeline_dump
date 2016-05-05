hive  \
    -hiveconf START_DATE=2016-04-10 \
    -hiveconf END_DATE=2016-04-10 \
    -hiveconf SYSTEM='' \
    -hiveconf DB_NAME=cluster_metrics_prod_2 \
    -hiveconf JOB_TABLE=job_fact \
    -hiveconf OUT_DIR=/tmp/tnystrand-slots-2-tez-nw \
    -hiveconf EDB_DB_NAME=rollup_edb_sdb_dims_prod_1 \
    -hiveconf MAX_LOOKBACK=45 \
    -hiveconf MAX_LOOKAHEAD=2 \
    -hiveconf hive.execution.engine=tez \
    -hiveconf hive.tez.java.opts=-Xmx4800m \
    -hiveconf hive.tez.container.size=5000 \
    -hiveconf tez.cpu.vcores=4 \
    -hiveconf tez.session.client.timeout.secs=10000000 \
    -hiveconf tez.session.am.dag.submit.timeout.secs=10000000 \
    -hiveconf tez.queue.name=research \
    "-f" "slots_for_graphite.backfill_noview.sql"
