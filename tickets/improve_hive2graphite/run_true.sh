hive  \
    -hiveconf START_DATE=2016-05-07 \
    -hiveconf OUT_DIR=/tmp/tnystrand-slots-2-tez/2016-05-07 \
    -hiveconf DB_NAME=dp_views \
    -hiveconf JOB_TABLE=job_fact \
    -hiveconf EDB_DB_NAME=rollup_edb_sdb_dims_prod_1 \
    -hiveconf MAX_LOOKBACK=45 \
    -hiveconf NORMAL_LOOKBACK=7 \
    -hiveconf MAX_LOOKAHEAD=2 \
    -hiveconf hive.execution.engine=tez \
    -hiveconf hive.tez.java.opts=-Xmx7800m \
    -hiveconf hive.tez.container.size=8000 \
    -hiveconf tez.cpu.vcores=4 \
    -hiveconf tez.session.client.timeout.secs=10000000 \
    -hiveconf tez.session.am.dag.submit.timeout.secs=10000000 \
    -hiveconf tez.queue.name=production \
    "-f" "slots_for_graphite.q"
