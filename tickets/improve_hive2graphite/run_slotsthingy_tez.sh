hive  \
    -hiveconf START_DATE=2016-04-10 \
    -hiveconf END_DATE=2016-04-10 \
    -hiveconf SYSTEM='' \
    -hiveconf DB_NAME=dp_views \
    -hiveconf JOB_TABLE=job_fact \
    -hiveconf OUT_DIR=/tmp/tnystrand-slots-2-tez \
    -hiveconf EDB_DB_NAME=rollup_edb_sdb_dims_prod_1 \
    -hiveconf MAX_LOOKBACK=45 \
    -hiveconf MAX_LOOKAHEAD=2 \
    -hiveconf hive.execution.engine=tez \
    -hiveconf hive.tez.java.opts=-Xmx7800m \
    -hiveconf hive.tez.container.size=8000 \
    -hiveconf tez.cpu.vcores=4 \
    -hiveconf tez.session.client.timeout.secs=10000000 \
    -hiveconf tez.session.am.dag.submit.timeout.secs=10000000 \
    -hiveconf tez.queue.name=research \
    "-f" "slots_for_graphite.backfill.sql"
