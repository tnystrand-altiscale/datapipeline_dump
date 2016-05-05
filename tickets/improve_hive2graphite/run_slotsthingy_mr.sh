hive  \
    -hiveconf START_DATE=2016-04-10 \
    -hiveconf END_DATE=2016-04-10 \
    -hiveconf SYSTEM='' \
    -hiveconf DB_NAME=dp_views \
    -hiveconf JOB_TABLE=job_fact \
    -hiveconf OUT_DIR=/tmp/tnystrand-slots-2-mr \
    -hiveconf EDB_DB_NAME=rollup_edb_sdb_dims_prod_1 \
    -hiveconf MAX_LOOKBACK=45 \
    -hiveconf MAX_LOOKAHEAD=2 \
    -hiveconf hive.execution.engine=mr \
    -hiveconf mapreduce.map.memory.mb=6000 \
    -hiveconf mapreduce.map.java.opts=-Xmx5800m \
    -hiveconf mapreduce.reduce.memory.mb=6000 \
    -hiveconf mapreduce.reduce.java.opts=-Xmx5800m \
    -hiveconf mapred.job.queue.name=research \
    -hiveconf mapred.reduce.tasks=1 \
    "-f" "slots_for_graphite.backfill.sql"
