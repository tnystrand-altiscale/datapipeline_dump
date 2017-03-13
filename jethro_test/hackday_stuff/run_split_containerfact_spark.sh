spark_version=1.6.1
sparksql_hivejars="$SPARK_HOME/sql/hive/target/spark-hive_2.10-${spark_version}.jar"
hive_jars_colon=$sparksql_hivejars:$(find $HIVE_HOME/lib/ -type f -name "*.jar" | tr -s '\n' ':')
hive_jars=$sparksql_hivejars,$(find $HIVE_HOME/lib/ -type f -name "*.jar" | tr -s '\n' ',')

spark_event_log_dir=$(grep 'spark.eventLog.dir' /etc/spark/spark-defaults.conf | tr -s ' ' '\t' | cut -f2)

# pyspark only supports yarn-client mode now
$SPARK_HOME/bin/spark-submit \
    --verbose \
    --master yarn \
    --deploy-mode client \
    --num-executors 10 \
    --queue research \
    --executor-memory 10G \
    --driver-class-path /etc/spark/hive-site.xml:$hive_jars_colon \
    --conf spark.eventLog.dir=${spark_event_log_dir}/$USER \
    --conf spark.yarn.dist.files=/etc/spark/hive-site.xml,$hive_jars \
    --conf spark.app.name='GanttContainerFacts' \
    --py-files split_containerfact_spark.py \
    split_containerfact_spark.py -s $1 -e $2
