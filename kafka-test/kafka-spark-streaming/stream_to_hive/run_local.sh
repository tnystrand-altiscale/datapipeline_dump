#!/bin/bash
SPARK_VERSION=1.6.1

# Provide required spark-hive and spark-hivethriftserver related JARs, and HIVE JARs at runtime
sparksql_hivejars="$SPARK_HOME/sql/hive/target/spark-hive_2.10-${SPARK_VERSION}.jar"
sparksql_hivethriftjars="$SPARK_HOME/sql/hive-thriftserver/target/spark-hive-thriftserver_2.10-${SPARK_VERSION}.jar"
hive_jars=$sparksql_hivejars,$sparksql_hivethriftjars,$(find $HIVE_HOME/lib/ -type f -name "*.jar" | tr -s '\n' ',')
hive_jars_colon=$sparksql_hivejars:$sparksql_hivethriftjars:$(find $HIVE_HOME/lib/ -type f -name "*.jar" | tr -s '\n' ':')
spark_event_log_dir=$(grep 'spark.eventLog.dir' /etc/spark/spark-defaults.conf | tr -s ' ' '\t' | cut -f2)

# Input sare the intput tables and database
time spark-submit \
    --verbose \
    --num-executors 1 \
    --queue production \
    --executor-memory 5G \
    --class JavaKafkaToHDFSHive \
    --master yarn \
    --deploy-mode client \
    --conf "spark.task.maxFailures=1" \
    --conf spark.eventLog.dir=${spark_event_log_dir}/$USER \
    --conf spark.yarn.dist.files=/etc/spark/hive-site.xml,$hive_jars \
    --conf spark.executor.extraClassPath=$(basename $sparksql_hivejars):$(basename $sparksql_hivethriftjars) \
    --driver-class-path hive-site.xml:$hive_jars_colon \
    target/JavaKafkaToHDFSHive-1.0-jar-with-dependencies.jar \
    "kafka01-us-west-1.test.altiscale.com:9092" \
    "resourcemanager"
