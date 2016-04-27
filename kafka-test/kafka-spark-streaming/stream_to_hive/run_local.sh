#!/bin/bash
#export SPARK_CONF_DIR='/home/tnystrand/spark'
#export SPARK_HOME='/opt/alti-spark-1.4.1.hadoop24.hive13/'

# Input sare the intput tables and database
time spark-submit \
    --verbose \
    --num-executors 1 \
    --queue production \
    --executor-memory 5G \
    --class BucketJobCategories \
    --master local \
    --deploy-mode client \
    --conf "spark.task.maxFailures=1"\
    target/KafkaToHiveStream-1.0-jar-with-dependencies.jar
