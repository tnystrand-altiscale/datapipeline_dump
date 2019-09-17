# Yarn cluster mode is good to run to avoid uploading all the jars everytime
# Note that there has to be some additional set up steps to make this work....
# Custom spark.conf etc...

# Upload missing or different jars to hdfs here...

# Set SPARK_CONF_DIR to user definet conf location...

# For fault tolerance several settings are changed as described in
# http://mkuthan.github.io/blog/2016/09/30/spark-streaming-on-yarn/

# Get directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Default driver logging level if not already set by environmental variable
APPSUM_DRIVER_LOGLEVEL=${APPSUM_DRIVER_LOGLEVEL:-INFO}
echo "Running with app driver loglevel (APPSUM_DRIVER_LOGLEVEL): "$APPSUM_DRIVER_LOGLEVEL

# Default executor logging level if not already set by environmental variable
APPSUM_EXECUTOR_LOGLEVEL=${APPSUM_EXECUTOR_LOGLEVEL:-INFO}
echo "Running with app executor loglevel (APPSUM_EXECUTOR_LOGLEVEL): "$APPSUM_EXECUTOR_LOGLEVEL

$SPARK_HOME/bin/spark-submit \
    --verbose \
    --master yarn \
    --deploy-mode cluster \
    --queue production \
    --driver-memory 512M \
    --executor-memory 1G \
    --executor-cores 7 \
    --num-executors 6 \
    --conf spark.eventLog.dir=hdfs:///logs/spark-history/$USER \
    --conf spark.yarn.dist.files="$DIR/spark_log4j/driver_log4j.properties,$DIR/spark_log4j/executor_log4j.properties" \
    --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=driver_log4j.properties -Djava.library.path=/opt/hadoop/lib/native" \
    --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=executor_log4j.properties -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy -Djava.library.path=/opt/hadoop/lib/native/" \
    --class com.sap.altiscale.datapipeline.applicationsummary_slothours.ApplicationSummarySlotHourStream \
    --conf spark.yarn.maxAppAttempts=4 \
    --conf spark.yarn.am.attemptFailuresValidityInterval=1h \
    --conf spark.yarn.max.executor.failures=48 \
    --conf spark.yarn.executor.failuresValidityInterval=1h \
    --conf spark.task.maxFailures=8 \
    target/applicationsummary_slothours-1.0.0-jar-with-dependencies.jar \
    --inputTopics logstash_rm_logs \
    --outputTopic cluster-metrics-json \
    --kafkaBrokers kafka-dev01.test.altiscale.com:9092 \
    --checkpointDirectory /user/$USER/spark_streaming_checkpoint/ApplicationSummarySlotHourStream_cluster \
    --enableCheckpointing \
    --checkpointInterval 60 \
    --processingInterval 60 \
    --nameTag dev-cluster
