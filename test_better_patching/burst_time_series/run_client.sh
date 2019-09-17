# Yarn client mode will upload all the jars to hdfs everytime (slower)
# Run with --verbose for spark stratup info

# Get directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Default driver logging level if not already set by environmental variable
APPSUM_DRIVER_LOGLEVEL=${APPSUM_DRIVER_LOGLEVEL:-INFO}
echo "Running with app driver loglevel (APPSUM_DRIVER_LOGLEVEL): "$APPSUM_DRIVER_LOGLEVEL

# Default executor logging level if not already set by environmental variable
APPSUM_EXECUTOR_LOGLEVEL=${APPSUM_EXECUTOR_LOGLEVEL:-INFO}
echo "Running with app executor  loglevel (APPSUM_EXECUTOR_LOGLEVEL): "$APPSUM_EXECUTOR_LOGLEVEL

$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --queue research \
    --driver-memory 512M \
    --executor-memory 1G \
    --executor-cores 1 \
    --num-executors 3 \
    --conf spark.eventLog.dir=hdfs:///logs/spark-history/$USER \
    --conf spark.yarn.dist.files="$DIR/spark_log4j/executor_log4j.properties" \
    --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=executor_log4j.properties -DAPPSUM_LOGLEVEL=$APPSUM_EXECUTOR_LOGLEVEL -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy -Djava.library.path=/opt/hadoop/lib/native/" \
    --driver-java-options="-Dlog4j.configuration=file:///$DIR/spark_log4j/local_log4j.properties -DAPPSUM_LOGLEVEL=$APPSUM_DRIVER_LOGLEVEL" \
    --class com.sap.altiscale.datapipeline.applicationsummary_slothours.ApplicationSummarySlotHourStream \
    target/applicationsummary_slothours-1.0.0-jar-with-dependencies.jar \
    --inputTopics logstash_rm_logs \
    --outputTopic cluster-metrics-json \
    --kafkaBrokers kafka-dev01.test.altiscale.com:9092 \
    --checkpointDirectory /user/$USER/spark_streaming_checkpoint/ApplicationSummarySlotHourStream_client \
    --enableCheckpointing \
    --checkpointInterval 60 \
    --processingInterval 60 \
    --nameTag dev-client
