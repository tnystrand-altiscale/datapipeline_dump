# Good for debugging since it runs locally on workbench
# and therefore does not need to upload a lot of jar files
# Run with --verbose for spark stratup info

# Build process:
# mvn package - for all code changes
# mvn install - copy jars to target/lib folder

# Get comma separated list of all jars needed by the application
jars=$(ls -1 target/lib/ | awk '{print "target/lib/"$1}' | sed \$d | paste -sd ',')

# Get directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Default logging level if not already set by environmental variable
APPSUM_LOGLEVEL=${APPSUM_LOGLEVEL:-DEBUG}
echo "Running with app loglevel (APPSUM_LOGLEVEL): "$APPSUM_LOGLEVEL

$SPARK_HOME/bin/spark-submit \
    --master local[3] \
    --driver-memory 512M \
    --executor-memory 1G \
    --executor-cores 3 \
    --num-executors 1 \
    --driver-java-options="-Dlog4j.configuration=file:///$DIR/spark_log4j/local_log4j.properties -DAPPSUM_LOGLEVEL=$APPSUM_LOGLEVEL" \
    --class com.sap.altiscale.datapipeline.applicationsummary_slothours.ApplicationSummarySlotHourStream \
    --jars $jars \
    target/applicationsummary_slothours-1.0.0.jar \
    --inputTopics logstash_rm_logs \
    --outputTopic cluster-metrics-json \
    --kafkaBrokers kafka-dev01.test.altiscale.com:9092 \
    --checkpointDirectory /user/$USER/spark_streaming_checkpoint/ApplicationSummarySlotHourStream_local \
    --enableCheckpointing \
    --processingInterval 10 \
    --checkpointInterval 10 \
    --collectAndPrintToDriver \
    --nameTag dev-local
