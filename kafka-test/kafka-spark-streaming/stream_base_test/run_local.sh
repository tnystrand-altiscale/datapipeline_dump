# Good for debugging since it runs locally on workbench
# and therefore does not need to upload a lot of jar files

# Build process:
# mvn package - for all code changes
# mvn install - copy jars to target/lib folder

# Get comma separated list of all jars needed by the application
jars=$(ls -1 target/lib/ | awk '{print "target/lib/"$1}' | sed \$d | paste -sd ',')

$SPARK_HOME/bin/spark-submit \
    --verbose \
    --master local[3] \
    --driver-memory 512M \
    --executor-memory 1G \
    --executor-cores 2 \
    --conf spark.eventLog.dir=hdfs:///logs/spark-history/$USER \
    --class com.sap.altiscale.datapipeline.stream_test.StreamTest \
    --jars $jars \
    target/stream_test-1.0-SNAPSHOT.jar
