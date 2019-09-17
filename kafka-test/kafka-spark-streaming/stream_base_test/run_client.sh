# Yarn client mode will upload all the jars to hdfs everytime (slower)
# One could consider making one fat jar for all the dependencies here

jars=$(ls -1 target/lib/ | awk '{print "target/lib/"$1}' | sed \$d | paste -sd ',')

$SPARK_HOME/bin/spark-submit \
    --verbose \
    --master yarn \
    --deploy-mode client \
    --queue research \
    --driver-memory 512M \
    --executor-memory 1G \
    --executor-cores 2 \
    --conf spark.eventLog.dir=hdfs:///logs/spark-history/$USER \
    --class com.sap.altiscale.datapipeline.stream_test.StreamTest \
    --jars $jars \
    target/stream_test-1.0-SNAPSHOT.jar


