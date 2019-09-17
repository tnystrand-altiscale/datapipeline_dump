# Yarn cluster mode is good to run to avoid uploading all the jars everytime
# Note that there has to be some additional set up steps to make this work....
# Custom spark.conf etc...

# Upload missing or different jars to hdfs here...

# Set SPARK_CONF_DIR to user definet conf location...

$SPARK_HOME/bin/spark-submit \
    --verbose \
    --master yarn \
    --deploy-mode cluster \
    --queue research \
    --driver-memory 512M \
    --executor-memory 1G \
    --executor-cores 2 \
    --conf spark.eventLog.dir=hdfs:///logs/spark-history/$USER \
    --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=yarncluster-driver-log4j.properties -Djava.library.path=/opt/hadoop/lib/native/' \
    --class com.sap.altiscale.datapipeline.stream_test.StreamTest \
    target/stream_test-1.0-SNAPSHOT.jar


