../confluent-2.0.0/bin/kafka-avro-console-producer --broker-list kafka01-us-west-1.test.altiscale.com:9092 --topic test_hdfs \
--property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
