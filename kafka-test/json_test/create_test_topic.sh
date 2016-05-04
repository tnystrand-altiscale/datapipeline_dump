../confluent-2.0.0/bin/kafka-topics \
    --create --zookeeper kafka01-us-west-1.test.altiscale.com:2181 \
    --replication-factor 1 --partitions 3 --topic test_json
