confluent-2.0.0/bin/kafka-topics \
    --delete --zookeeper kafka01-us-west-1.test.altiscale.com:2181 \
    --replication-factor 1 --partitions 1 --topic test_json
