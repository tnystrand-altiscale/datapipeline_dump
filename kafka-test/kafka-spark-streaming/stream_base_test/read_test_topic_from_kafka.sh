# vim: set wrap:
# This should work! 
# For some reason it doesn't, see for instance
# (http://stackoverflow.com/questions/41774446/kafka-bootstrap-servers-vs-zookeeper-in-kafka-console-consumer)
./kafka/bin/kafka-console-consumer.sh --bootstrap-server=kafka01-sc1.service.altiscale.com:9092 --topic=filtered_RM_on_dogfood --property print.key=true --from-beginning

# Running essentially the same cmd from an old version of kafka-console-consumer, works fine
#~/semi_serious/kafka-test/confluent-2.0.0/bin/kafka-console-consumer --zookeeper kafka01-sc1.service.altiscale.com:2181 --topic=filtered_RM_on_dogfood --property print.key=true --from-beginning
