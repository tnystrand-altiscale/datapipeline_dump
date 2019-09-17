Kafka and Spark streaming test
------------------------------

Contains a simple example of reading from a kafka topic, filtering and writing back to another topic - filtered_RM_on_dogfood

To compile and get a basic jar:
```
mvn package
```

To move jars into target folder:
```
mvn install
```

To run the program:
```
./run_local.sh
```

The suggested .sh file runs SPARK in 'debug mode', that is locally
It is equally possible to run in yarn-client or yarn-cluster mode,
but this requires uploading all the jars to hdfs (templates ./run_cluster.sh ./run_client.sh)
In client mode, this is done automatically by SPARK (convenient to make one fat jar)
In cluster mode, this could/should be done 'manually'
See [altiscale doc](https://documentation.altiscale.com/spark-2-0-with-altiscale) for instance 
To create the fat-jar, we need to change the maven pom.xml accordingly, possibly include making the
jar-with-dependencies.jar in the install step

To verify that everything works as it should, run 
```
./get_kafka_for_testing.sh
./read_test_topic_from_kafka.sh
```
which downloads a kafka version so we can thereafter run console-consumer.sh

Note that our kafka installation seemes to have an issue with this, and I could only get the messages by running
an older version using --zookeeper instead of the newer --bootstrap-server.
There was a mentioning [online](http://stackoverflow.com/questions/41774446/kafka-bootstrap-servers-vs-zookeeper-in-kafka-console-consumer)
of people experiencing similar problems...but leaving the newer version for now

Extra
-----
This folder also contains some .log example messages from the raw RM messages as well as a set_env.sh script (this is not strictly necessary to run anymore)
