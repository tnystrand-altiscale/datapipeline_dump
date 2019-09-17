###Application Summary Streaming Slot Hours
###----------------------------------------

####What it does:

####Caveat:

####Testing:

Compiling:
```
mvn compile
```

Testing:
```
mvn clean test
```

To move jars into target folder and build a stand-alone jar:
```
mvn install
```

To run the program against altiscale kafka instance:
```
./run_local.sh
```

Note:
If getting an error similar to:
```Exception in thread "main" java.lang.NoSuchMethodError: scala.Predef$.ArrowAssoc(Ljava/lang/Object;)Ljava/lang/Object;```
It is necessary to run
```
source ./set_env.sh
```
ahead of run_local (this is for workbenches that still use spark < v1.6)
Also: *if* running from new build, always clean up checkpoint directory
```
hdfs dfs -rm -r <checkpointdir>
```

Running SPARK locally is used for debugging (since it does not need installing and uploading jars every developing cycle)

To run in yarn-client or yarn-cluster mode:
```
./run_client.sh
```
or (for simulating production)
```
./run_cluster.sh
```

If the altiscale kafka services are for some reason unreachable, the program
can still be tested by running:
```
./run_offline.sh
```
This will publish and read messages from kafka on localhost.
To start kafka on localhost run in given order:
```
../run_local/kafka_zoo.sh
../run_local/kafka_boot.sh
```

Client mode runs the SPARK driver on the client
Cluster mode runs the SPARK driver on the cluster, inside a YARN container
Offline mode runs on local machine

In client mode all required jars will be uploaded to the cluster through the client
In cluster mode all jars should be available on the cluster - this means they can either be uploaded manually before launch or through spark submit
See [altiscale doc](https://documentation.altiscale.com/spark-2-0-with-altiscale) for instance 

Cluster mode enables YARN to handle driver restart upon failure
Terminating the application in cluster mode requires killing the application in YARN

Production run
--------------
Run run_production.sh
Possible tweak number of reattempts
Possible tweak n exectures, n cores and sixes

Extra
-----
To run the following scripts, first run:
```
../get_kafka_for_testing.sh
```
, which downloads a kafka version under ./kafka
./read_kafka_logstash_rm_logs.sh is used to verify that there are values in the input kafka topic
./list_kafka_topics.sh shows all currently registered kafka topics
./read_kafka_cluster_metrics_json_test.sh is used to verify there are values in the output kafka topic

rdir, wdir, tdir are symlinks into the maven tree structure

./send_appsumaries_to_kafka.sh sends mock application summary messages to kafka input topic - used for debugging

Unit Tests
----------
Testing RDDs required simulating kafka and run spark locally
Some useful links which explains the idea behind the testing:
(Fixed clock)[http://blog.ippon.tech/testing-strategy-for-spark-streaming/]
(Checkpoint example)[https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/RecoverableNetworkWordCount.scala]


Logging
-------
* App name must have name production for production runs to separate from other runs in RM for maintenance etc..
* Logs in local mode are all sent to console
* Logs in yarn client mode are split into driver and executor
** Driver logs go to console
** Execturor logs go to /logs/spark-history/${user.name}/<application_id>. These can also be accessed via yarn resourcemanger through yarn logs -applicationId <application_id> | less or resourcemanager UI
* Logs in yarn cluster mode for both driver and executor are sent to their respective containers. All logs go to /logs/spark-history/${user.name}/<application_id> but can also be accessed as the executor logs in client mode
* To changing logging before a run, set appropriate environmental variables. APPSUM_LOGLEVEL, APPSUM_DRIVER_LOGLEVEL, APPSUM_EXECUTOR_LOGLEVEL. The default behavior is WARN for run_cluster.sh (production runs)

Checkpointing:
-------------
Run without checkpointing if running run_cluster, run_local and run_client interleaved since various issues will likely result

TODO
----
* Dynamic allocation: check that this works: spark.dynamicAllocation.enabled=true and spark.shuffle.service.enabled=true
* com.sap.altiscale.datapipeline.applicationsummary_slothours.ExtractApplicationSummary does not follow log4j pattern -> gives warnings
    account for this in code?
* Research dynaminc cluster configuration
* Test for recovering from checkpoint
* SPARK version resiliance
* What does the checkpoint exactly do? Put everything to HDFS?
* AVRO support?
* Tune Spark checkpoint interval
* Package name BDS?
* Restructure code from having a library of SPARK methods to library of lambdas, update test code accordingly
* Possible reason for checkpointing failing with KafkaSinks are the broadcast variables are not reiniilaized
  Need to initilize singelton objects
* Change code so it does not lump all minutes together after recovering from checkpoint
    Some comments is left in:

    commit 0d27c0279201443823d5b47e86e67c5b657adb36
    Author: Thomas Nystrand <thomas_nystrand@hotmail.com>
    Date:   Mon Jun 19 14:05:26 2017 +0200

        ApplicationSummary debug commit history

    To add the 'last -previous- minutes slothour' there are three choices:*
    1) Collect everything on driver/process and send to kafka
       + Easy to debug and maintain code
       + Avoid potential 'overkill' solutions
       - Slow for big data (100 clusters*1min is tiny - but if we want to catch up from failure...)
    2) Group all minute messages in the same executor and loop over minutes
       + Fast for small groups of per minute messages
       - Requires shuffling the data
       - More scalable than sending to driver but could still potentially crash if SPARK ctaches up from old checkpoint
         (60*24*7 = 10080 entries for one week)
    3) Use SPARK built in functions exclusively. Max to find latest minute. Filter out rest and send.
       + Scales well
       - Requires a join operation, which is inneficient since SPARK shuffles data internally
       Get the maximum minutes to rollover

    *Considering alt 2) and alt 3)*
    If grouping in 2) start creating huge lists and slows down the process,
      we can achieve the same by finding the maximum and minimum minutes of the
      seprate partitions. Broadcasted value is added to RDD. Maximum is added
      to new broadcast map. Filter rdd with minimum to remove maximum.
      Send everything from this rdd to kafka.

    *Either way we need to construct a feedback loop*
    This could/should be a broadcast variable for faster execution - but two things are required:
    1) Checkpointing dictates that it must be wrapped in a lazy-transient variable in a singelton object
       This must be recreated when recovering from checkpoints
       See: https://issues.apache.org/jira/browse/SPARK-5206 (last comment)
    2) Must update the variable on every batch turn - broadcasts are however declared as final
       and we must therefore recreate the spark context (or unpersist it and rebroadcast?)
       See: https://stackoverflow.com/questions/33372264/how-can-i-update-a-broadcast-variable-in-spark-streaming 
