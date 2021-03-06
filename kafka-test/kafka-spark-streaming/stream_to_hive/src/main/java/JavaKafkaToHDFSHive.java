/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with

 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import scala.Tuple2;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.avro.io.JsonDecoder;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ run_local.sh JavaKafkaToHDFSHive broker1-host:port,broker2-host:port \
 *      topic1,topic2
 */

public final class JavaKafkaToHDFSHive {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: JavaKafkaToHDFSHive <brokers> <topics>\n" +
          "  <brokers> is a list of one or more Kafka brokers\n" +
          "  <topics> is a list of one or more kafka topics to consume from\n\n");
      System.exit(1);
    }

    int debugPrint = 1;

    // StreamingExamples.setStreamingLogLevels();

    String groupID = "dp_test_to_hive";
    String brokers = args[0];
    String topics = args[1];

    System.out.println("Brokers: " + brokers);
    System.out.println("Topics: " + topics);

    // Create context with a 2 seconds batch interval

    SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaToHDFSHive");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    HiveContext hc = new HiveContext(sc);
    JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(10));

    Set<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
    Map<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list", brokers);
    kafkaParams.put("group.id", groupID);

    // Create direct kafka stream with brokers and topics
    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
        jssc,
        String.class,
        String.class,
        StringDecoder.class,//JsonDecoder.class,
        StringDecoder.class,//JsonDecoder.class,
        kafkaParams,
        topicsSet
    );

   VoidFunction < Tuple2 < String, String > > keyData = new VoidFunction < Tuple2 < String, String > > () {
        public void call(Tuple2<String, String> line) {
           System.out.println(line._1() + " : " + line._2());
        }
    };

   VoidFunction < Tuple2 < String, String > > keyData = new VoidFunction < Tuple2 < String, String > > () {
        public void call(Tuple2<String, String> line) {
           System.out.println(line._1() + " : " + line._2());
        }
    };


    Function <>

    //messages.foreach(keyData);
    messages.foreachRDD(keyData);

    //if (debugPrint>0) {
    //    Map<String, String> localResultSet = messages.collectAsMap();
    //    for(Map.Entry<String, String> entry : localResultSet.entrySet()) {
    //        System.out.println(entry.getKey() + " : " + entry.getValue());
    //    }
    //}

    //PairFunction<String, String, String, String> keyData = new PairFunction<String, String, String, String>() {
    //    @override
    //    public Tuple2<String, String, String> call(Tuple2<String, String> line) {
    //        return new Tuple2(line._1(), line._2());
    //    }
    //};

    //JavaDStream<String, String> pairs = messages.mapToPair(keyData);


    

    //// Get the lines
    //JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
    //  @Override
    //  public String call(Tuple2<String, String> tuple2) {
    //    return tuple2._2();
    //  }
    //});

    //JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
    //  @Override
    //  public Iterable<String> call(String x) {
    //    return Arrays.asList(x.split(" "));
    //  }
    //});
    //JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
    //  new PairFunction<String, String, Integer>() {
    //    @Override
    //    public Tuple2<String, Integer> call(String s) {
    //      return new Tuple2<String, Integer>(s, 1);
    //    }
    //  }).reduceByKey(
    //    new Function2<Integer, Integer, Integer>() {
    //    @Override
    //    public Integer call(Integer i1, Integer i2) {
    //      return i1 + i2;
    //    }
    //  });
    //wordCounts.print();

    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }
}

