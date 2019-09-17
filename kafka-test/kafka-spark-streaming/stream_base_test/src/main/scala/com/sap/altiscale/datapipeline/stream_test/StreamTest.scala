package com.sap.altiscale.datapipeline.stream_test;

import scala.collection.JavaConversions._

import kafka.serializer.StringDecoder

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata, ProducerConfig}

import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import java.util.HashMap

object StreamTest {
    def main(args: Array[String]): Unit = {
        val appName = "StreamFromAndToKafka"
        val conf = new SparkConf().setAppName(appName)

        // Batch intercal of 1 second (DStream will send info to executors in batches each second)
        val ssc = new StreamingContext(conf, Seconds(1))
        ssc.checkpoint("checkpoint")

        // Kafka parameters
        val topics = "resourcemanager"
        val kafkaBrokers = "kafka01-sc1.service.altiscale.com:9092"
        val writeTopic = "filtered_RM_on_dogfood"


        // Create direct kafka stream with brokers and topics
        val topicsSet = topics.split(",").toSet
        val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokers)
        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
              ssc, kafkaParams, topicsSet)

        // Dummy filtering
        val filteredMessages = messages.filter {line => 
           line._1 == "rm-dogfood"
        }
        filteredMessages.print()

        val props = new HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer")


        // Code from http://allegro.tech/2015/08/spark-kafka-integration.html
        val kafkaSink = ssc.sparkContext.broadcast(KafkaSink(props.toMap))
        messages.foreachRDD { rdd =>
            rdd.foreach { message =>
                kafkaSink.value.send(writeTopic, message._1, message._2)
            }
        }

        // Start the computation
        ssc.start()
        ssc.awaitTermination()
    }
}


// Code from http://allegro.tech/2015/08/spark-kafka-integration.html
// Avoids creating many kafka producers per RDD by lazy evaluation
// KafkaProducer is not serializable due to open sockets, hence lazy eval once sent to executors and avoid
// reopening for each new version batach (and partition). KafkaProducer is thread safe.
class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {
    lazy val producer = createProducer()
    def send(topic: String, key: String, value: String): Unit = producer.send(new ProducerRecord(topic, key, value))
}

object KafkaSink {
    def apply(config: Map[String, Object]): KafkaSink = {
        val f = () => {
            val producer = new KafkaProducer[String, String](config)
            // Make sure all messages gets flushed before JVM shutdown
            sys.addShutdownHook {
                producer.close()
            }
            producer
        }
        new KafkaSink(f)
    }
}
