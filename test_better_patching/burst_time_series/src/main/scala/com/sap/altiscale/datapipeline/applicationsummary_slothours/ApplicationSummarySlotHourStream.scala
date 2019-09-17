package com.sap.altiscale.datapipeline.applicationsummary_slothours

import java.util.regex.Pattern

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

import kafka.serializer.StringDecoder

import org.apache.spark.{SparkConf, SparkContext, TaskContext, SparkEnv}
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka.KafkaUtils

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.streaming.Time
import org.apache.spark.rdd.RDD

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ApplicationSummarySlotHourStream {
    val logger = LoggerFactory.getLogger(classOf[ApplicationSummarySlotHourStream])
}

object ApplicationSummarySlotHourStream {
    // For logging purposes
    val as = new ApplicationSummarySlotHourStream

    // If printing Ouput
    var collectAndPrintToDriver = false
    val dbgprint ="DBGPRINT -- "

    // Spearate streaming pipe into a function for checkpointing
    def createStreamingContext(inputTopics: String, writeTopic: String, kafkaBrokers: String, checkpointDirectory: String, 
                               processingInterval: Int, checkpointInterval: Int,
                               appName: String, filterMsgType: String, tag: String): StreamingContext = {
        // Configuration for Spark run
        val conf = new SparkConf().setAppName(appName)
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        as.logger.debug("Running with appname: " + appName)

        // Batch intercal of x seconds (DStream will send info to executors in batches each second)
        val batchInterval = Seconds(processingInterval)
        val ssc = new StreamingContext(conf, batchInterval)
        if (enableCheckpointing)
            ssc.checkpoint(checkpointDirectory)

        // Create direct kafka stream with brokers and topics
        val topicsSet = inputTopics.split(",").toSet
        val kafkaParams = Map[String, String](
            "metadata.broker.list" -> kafkaBrokers,
            "auto.offset.reset" -> "largest")
        as.logger.debug("KafkaParameters: " + kafkaParams)

        // Print batch timestamp
        if (collectAndPrintToDriver) messages.transform { (rdd, time) => 
                val minutestart = ((time.milliseconds/batchInterval.milliseconds*batchInterval.milliseconds)/1000).toInt
                rdd.map({ _ =>
                    dbgprint + 
                    "\n=============================================================\n" +
                    "Start of new batch with timestamp: " + minutestart + " (" + time.milliseconds  + ")"
                })}.foreachRDD{_.take(1).toList.foreach{ as.logger.info(_) }}
        
        // Print input
        if (collectAndPrintToDriver) {
            messages.map{ msg =>
                "Executor: '" + SparkEnv.get.executorId + 
                "'\tRDD partition id: " + TaskContext.get.partitionId + 
                "'\tMessage key: '" + msg._1 + 
                "\tMessage: " + msg._2
            }.foreachRDD{x =>
                    as.logger.info(dbgprint + "\n\tINPUT MESSAGES:\n\t" + x.collect().toList.mkString("\n\t") + "\n")
            }
        }


        // Filter out relevant messages and parse the log4j format to get slothour information
        val parsedMessages = ExtractApplicationSummary.parseJSON2Map(messages)
        val parsedlog4J = ExtractApplicationSummary.parselog4J(parsedMessages, log4jpattern)
        val appSummarymsgs = ExtractApplicationSummary.filterMessages(parsedlog4J, filterMsgType)
        val slothoursInfo = ExtractApplicationSummary.extractColumns(appSummarymsgs)

        // Print output
        if (collectAndPrintToDriver) {
            slothoursJSON.foreachRDD {x =>
                as.logger.info( dbgprint + "\n\tOUTPUT MESSAGES:\n\t" + x.collect().toList.map {y =>
                    y._1 + ": " + y._2
                }.mkString("\n\t") + "\n")
            }
        }

        ssc
    }

    def main(args: Array[String]): Unit = {
        as.logger.info("Running applicationsummary slothour Spark Streaming processing application")
        as.logger.debug("Arguments: \n" + args.mkString("\n"))

        // Parse input arguments
        val conf = ParseInput.parse(args)
        as.logger.debug("Parsed arguments: " + conf)

        // Set instanace variables
        collectAndPrintToDriver = conf.collectAndPrintToDriver
        val nameTag = conf.nameTag

        // Application settings
        val filterMsgType = "org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary"
        val tag = "slothours-by-minute"
        val appName = "ApplicationSummarySlotHourStream[" + nameTag + "]"

        // Create new context from checkpointing if not reusing the producer
        // Start a new streaming context or recover from checkpoint
        var ssc = {  
            if (enableCheckpointing) {
                as.logger.info("Reusing or creating new streaming context")
                StreamingContext.getOrCreate(conf.checkpointDirectory, 
                    () => createStreamingContext(conf.inputTopics, 
                                                 conf.outputTopic, 
                                                 conf.kafkaBrokers, 
                                                 conf.checkpointDirectory, 
                                                 conf.processingInterval,
                                                 conf.checkpointInterval,
                                                 appName, 
                                                 filterMsgType, 
                                                 tag))
            }
            else {
                as.logger.info("Ceating a new streaming context")
                createStreamingContext(conf.inputTopics, 
                                       conf.outputTopic, 
                                       conf.kafkaBrokers, 
                                       conf.checkpointDirectory, 
                                       conf.processingInterval,
                                       conf.checkpointInterval,
                                       appName, 
                                       filterMsgType, 
                                       tag)
            }
        }

        as.logger.info("Starting the direct spark streaming")
        ssc.start()
        ssc.awaitTermination()
        as.logger.info("Slothour processing stopped")
    }
}
