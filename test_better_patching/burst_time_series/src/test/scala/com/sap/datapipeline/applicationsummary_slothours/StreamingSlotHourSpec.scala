/* 
Test that the slothour metrics pipeline work as expected by simulating SPARK dstream with a Queue
The spark streaming progress is controlled with a 'Fixed' clock
Upon advancement, this clock manually triggers new batch processing
However, in SPARK 2.x, we still need to wait for SPARK to finish executing to get the results
The clock is only a 'trigger' that queues up computations

Pickle JSON format is used for all test cases input and output data
A few useful lines to get the output in pickle json for a test case:
    Thread.sleep(batchDuration.milliseconds)
    println(appSummaryLst)
    println("PCKL:\n" + appSummaryLst.toList.pickle)
*/

package com.sap.altiscale.datapipeline.applicationsummary_slothours

// For maven resource laoding
import java.io.{InputStream, PrintWriter}
import java.util.regex.Pattern

import scala.io.Source
import scala.collection.mutable.{Queue, ListBuffer, Map}

import org.apache.spark.{FixedClock, SparkConf, SparkContext}
import org.apache.spark.streaming.{Clock, Seconds, StreamingContext, Duration}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.rdd.RDD

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers, GivenWhenThen}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}

// Pickle reading in json format
import scala.pickling._
import json._

// Tests for some of the ExtractApplicationSummary functions
class SparkStreamingExampleSpec extends FlatSpec with BeforeAndAfter with GivenWhenThen with Eventually {
    // Create a local spark cluster of two or more executors (minimum since straming runs in separate thread)
    val master = "local[2]"
    val appName = "TestStreamingSlotHourSummary"

    // SPARK operation related
    var sc: SparkContext = _
    var ssc: StreamingContext = _
    var fixedClock: FixedClock = _
    var batchDuration : Duration = _

    // Set higher than default timeout for all assert waits in the 'eventually' trait
    override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(3000, Millis)))

    // Set up a single spark context for all tests
    before {
        // Spark vill process streams in batches of 1 second
        batchDuration = Seconds(1)
        val conf = new SparkConf()
            .setMaster(master)
            .setAppName(appName)
            .set("spark.streaming.clock", "org.apache.spark.FixedClock")
        ssc = new StreamingContext(conf, batchDuration)
        fixedClock = Clock.getFixedClock(ssc)
    }

    // Use a queueStream to simulate kafka
    trait KafkaSimulater[T, R] {
        val messages = new Queue[RDD[(T, R)]]()
        val dstream = ssc.queueStream(messages)
    }

    // Stop the sparkcontext non gracefully - its faster since we don't care about data losing
    after {
        if (ssc != null) {
            ssc.stop(stopSparkContext = true, stopGracefully = false)
        }
    }

    // Read file via getRousource as custom in maven
    def readFile(filename: String): List[String] = {
        val stream : InputStream = getClass.getResourceAsStream(filename)
        Source.fromInputStream(stream).getLines.toList
    }

    // Read file for unpickling
    def readPickledJSONfile(filename: String): String = {
        readFile(filename).mkString("")
    }

    // Read file and parse to tuple on tab 
    def readKafkaConsoleFile(filename: String): List[(String, String)] = {
        readFile(filename).map {line => (line.split("\t", 2)(0), line.split("\t", 2)(1))}
    }

    def getRDDmsgs[T](inrdd: DStream[(String, T)], appSummaryLst: ListBuffer[(String, T)]): Unit = {
        inrdd.foreachRDD {rdd => appSummaryLst ++= rdd.collect().toList}
    }

    // Move clock forward a batch interval period
    def waitBatchTime(): Unit = {
        wait(batchDuration)
    }

    // Move clock forward
    def wait(time: Duration): Unit = {
        fixedClock.addTime(time)
    }



    //--------------------------------------------------Tests--------------------------------------------------//
    "Application summary parse methods" should "be able to extract log4j info" in new KafkaSimulater[String, String] {
        // The epxpected output
        val expectedMsgs = readPickledJSONfile("/example_streaming_parsed.pkl").unpickle[List[(String, List[String])]]
        var appSummaryLst = ListBuffer.empty[(String, List[String])]

        // Parsing out the Log4J needs a reexp off <datetime> <priority> <category> <message>
        val regex = "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) ([^\\s]+) (.*): (.*)$"
        val log4jpattern = Pattern.compile(regex)

        // Parsing to log4J format
        val parsedMessages = ExtractApplicationSummary.parseJSON2Map(dstream)
        val parsedlog4Jmsgs = ExtractApplicationSummary.parselog4J(parsedMessages, log4jpattern)

        parsedlog4Jmsgs.foreachRDD {rdd => appSummaryLst ++= rdd.collect().toList}

        // Collect filtered messages from SPARK
        getRDDmsgs(parsedlog4Jmsgs, appSummaryLst)
        // Start streaming through the defined stages
        ssc.start()

        // Append data to the 'kafka' queue from resource folder
        val messagesTuple = readKafkaConsoleFile("/example_streaming_input.txt")
        messages += ssc.sparkContext.makeRDD(messagesTuple)

        // Move virtual time forward so spark streaming will process its batch
        waitBatchTime()

        // Make sure arrays are the same - need eventually since changing the time will
        // correctly change pass the batchwindow, but SPARK still need time to compute the results
        // in its separate thread
        eventually {
           assert(appSummaryLst.equals(expectedMsgs), "\nGOT:\n" + appSummaryLst.mkString("\n") + 
                                                      "\nEXPECTED:\n" + expectedMsgs.mkString("\n"))
        }
    }


    //--------------------------------------------------------------------------------------------------------//
    "An applicationsummary extraction filter" should "return only applicationsummary messages" in new KafkaSimulater[String, List[String]] {
        // The epxpected output
        val expectedMsgs = readPickledJSONfile("/example_streaming_filtered.pkl").unpickle[List[(String, List[String])]]
        var appSummaryLst = ListBuffer.empty[(String, List[String])]

        // Spark streaming filtering     
        val filterMsgType = "org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary"
        val appSummaryMsgs = ExtractApplicationSummary.filterMessages(dstream, filterMsgType)

        // Gather info from spark RDD's
        getRDDmsgs(appSummaryMsgs, appSummaryLst)
        ssc.start()

        // Append data to the 'kafka' queue from resource folder
        val messagesTuple = readPickledJSONfile("/example_streaming_parsed.pkl").unpickle[List[(String, List[String])]]
        messages += ssc.sparkContext.makeRDD(messagesTuple)

        // Move virtual time forward so spark streaming will process its batch
        waitBatchTime()

        // Compare extracted result against results read from file
        eventually {
            assert(appSummaryLst.equals(expectedMsgs), "\nGOT:\n" + appSummaryLst.mkString("\n") + 
                                                       "\nEXPECTED:\n" + expectedMsgs.mkString("\n"))
        }
    }


    //--------------------------------------------------------------------------------------------------------//
    "A applicationsummary extraction" should "return cluster, time, tag and slothour in JSON format" in new KafkaSimulater[String, List[String]] {
        // The epxpected output
        val expectedMsgs = readPickledJSONfile("/example_streaming_extract.pkl").unpickle[List[(String, List[Any])]]
        var appSummaryLst = ListBuffer.empty[(String, List[Any])]

        // Collect info from application summary messages
        val tag = "slothours-by-minute"
        val slothourMessages = ExtractApplicationSummary.extractColumns(dstream)

        // Gather info from spark RDD's
        getRDDmsgs(slothourMessages, appSummaryLst)
        ssc.start()

        // Append data to the queue from resource folder
        val messagesTuple = readPickledJSONfile("/example_streaming_filtered.pkl").unpickle[List[(String, List[String])]]
        messages += ssc.sparkContext.makeRDD(messagesTuple)

        // Move virtual time forward so spark streaming process its batch
        waitBatchTime()

        // Compare extracted result against results read from file
        eventually {
            assert(appSummaryLst.equals(expectedMsgs), "\nGOT:\n" + appSummaryLst.mkString("\n") + 
                                                       "\nEXPECTED:\n" + expectedMsgs.mkString("\n"))
        }
    }


    //--------------------------------------------------------------------------------------------------------//
    "An aggregation by key" should "group messages by SPARK processing time" in new KafkaSimulater[String, List[Any]] {
        // The epxpected output
        val expectedMsgs = readPickledJSONfile("/example_streaming_grouped.pkl").unpickle[List[(String, List[Any])]]
        var appSummaryLst = ListBuffer.empty[(String, List[Any])]

        // Group all in current RDD
        val slothourSummed = ExtractApplicationSummary.aggregateSlothoursByKey[String](dstream)

        // Gather info from spark RDD's
        getRDDmsgs(slothourSummed, appSummaryLst)
        ssc.start()

        // Append data to the queue from resource folder
        val messagesTuple = readPickledJSONfile("/example_streaming_extract.pkl").unpickle[List[(String, List[Any])]]

        // Pusch first batch of data
        messages += ssc.sparkContext.makeRDD(messagesTuple)
        waitBatchTime()

        // Push in a second batch of data
        messages += ssc.sparkContext.makeRDD(messagesTuple)
        waitBatchTime()

        // Compare extracted result against results read from file
        eventually {
            assert(appSummaryLst.equals(expectedMsgs), "\nGOT:\n" + appSummaryLst.mkString("\n") + 
                                                       "\nEXPECTED:\n" + expectedMsgs.mkString("\n"))
        }
    }


    //--------------------------------------------------------------------------------------------------------//
    // NOTE that output is dependent on batch interval. Changing it will fail the test.
    "A low latency pipeline" should "add a timestamp for each batch interval" in new KafkaSimulater[String, List[Any]] {
        // The epxpected output
        val expectedMsgs = readPickledJSONfile("/example_streaming_timestamped.pkl").unpickle[List[(String, List[Any])]]
        var appSummaryLst = ListBuffer.empty[(String, List[Any])]

        // Group all in current RDD
        val slothourSummed = ExtractApplicationSummary.timestampToMinuteStart(dstream, batchDuration)

        // Gather info from spark RDD's
        getRDDmsgs(slothourSummed, appSummaryLst)
        ssc.start()

        // Append data to the queue from resource folder
        val messagesTuple = readPickledJSONfile("/example_streaming_grouped.pkl").unpickle[List[(String, List[Any])]]

        // Push two groups of messages
        messages += ssc.sparkContext.makeRDD(messagesTuple)
        waitBatchTime()
        messages += ssc.sparkContext.makeRDD(messagesTuple)
        waitBatchTime()

        // Compare extracted result against results read from file
        eventually {
            assert(appSummaryLst.equals(expectedMsgs), "\nGOT:\n" + appSummaryLst.mkString("\n") + 
                                                       "\nEXPECTED:\n" + expectedMsgs.mkString("\n"))
        }
    }
}
