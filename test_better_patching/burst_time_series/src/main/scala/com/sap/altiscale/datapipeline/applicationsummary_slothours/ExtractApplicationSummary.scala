package com.sap.altiscale.datapipeline.applicationsummary_slothours

import java.util.regex.Pattern

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{InputDStream, DStream}
import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.{SparkEnv, TaskContext}

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.collection.Map
import scala.util.parsing.json.{JSON, JSONObject}
import scala.reflect.ClassTag

import org.slf4j.Logger
import org.slf4j.LoggerFactory


class ExtractApplicationSummary {
    val logger = LoggerFactory.getLogger(classOf[ExtractApplicationSummary])
}

object ExtractApplicationSummary {
    val dc = new ExtractApplicationSummary

    /*
    Parse messages in JSON format - filters out unparsable messages
    performance concerns:
      * Maps are 'heavy' - consider light weight list
      * Returning entire parsed JSON RDD can incur memory overhead (consider direct filtering)
        and parsing twice otherwise
    Input:
        dpci    {"fullmessage":"2017-04-27 14:46:22,518 INFO org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary: appId=application_1492563482964_0024,name=select count(*) from ...line_dev.cluster_dim(Stage-1),user=alti-test-01,queue=default,state=FINISHED,trackingUrl=http://rm-dpci.s3s.altiscale.com:8088/proxy/application_1492563482964_0024/,appMasterHost=203-09-01-c02.sc1.altiscale.com,startTime=1493304344446,finishTime=1493304375842,finalStatus=SUCCEEDED,memorySeconds=95465,vcoreSeconds=54,preemptedAMContainers=0,preemptedNonAMContainers=0,preemptedResources=<memory:0\\, vCores:0>,applicationType=MAPREDUCE","path":"/var/hadoop/mapred/log/yarn-yarn-resourcemanager-200-11-08.sc1.verticloud.com.log","cluster":"dpci","@timestamp":"2017-04-27T14:46:23.037Z","@version":"1","host":"200-11-08.sc1.verticloud.com","time":"","priority":"","category":"","message":""}
    Output:
        dpci    
            Map(
                "fullmessage" -> "...",
                "path" -> /var/hadoop/mapred/log/yarn-yarn-resourcemanager-200-11-08.sc1.verticloud.com.log",
                "cluster" -> "dpci",
                ,"@version" -> "1",
                "host" -> "200-11-08.sc1.verticloud.com",
                "time" -> "",
                "priority" -> "",
                "category" -> "",
                "message" -> ""
                )
    */
    def parseJSON2Map(messages: InputDStream[(String, String)]): DStream[(String, Map[String, String])] = {
        messages.flatMap {case(key, value) =>
            val jsonStructure = JSON.parseFull(value)
            jsonStructure match {
                case Some(e: Map[_,_]) => {
                    Some((key, e.asInstanceOf[Map[String, String]]))
                }
                case _ => {
                    dc.logger.warn("Unparsable message:\n" + value)
                    None
                }
            }
        }
    }


    /*
    Parse to log4j format - add cluster to output in case keys and cluster names differ and filter out unparsable messages
    Input:
        See parseJSON2Map output
    Output:
        dpci
            List(
                "dpci",
                "2017-04-27 14:46:22,518",
                "INFO",
                "org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary:,
                appId=application_1492563482964_0024,name=select count(*) from ...line_dev.cluster_dim(Stage-1),user=alti-test-01,queue=default,state=FINISHED,trackingUrl=http://rm-dpci.s3s.altiscale.com:8088/proxy/application_1492563482964_0024/,appMasterHost=203-09-01-c02.sc1.altiscale.com,startTime=1493304344446,finishTime=1493304375842,finalStatus=SUCCEEDED,memorySeconds=95465,vcoreSeconds=54,preemptedAMContainers=0,preemptedNonAMContainers=0,preemptedResources=<memory:0\\, vCores:0>,applicationType=MAPREDUCE"
            )
    */
    def parselog4J(messages: DStream[(String, Map[String, String])], log4jpattern: Pattern): 
            DStream[(String, List[String])] = {
        messages.flatMap {case(key, value) =>
            var m = log4jpattern.matcher(value("fullmessage"))
            if (m.matches()) {
                val datetime: String = m.group(1)
                val priority: String = m.group(2)
                val category: String = m.group(3)
                val logMessage: String = m.group(4)
                val outputList = List(value("cluster"), datetime, priority, category, logMessage)
                dc.logger.debug("Log4j parsed message: " + outputList.mkString("\t"))
                Some((key, outputList))
            }
            else {
                dc.logger.warn("No matches found for:\n" + value + "\nUsing pattern: " + log4jpattern)
                None 
            }
        }
    }

    // Select only messages which has a defined type
    def filterMessages(messages: DStream[(String, List[String])], filterMsgType: String): 
            DStream[(String, List[String])] = {
        // Filtering on category
        messages.filter {case(key, value) => value(3) == filterMsgType}
    }

    /* Extracts cluster, time and slothour value from apache log message
    Input:
        See parselog4J output
    Output:
        dpci
            List(
                "dpci",
                1493304375842.0,
                95465.0
            )
    */
    def extractColumns(messages: DStream[(String, List[String])]) : DStream[(String, List[Any])] = {
        messages.map {case(key, value)  =>
            val cluster = value(0)
            // Parsing out the individual components of the ApplicationSummary message
            val logMessage = value(4)
            val msgComponents = logMessage.split("(?<!\\\\),")
            // HashMaps are potentially slow, could just index...
            var logMsgMap = new HashMap[String, String]()
            msgComponents.foreach { m =>
                // Split only on first '='
                val keyval = m.split("=",2)
                logMsgMap.put(keyval(0), keyval(1))
            }

            // Doubles, since finishtime would require bigints
            (key, List(cluster, logMsgMap("finishTime").toDouble, logMsgMap("memorySeconds").toDouble/2.5/1000/3600))
        }
    }

    /* Hardcoded message structure - changes finishtime to minute start of input
    Input:
        See extractColumns output
    Output:
        dpci
            List(
                "dpci",
                1493304360,
                95465.0
            )
    TODO: Consider change this to a method that attempts to group by minute
    */
    def timestampToMinuteStart(messages: DStream[(String, List[Any])], batchInterval: Duration): DStream[(String, List[Any])] = {
        messages.transform { (rdd, time) =>
            val minuteStart = ((time.milliseconds/batchInterval.milliseconds*batchInterval.milliseconds)/1000).toInt
            rdd.map { case(key, value) => (key, List(value(0), minuteStart, value(2)))}
        }
    }

    // Tag on extra information to RDD
    def extendColumns(messages: DStream[(String, List[Any])], extras: List[Any]) : DStream[(String, List[Any])] = {
        messages.map {case(key, value) => (key, value ++ extras) }
    }

    // Convert messages to json format
    def messagesToJSON(messages: DStream[(String, List[Any])], schema: List[String]) : DStream[(String, String)] = {
        messages.map {case(key, value) =>
            val jsonstr = JSONObject((schema zip value).toMap).toString()
            dc.logger.debug("Executor: '" + SparkEnv.get.executorId + "'\tMessage key: '" + 
                    key + "'\tRDD partition id: " + TaskContext.get.partitionId + "\tMessage: " + jsonstr)
            (key, jsonstr)
        }
    }

    // Sum slothours and take maximum finishminute
    def aggregateSlothoursByKey[T <: Any](messages: DStream[(T, List[Any])])
            (implicit m: ClassTag[T]): DStream[(T, List[Any])] = {
        messages.reduceByKey { (a: List[Any], b: List[Any]) => 
            val cluster = a(0)
            val finishminute = math.max(a(1).asInstanceOf[Double], a(2).asInstanceOf[Double])
            val slothours = a(2).asInstanceOf[Double] + b(2).asInstanceOf[Double]
            List(cluster, finishminute, slothours)
        }
    }
}
