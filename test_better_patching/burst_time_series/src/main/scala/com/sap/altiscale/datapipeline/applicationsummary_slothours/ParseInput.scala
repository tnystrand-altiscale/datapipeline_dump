package com.sap.altiscale.datapipeline.applicationsummary_slothours

case class Config(  inputTopics: String = "",
                    outputTopic: String = "",
                    kafkaBrokers: String = "",
                    checkpointDirectory: String = "",
                    processingInterval: Int = -1,
                    checkpointInterval: Int = 60,
                    nameTag: String = "",
                    collectAndPrintToDriver: Boolean = false,
                    enableCheckpointing: Boolean = false)

object ParseInput {
    def parse(args: Array[String]): Config = {
        // Checking input arguments
        val parser = new scopt.OptionParser[Config]("scopt") {
            head("Applicationsummary slothour filtering")

            opt[String]('i', "inputTopics").required().action( (x, c) =>
                c.copy(inputTopics = x) ).text("Topic to read from in kafka")
            opt[String]('o', "outputTopic").required().action( (x, c) =>
                c.copy(outputTopic = x) ).text("Topic to write to in kafka")
            opt[String]('k', "kafkaBrokers").valueName("<broker:port,...>").required().action( (x, c) =>
                c.copy(kafkaBrokers = x) ).text("Comma separated list of borkers")
            opt[String]('c', "checkpointDirectory").required().action( (x, c) =>
                c.copy(checkpointDirectory = x) ).text("HDFS dir to checkpoint spark data")
            opt[Int]('r', "processingInterval").required().action( (x, c) =>
                c.copy(processingInterval = x) ).text("The batch interval for SPARK streaming processing")
            opt[Int]('r', "checkpointInterval").action( (x, c) =>
                c.copy(checkpointInterval = x) ).text("The interval for writing checkpoints - " + 
                    "can have adverse effects if lower than batch interval, but is resource heavy if set to faster...")
            opt[String]('n', "nameTag").required().action( (x, c) =>
                c.copy(nameTag = x) ).text("Give job a useful name tag")
            opt[Unit]('e', "enableCheckpointing").action( (_, c) =>
                c.copy(enableCheckpointing = true) ).text("Enable checkpointing - might be good to leave false for testing")
            opt[Unit]('p', "collectAndPrintToDriver").action( (_, c) =>
                c.copy(collectAndPrintToDriver = true) ).text("Collects all messages and 'prints' them through the logger")
        }

        // Create config to store parsed input
        var config : Config = Config()

        // Parse the input arguments 
        parser.parse(args, config) match {
            case Some(c) =>
                config = c
            case None =>
                // arguments are bad, error message will have been displayed
                System.exit(1)
        }
        config
    }
}
