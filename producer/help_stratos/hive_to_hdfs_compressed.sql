-- export HADOOP_CLASSPATH=/opt/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-distcp-2.7.1.jar

set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set mapred.output.compression.type=BLOCK;
set hive.exec.copyfile.maxsize=100000000000;
set hive.cli.print.header=true;

--INSERT OVERWRITE DIRECTORY '/user/tnystrand/stratos_1/1_request_series' select * from thomas_tostratos.stratos_1;
--INSERT OVERWRITE DIRECTORY '/user/tnystrand/stratos_1/2a_percontainer' select * from thomas_tostratos.stratos_2a_pertask;
INSERT OVERWRITE DIRECTORY '/user/tnystrand/stratos_1/2b_perjob' select * from thomas_tostratos.stratos_2a_perjob;
--INSERT OVERWRITE DIRECTORY '/user/tnystrand/stratos_1/3_host_series' select * from thomas_tostratos.stratos_3;

