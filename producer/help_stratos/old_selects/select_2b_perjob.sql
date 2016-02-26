--SET mapred.output.compress=true;
--SET hive.exec.compress.output=true;
--SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
--SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
--SET mapred.output.compression.type=BLOCK;
--
--INSERT overwrite '2a_perjob' select * from thomas_tostratos.stratos_2a_perjob;

-- export HADOOP_CLASSPATH=/opt/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-distcp-2.7.1.jar

set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set mapred.output.compression.type=BLOCK;
set hive.exec.copyfile.maxsize=10000000000;
INSERT OVERWRITE DIRECTORY '/user/tnystrand/help_stratos/2b_perjob' select * from thomas_tostratos.stratos_2a_perjob;
