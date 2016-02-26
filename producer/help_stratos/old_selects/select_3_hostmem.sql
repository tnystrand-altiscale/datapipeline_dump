 SET hive.exec.compress.output=true;
 SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
 SET mapred.output.compression.type=BLOCK;

select * from thomas_tostratos.stratos_3;
