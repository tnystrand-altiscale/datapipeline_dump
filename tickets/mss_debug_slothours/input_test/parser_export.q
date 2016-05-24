SET hiveconf:TMP_DIR=/home/tnystrand/semi_serious/tickets/mss_debug_slothours/input_test/data/day17;
SET hiveconf:LOG_DB=logarchive_prod_2;
SET hiveconf:LOG_TABLE=logs;
SET hiveconf:START_DATE=2016-05-14;
SET hiveconf:END_DATE=2016-05-18;
SET hiveconf:SERVICE=resourcemanager;
SET hiveconf:SYSTEM=marketshare;
SET hiveconf:SOURCE_CLAUSE=(source = 'org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary' OR source = 'org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo' OR source = 'org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl' OR source = 'org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue');


USE ${hiveconf:LOG_DB};
--SET hive.exec.compress.output=true;
--SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
--SET mapred.output.compression.type=BLOCK;
set mapreduce.map.memory.mb=5120;
set mapred.reduce.tasks=1;

--Export data from log_archive for a particular service and date range

INSERT OVERWRITE LOCAL DIRECTORY '${hiveconf:TMP_DIR}'
SELECT * FROM
(
  SELECT MAP('type', 'logs', 'hostname', hostname, 'timestamp', cast(timestamp as string), 'time', time, 'source', source, 'message', message, 'level', level, 'offset', cast(offset as string), 'system', system, 'service', service, 'date', date) 
  FROM 
    ${hiveconf:LOG_TABLE} 
  WHERE 
    date >= '${hiveconf:START_DATE}' 
    AND date <= '${hiveconf:END_DATE}' 
    AND service = '${hiveconf:SERVICE}'
    AND ${hiveconf:SOURCE_CLAUSE}
    AND system='${hiveconf:SYSTEM}'
) subquery;
