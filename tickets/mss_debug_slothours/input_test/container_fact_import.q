USE ${DB_NAME};

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000000;
SET hive.exec.max.dynamic.partitions.pernode=1000000;
SET mapreduce.map.memory.mb=20120;
SET mapreduce.map.java.opts=-Xmx20000m;
SET mapreduce.reduce.memory.mb=20120;
SET mapreduce.reduce.java.opts=-Xmx20000m;

 
DROP TABLE ${TMP_TABLE};

CREATE EXTERNAL TABLE ${TMP_TABLE}(data MAP<STRING,STRING>)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${INPUT_DIR}';

DROP TABLE IF EXISTS container_fact_${RUN_ID}_part_temp;

CREATE TABLE IF NOT EXISTS container_fact (jobid string, containerid string, requestedtime bigint, reservedtime bigint, allocatedtime bigint, acquiredtime bigint, expiredtime bigint, runningtime bigint, killedtime bigint, releasedtime bigint, completedtime bigint, memory bigint, vcores int, queue string, host string, priority int, account bigint, cluster_uuid string, principal_uuid string, user_key string, clustermemory bigint, numberapps bigint, cluster_vcores bigint) PARTITIONED BY (system string, date date) STORED AS ORC;

CREATE TABLE IF NOT EXISTS container_fact_${RUN_ID}_part_temp (jobid string, containerid string, requestedtime bigint, reservedtime bigint, allocatedtime bigint, acquiredtime bigint, expiredtime bigint, runningtime bigint, killedtime bigint, releasedtime bigint, completedtime bigint, memory bigint, vcores int, queue string, host string, priority int, account bigint, cluster_uuid string, principal_uuid string, user_key string, clustermemory bigint, numberapps bigint, cluster_vcores bigint) PARTITIONED BY (system string, date date) STORED AS ORC;

INSERT OVERWRITE TABLE container_fact_${RUN_ID}_part_temp PARTITION (system, date)
SELECT data['jobid'], data['containerid'], cast(data['requestedTime'] as bigint), cast(data['reservedTime'] as bigint), cast(data['allocatedTime'] as bigint), cast(data['acquiredTime'] as bigint), cast(data['expiredTime'] as bigint), cast(data['runningTime'] as bigint), cast(data['killedTime'] as bigint), cast(data['releasedTime'] as bigint), cast(data['completedTime'] as bigint), cast(data['memory'] as bigint), cast(data['vcores'] as int), data['queue'], data['host'], cast(data['priority'] as int), cast(data['account'] as bigint), data['cluster_uuid'], data['principal_uuid'], data['user_key'], cast(data['clusterMemory'] as bigint), cast(data['numberApps'] as bigint), cast(data['cluster_vcores'] as bigint), data['system'], data['date']
FROM ${TMP_TABLE}
WHERE data['type'] = 'container_fact';

CREATE TABLE IF NOT EXISTS bucket(applicationid string, key string, requestedtime bigint, count int, user string) PARTITIONED BY (system string, date date) STORED AS ORC;

INSERT OVERWRITE TABLE bucket PARTITION (system, date)
SELECT data['applicationid'], data['key'], cast(data['requestedTime'] as bigint), cast(data['count'] as int), data['user'], data['system'], data['date']
FROM ${TMP_TABLE}
WHERE data['type'] = 'bucket';

CREATE TABLE IF NOT EXISTS unfinished_container (applicationid string, containerid string, requestedtime bigint, reservedtime bigint, allocatedtime bigint, acquiredtime bigint, expiredtime bigint, runningtime bigint, killedtime bigint, releasedtime bigint, completedtime bigint, memory bigint, vcores int, queue string, host string, priority int, clustermemory bigint, numberapps bigint, cluster_vcores bigint) PARTITIONED BY (system string, date date) STORED AS ORC;

INSERT OVERWRITE TABLE unfinished_container PARTITION (system, date)
SELECT data['applicationid'], data['containerid'], cast(data['requestedTime'] as bigint), cast(data['reservedTime'] as bigint), cast(data['allocatedTime'] as bigint), cast(data['acquiredTime'] as bigint), cast(data['expiredTime'] as bigint), cast(data['runningTime'] as bigint), cast(data['killedTime'] as bigint), cast(data['releasedTime'] as bigint), cast(data['completedTime'] as bigint), cast(data['memory'] as bigint), cast(data['vcores'] as int), data['queue'], data['host'], cast(data['priority'] as int), cast(data['clusterMemory'] as bigint), cast(data['numberApps'] as bigint), cast(data['cluster_vcores'] as bigint), data['system'], data['date']
FROM ${TMP_TABLE}
WHERE data['type'] = 'unfinished_container';

DROP TABLE ${TMP_TABLE};
