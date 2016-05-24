USE ${EDB_SDB};

SET mapred.reduce.tasks = 1;

INSERT OVERWRITE DIRECTORY '${TMP_DIR}/cluster_dim'
SELECT * FROM
(
  SELECT MAP('type', 'cluster_dim', 'uuid', uuid, 'account', cast(account as string), 'name', name) FROM cluster_dim
) subquery;

INSERT OVERWRITE DIRECTORY '${TMP_DIR}/user_map'
SELECT * FROM
(
  SELECT MAP('type', 'user_map', 'principal_uuid', principal_uuid, 'user', user, 'system_user_name', system_user_name, 'cluster', cluster) FROM user_map
) subquery;

CREATE DATABASE IF NOT EXISTS ${DB_NAME};
USE ${DB_NAME};

SET mapred.reduce.tasks = 400;

CREATE TABLE IF NOT EXISTS bucket(applicationid string, key string, requestedtime bigint, count int, user string) PARTITIONED BY (system string, date date) STORED AS ORC;

CREATE TABLE IF NOT EXISTS unfinished_container (applicationid string, containerid string, requestedtime bigint, reservedtime bigint, allocatedtime bigint, acquiredtime bigint, expiredtime bigint, runningtime bigint, killedtime bigint, releasedtime bigint, completedtime bigint, memory bigint, vcores int, queue string, host string, priority int, clustermemory bigint, numberapps bigint, cluster_vcores bigint) PARTITIONED BY (system string, date date) STORED AS ORC;

INSERT OVERWRITE DIRECTORY '${TMP_DIR}/container_fact_export'
SELECT * FROM 
(
  SELECT MAP('type', 'bucket', 'applicationid', applicationid, 'key', key, 'requestedtime', cast(requestedtime as string), 'count', cast(count as string), 'user', user, 'system', system) FROM bucket WHERE date = '${previous_date}' 
  UNION ALL
  SELECT MAP('type', 'unfinished_container', 'applicationid', applicationid, 'containerid', containerid, 'requestedtime', cast(requestedtime as string), 'reservedtime', cast(reservedtime as string), 'allocatedtime', cast(allocatedtime as string), 'acquiredtime', cast(acquiredtime as string), 'expiredtime', cast(expiredtime as string), 'runningtime', cast(runningtime as string), 'killedtime', cast(killedtime as string), 'releasedtime', cast(releasedtime as string), 'completedtime', cast(completedtime as string), 'memory', cast(memory as string), 'vcores', cast(vcores as string), 'queue', queue, 'host', host, 'priority', cast(priority as string), 'clustermemory', cast(clustermemory as string), 'numberapps', cast(numberapps as string), 'cluster_vcores', cast(cluster_vcores as string), 'system', system) FROM unfinished_container WHERE date = '${previous_date}'
)
sub_query;
