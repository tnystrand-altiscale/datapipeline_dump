USE ${DB_NAME};

INSERT OVERWRITE DIRECTORY '${TMP_DIR}/container_time_series_export'
SELECT * FROM 
(
  SELECT MAP('type', 'container_fact', 'memory', cast(memory as string), 'queue', queue, 'containerid', containerid, 'account', account, 'cluster_uuid', cluster_uuid, 'principal_uuid', principal_uuid, 'user_key', user_key, 'clustermemory', cast(clustermemory as string), 'vcores', cast(vcores as string), 'numberapps', cast(numberapps as string), 'host', host, 'requestedtime', cast(requestedtime as string), 'reservedtime', cast(reservedtime as string), 'allocatedtime', cast(allocatedtime as string), 'acquiredtime', cast(acquiredtime as string), 'expiredtime', cast(expiredtime as string), 'runningtime', cast(runningtime as string), 'killedtime', cast(killedtime as string), 'releasedtime', cast(releasedtime as string), 'completedtime', cast(completedtime as string), 'cluster_vcores', cast(cluster_vcores as string), 'date', cast(date as string), 'system', system) FROM container_fact WHERE date >= '${start_date}' AND date <= '${end_date}'
)
sub_query;
