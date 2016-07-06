set hive.exec.copyfile.maxsize=1000000000000;
INSERT OVERWRITE DIRECTORY '${hiveconf:TGT_DIR}'
select
    floor(mt_timestamp) as mt_timestamp    ,
    matched_inventory       ,
    desired_capacity        ,
    requested_delta         ,
    cluster_capacity        ,
    locked_by               ,
    mt_cluster              ,
    floor(qm_timestamp) as qm_timestamp    ,
    pendinggb               ,
    allocatedgb             ,
    availablegb             ,
    reservedgb              ,
    qm_cluster              ,
    qm_queue                ,
    memory                  ,
    memory_in_wait          ,
    cluster_memory_capacity ,
    floor(minute_start) as minute_start    ,
    cts_queue               ,
    fulfilled_capacity      ,
    flow_status             ,
    flow_duration           ,
    partition_date          ,
    system
from
    cluster_metrics_prod_2.burst_time_series
where
partition_date between '${hiveconf:START_DATE}' and '${hiveconf:END_DATE}'
