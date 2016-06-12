--set hiveconf:local_data=/home/tnystrand/semi_serious/tickets/kafka_to_hive/data/mt_tohive.csv;

set hiveconf:target_table=dp_prod_1_metrics_archive.mt_burst;
set hiveconf:tmp_table=dp_prod_1_metrics_archive.mt_burst_tmp;


-- hive -e "select * from thomastest.mt_burst limit 10"
drop table if exists ${hiveconf:target_table};
drop table if exists ${hiveconf:tmp_table};

-- Reading into temporary table since we need to pad with date field and save as orc
create table ${hiveconf:tmp_table}
    (
        matched_inventory   String,
        cluster_capacity    double,
        timestamp           double,
        locked_by           String,
        desired_capacity    double,
        cluster             String,
        requested_delta     double
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    tblproperties("skip.header.line.count"="1");

-- Output table
create table if not exists ${hiveconf:target_table}
    (
        matched_inventory   String,
        cluster_capacity    double,
        timestamp           double,
        locked_by           String,
        desired_capacity    double,
        requested_delta     double
    )
partitioned by
    (
        system              String,
        partition_date      String
    )
stored as orc;

load data local inpath '${hiveconf:local_data}' into table ${hiveconf:tmp_table};

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table 
    ${hiveconf:target_table}
partition 
    (system, partition_date)
select
    matched_inventory,
    cluster_capacity,
    timestamp,
    locked_by,
    desired_capacity,
    requested_delta,
    cluster as system,
    -- Must be in seconds
    to_date(from_unixtime(floor(timestamp/1000))) as partition_date
from
    ${hiveconf:tmp_table}
    

