set hiveconf:target_table=thomastest.mt_burst;

drop table if exists ${hiveconf:target_table};

create table ${hiveconf:target_table}
    (
        flow_duration       int,
        matched_inventory   String,
        cluster_capacity    double,
        timestamp           double,
        locked_by           String,
        desired_capacity    double,
        flow_status         String,
        cluster             String,
        requested_delta     double,
        fulfilled_capacity  double
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n';

load data local inpath '/home/tnystrand/semi_serious/tickets/join_abins_sources/data/mt_burst_1462147200000_processed.csv' into table ${hiveconf:target_table}
