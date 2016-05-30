set hiveconf:target_table=thomastest.mt_burst;

drop table if exists ${hiveconf:target_table};

create table ${hiveconf:target_table}
    (
        matched_inventory   String,
        cluster_capacity    double,
        timestamp           double,
        locked_by           String,
        desired_capacity    double,
        cluster             String,
        flow_status         String,
        requested_delta     double,
        fulfilled_capacity  double
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n';

load data local inpath '/home/tnystrand/semi_serious/tickets/join_abins_sources/data4/mt_burst_1463460898000_processed.csv' into table ${hiveconf:target_table}
