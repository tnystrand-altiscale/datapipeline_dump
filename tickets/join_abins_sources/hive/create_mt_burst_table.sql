set hiveconf:target_table=thomastest.mt_burst;

-- select * from thomastest.mt_burst limit 10
drop table if exists ${hiveconf:target_table};

create table ${hiveconf:target_table}
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
    LINES TERMINATED BY '\n';

load data local inpath '/home/tnystrand/semi_serious/tickets/join_abins_sources/data5/mt_1463961600000_processed.csv' into table ${hiveconf:target_table}
