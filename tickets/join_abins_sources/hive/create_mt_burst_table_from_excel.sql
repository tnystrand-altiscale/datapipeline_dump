set hiveconf:target_table=thomastest.mt_burst_2;

drop table if exists ${hiveconf:target_table};

create table ${hiveconf:target_table}
    (
        cluster             String,
        timestamp           double,
        date                String,
        cluster_capacity    double,
        desired_capacity    double,
        requested_delta     double
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n';

load data local inpath '/home/tnystrand/semi_serious/tickets/join_abins_sources/data2/mt_burst_1462147200000_processed.csv' into table ${hiveconf:target_table}
