create table if not exists thomastest.partitions_test
(
    jobid string
)
partitioned by
(
    system string,
    date string
);

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

insert overwrite table thomastest.partitions_test
partition(system,date)
select
    jobid,
    system,
    date
from
    cluster_metrics_prod_2.job_fact
where
    system='dogfood'
    and date='2016-05-01'
