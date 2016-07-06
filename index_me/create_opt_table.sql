create table if not exists thomastest.container_fact_test
    (
       jobid                string,
       containerid          string,
       requestedtime        bigint,
       reservedtime         bigint,
       allocatedtime        bigint,
       acquiredtime         bigint,
       expiredtime          bigint,
       runningtime          bigint,
       killedtime           bigint,
       releasedtime         bigint,
       completedtime        bigint,
       memory               bigint,
       vcores               int,
       queue                string,
       host                 string,
       priority             int,
       account              bigint,
       cluster_uuid         string,
       principal_uuid       string,
       user_key             string,
       clustermemory        bigint,
       numberapps           bigint,
       cluster_vcores       bigint
    )
    partitioned by
    (    
        system              string,
        date                string
    )
    clustered by (jobid) sorted by(containerid) into 32 buckets
    stored as orc;

set hive.enforce.bucketing = true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table thomastest.container_fact_test
partition(system, date)
select * from cluster_metrics_prod_2.container_fact where date>='2016-05-01';
