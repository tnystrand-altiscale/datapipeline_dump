set hivevar:SYSTEM=;
set hivevar:START_DATE=2016-05-01;
set hivevar:END_DATE=2016-06-20;

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
    clustered by (containerid) into 512 buckets
    stored as orc;

set hive.enforce.bucketing = true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table thomastest.container_fact_test
partition(system, date)
select
    *
from
    cluster_metrics_prod_2.container_fact
where
    date between '${START_DATE}' and '${END_DATE}' and system like '${SYSTEM}';
