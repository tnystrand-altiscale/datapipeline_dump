ADD JAR ../udf_altiscale/target/HiveUDF-1.0-jar-with-dependencies.jar;
create temporary function sha1 as 'com.altiscale.pipeline.hive.udf.Sha1';

set hiveconf:start_date='2015-06-01';
set hiveconf:end_date='2016-02-01';
set hiveconf:system_dog='dogfood';
set hiveconf:system_jum='jumbo';
set hiveconf:system_jun='jungledata';

use thomas_tostratos;

drop table if exists stratos_3;

create table stratos_3
stored as orc
as
select
    timestamp,
    hostname,
    avg(available_memory + used_memory) as total_memory,
    avg(available_vcores + used_vcores) as total_vcores,
    sha1(system) as system
from 
    thomas_test_parsed_from_hdfs.host_usage_data
where
    date between ${hiveconf:start_date} and ${hiveconf:end_date}
    and system != ${hiveconf:system_dog}
    and system != ${hiveconf:system_jun}
    and system != ${hiveconf:system_jum}
group by
    timestamp,
    hostname,
    system

