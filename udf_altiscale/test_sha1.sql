ADD JAR ./target/HiveUDF-1.0-jar-with-dependencies.jar;
create temporary function sha1 as 'com.altiscale.pipeline.hive.udf.Sha1';
select sha1(system) from cluster_metrics_prod_2.container_fact limit 10;
