Example run:

hive -f project.q

JethroClient pipeline localhost:9112 -p <pass> -i create_table.ddl

JethroLoader pipeline table.desc hdfs://nn-dogfood.s3s.altiscale.com:8020/data/log_pipeline/jethro_staging/container_fact_event_flattened

Check results:

JethroClient pipeline localhost:9112 -p <pass>


Jethro recommends 'a few billion rows per partition' to avoid small file
problem. Hence set partition ~3 months.
