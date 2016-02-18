nohup hive -f create_tables/memory_requested_series.sql > nohup.1.out &

nohup hive -f create_tables/percontainer_mem_and_vcores.sql > nohup.2a.out &

nohup hive -f create_tables/perjob_mem_and_vcores.sql > nohup.2b.out &

nohup hive -f create_tables/host_mem_and_vcores.sql > nohup.3.out &
