Scripts to create tables for stratos project
and subsequently send them

The current order of execution is:

make_sql_tables.sh
hive_to_hdfs_compressed.sql
hdfs_to_workbench.sh
workbench_to_startos.sh
