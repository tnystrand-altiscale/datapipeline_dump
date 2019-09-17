# Copies latest oozie backup from hdfs and loads it to local mariadb

HADOOP_USER_NAME=hdfs hdfs dfs -ls /backups/oozie/db/ |\
    tail -1 |\
    awk '{print $8}' |\
    xargs -n 1 -I {} bash -c 'HADOOP_USER_NAME=hdfs hdfs dfs -ls {}' |\
    grep -v sha256 |\
    awk '{print $8}' |\
    xargs -n 1 -I {} bash -c 'HADOOP_USER_NAME=hdfs hdfs dfs -copyToLocal {} ooziedb.sql.gz' &&\
    gunzip ooziedb.sql.gz

/home/pipeline/mariadb/mysql -P 3306 -h 127.0.0.1 < ooziedb.sql
rm ooziedb.sql
