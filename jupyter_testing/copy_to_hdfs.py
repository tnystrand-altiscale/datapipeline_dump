import os, subprocess
destination = "/user/tnystrand/spark_hive_libs/"
spark_yarn_dist_files="""file:/etc/spark/hive-site.xml,file:/opt/spark/sql/hive/target/spark-hive_2.10-1.6.1.jar,file:/opt/spark/sql/hive-thriftserver/target/spark-hive-thriftserver_2.10-1.6.1.jar,file:/opt/hive/lib/netty-3.7.0.Final.jar,file:/opt/hive/lib/zookeeper-3.4.6.jar,file:/opt/hive/lib/accumulo-fate-1.6.0.jar,file:/opt/hive/lib/groovy-all-2.1.6.jar,file:/opt/hive/lib/geronimo-jaspic_1.0_spec-1.0.jar,file:/opt/hive/lib/paranamer-2.3.jar,file:/opt/hive/lib/hive-testutils-1.2.1.jar,file:/opt/hive/lib/hive-hwi-1.2.1.jar,file:/opt/hive/lib/jline-2.12.jar,file:/opt/hive/lib/hive-cli-1.2.1.jar,file:/opt/hive/lib/commons-httpclient-3.0.1.jar,file:/opt/hive/lib/hamcrest-core-1.1.jar,file:/opt/hive/lib/hive-accumulo-handler-1.2.1.jar,file:/opt/hive/lib/avro-1.7.5.jar,file:/opt/hive/lib/curator-framework-2.6.0.jar,file:/opt/hive/lib/jetty-all-7.6.0.v20120127.jar,file:/opt/hive/lib/commons-cli-1.2.jar,file:/opt/hive/lib/derby-10.10.2.0.jar,file:/opt/hive/lib/datanucleus-api-jdo-3.2.6.jar,file:/opt/hive/lib/bonecp-0.8.0.RELEASE.jar,file:/opt/hive/lib/ST4-4.0.4.jar,file:/opt/hive/lib/snappy-java-1.0.5.jar,file:/opt/hive/lib/stax-api-1.0.1.jar,file:/opt/hive/lib/maven-scm-api-1.4.jar,file:/opt/hive/lib/opencsv-2.3.jar,file:/opt/hive/lib/xz-1.0.jar,file:/opt/hive/lib/asm-tree-3.1.jar,file:/opt/hive/lib/jcommander-1.32.jar,file:/opt/hive/lib/junit-4.11.jar,file:/opt/hive/lib/calcite-linq4j-1.2.0-incubating.jar,file:/opt/hive/lib/apache-log4j-extras-1.2.17.jar,file:/opt/hive/lib/httpclient-4.4.jar,file:/opt/hive/lib/hive-shims-common-1.2.1.jar,file:/opt/hive/lib/curator-client-2.6.0.jar,file:/opt/hive/lib/parquet-hadoop-bundle-1.6.0.jar,file:/opt/hive/lib/hive-ant-1.2.1.jar,file:/opt/hive/lib/commons-logging-1.1.3.jar,file:/opt/hive/lib/hive-jdbc-1.2.1.jar,file:/opt/hive/lib/pentaho-aggdesigner-algorithm-5.1.5-jhyde.jar,file:/opt/hive/lib/hive-shims-1.2.1.jar,file:/opt/hive/lib/hive-contrib-1.2.1.jar,file:/opt/hive/lib/hive-metastore-1.2.1.jar,file:/opt/hive/lib/regexp-1.3.jar,file:/opt/hive/lib/ant-launcher-1.9.1.jar,file:/opt/hive/lib/hive-hbase-handler-1.2.1.jar,file:/opt/hive/lib/jdo-api-3.0.1.jar,file:/opt/hive/lib/libthrift-0.9.2.jar,file:/opt/hive/lib/accumulo-trace-1.6.0.jar,file:/opt/hive/lib/hive-shims-scheduler-1.2.1.jar,file:/opt/hive/lib/hive-common-1.2.1.jar,file:/opt/hive/lib/commons-dbcp-1.4.jar,file:/opt/hive/lib/hive-shims-0.20S-1.2.1.jar,file:/opt/hive/lib/calcite-avatica-1.2.0-incubating.jar,file:/opt/hive/lib/super-csv-2.2.0.jar,file:/opt/hive/lib/stringtemplate-3.2.1.jar,file:/opt/hive/lib/ant-1.9.1.jar,file:/opt/hive/lib/log4j-1.2.16.jar,file:/opt/hive/lib/guava-14.0.1.jar,file:/opt/hive/lib/hive-service-1.2.1.jar,file:/opt/hive/lib/asm-commons-3.1.jar,file:/opt/hive/lib/antlr-runtime-3.4.jar,file:/opt/hive/lib/maven-scm-provider-svn-commons-1.4.jar,file:/opt/hive/lib/mail-1.4.1.jar,file:/opt/hive/lib/commons-configuration-1.6.jar,file:/opt/hive/lib/accumulo-start-1.6.0.jar,file:/opt/hive/lib/jta-1.1.jar,file:/opt/hive/lib/plexus-utils-1.5.6.jar,file:/opt/hive/lib/hive-serde-1.2.1.jar,file:/opt/hive/lib/tempus-fugit-1.1.jar,file:/opt/hive/lib/geronimo-annotation_1.0_spec-1.1.1.jar,file:/opt/hive/lib/hive-beeline-1.2.1.jar,file:/opt/hive/lib/commons-codec-1.4.jar,file:/opt/hive/lib/commons-compiler-2.7.6.jar,file:/opt/hive/lib/jpam-1.1.jar,file:/opt/hive/lib/libfb303-0.9.2.jar,file:/opt/hive/lib/commons-vfs2-2.0.jar,file:/opt/hive/lib/joda-time-2.5.jar,file:/opt/hive/lib/curator-recipes-2.6.0.jar,file:/opt/hive/lib/janino-2.7.6.jar,file:/opt/hive/lib/commons-pool-1.5.4.jar,file:/opt/hive/lib/hive-jdbc-1.2.1-standalone.jar,file:/opt/hive/lib/commons-beanutils-1.7.0.jar,file:/opt/hive/lib/antlr-2.7.7.jar,file:/opt/hive/lib/commons-lang-2.6.jar,file:/opt/hive/lib/eigenbase-properties-1.1.5.jar,file:/opt/hive/lib/calcite-core-1.2.0-incubating.jar,file:/opt/hive/lib/datanucleus-core-3.2.10.jar,file:/opt/hive/lib/jetty-all-server-7.6.0.v20120127.jar,file:/opt/hive/lib/jsr305-3.0.0.jar,file:/opt/hive/lib/json-20090211.jar,file:/opt/hive/lib/commons-io-2.4.jar,file:/opt/hive/lib/commons-compress-1.4.1.jar,file:/opt/hive/lib/velocity-1.5.jar,file:/opt/hive/lib/servlet-api-2.5.jar,file:/opt/hive/lib/oro-2.0.8.jar,file:/opt/hive/lib/commons-math-2.1.jar,file:/opt/hive/lib/maven-scm-provider-svnexe-1.4.jar,file:/opt/hive/lib/commons-beanutils-core-1.8.0.jar,file:/opt/hive/lib/hive-shims-0.23-1.2.1.jar,file:/opt/hive/lib/httpcore-4.4.jar,file:/opt/hive/lib/datanucleus-rdbms-3.2.9.jar,file:/opt/hive/lib/commons-collections-3.2.1.jar,file:/opt/hive/lib/activation-1.1.jar,file:/opt/hive/lib/hive-exec-1.2.1.jar,file:/opt/hive/lib/accumulo-core-1.6.0.jar,file:/opt/hive/lib/commons-digester-1.8.jar,file:/opt/hive/lib/ivy-2.4.0.jar,file:/opt/hive/lib/geronimo-jta_1.1_spec-1.1.1.jar"""
libs = spark_yarn_dist_files.split(",")
cmds = ["hdfs dfs -put -f {0} {1}".format(lib.replace("file:", ""), destination) for lib in libs]
print '\n'.join(cmds)

hdfslibs = ["hdfs://{0}".format(os.path.join(destination, os.path.basename(lib))) for lib in libs]
print "spark.yarn.dist.files={0}".format(",".join(hdfslibs))

print cmds[0]
p = subprocess.Popen(cmds[0], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
p.communicate()