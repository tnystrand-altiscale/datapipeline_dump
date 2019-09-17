beeline \
    -u jdbc:hive2://105-30.sc1.altiscale.com:19123 \
    -n tnystrand \
    --hiveconf spark.vora.discovery=105-30.sc1.altiscale.com:8500 \
    --hiveconf mapreduce.map.memory.mb=20000 \
    --hiveconf mapreduce.map.java.opts=-Xmx18000m \
    --hiveconf mapreduce.reduce.memory.mb=20000 \
    --hiveconf mapreduce.reduce.java.opts=-Xmx18000m \
    --showHeader true \
    --outputformat=tsv2 \
    "$1" "$2"
