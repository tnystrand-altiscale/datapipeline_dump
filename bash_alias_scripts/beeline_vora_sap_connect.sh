beeline \
    -u jdbc:hive2://305-10.rt1.altiscale.com:19123 \
    -n tnystrand \
    --hiveconf mapreduce.map.memory.mb=20000 \
    --hiveconf mapreduce.map.java.opts=-Xmx18000m \
    --hiveconf mapreduce.reduce.memory.mb=20000 \
    --hiveconf mapreduce.reduce.java.opts=-Xmx18000m \
    --showHeader true \
    --outputformat=csv2 \
    "$1" "$2"
