beeline \
    -u jdbc:hive2://hiveserver-dogfood.s3s.altiscale.com:10000 \
    -n tnystrand \
    --hiveconf mapreduce.map.memory.mb=20000 \
    --hiveconf mapreduce.map.java.opts=-Xmx18000m \
    --hiveconf mapreduce.reduce.memory.mb=20000 \
    --hiveconf mapreduce.reduce.java.opts=-Xmx18000m \
    --showHeader true \
    --outputformat=tsv2 \
    "$1" "$2"
    
