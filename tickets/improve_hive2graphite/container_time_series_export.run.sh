python /home/tnystrand/usefulscripts/besthivequeuelaunch.py \
    -f container_time_series_export.q \
    -hiveconf DB_NAME=cluster_metrics_prod_2 \
    -hiveconf TMP_DIR=/tmp/tnystrand/ \
    -hiveconf start_date='2016-04-01' \
    -hiveconf end_date='2016-04-01'
