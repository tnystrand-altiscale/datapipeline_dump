#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Timestamp from which data will be collect (till most recent posted metric)
timestamp=1467222302000

# Data is stored here
workdir=$DIR/data/data_$timestamp
rm -r $workdir 2>/dev/null

# Collecting kafka metrics for mt-burst topic
time $DIR/kafka_parse_metrics show \
    --topic mt-burst-metrics \
    --timestamp $timestamp \
    --path $workdir/burst \
    --kafka kafka01-sc1.service.altiscale.com:9092 \
            kafka02-sc1.service.altiscale.com:9092 \
            kafka03-sc1.service.altiscale.com:9092

## Clusters are needed as input since some noise is posted to burst-metrics right now
#clusters=$(hive -e \
#    "set hive.cli.print.header=false;
#    select distinct name from rollup_edb_sdb_dims_prod_1.cluster_dim")
#
## Can be set as a string and inputted
#echo $clusters
#
## Collecting kafka metrics for resourcemanager topic
#time $DIR/kafka_parse_metrics show \
#    --topic resourcemanager \
#    --timestamp $timestamp \
#    --clusters $clusters \
#    --path $workdir/resourcemanager \
#    --kafka kafka01-sc1.service.altiscale.com:9092 \
#            kafka02-sc1.service.altiscale.com:9092 \
#            kafka03-sc1.service.altiscale.com:9092
