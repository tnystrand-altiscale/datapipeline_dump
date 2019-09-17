from __future__ import print_function

import sys
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# sc es el Spark Context

sc = SparkContext(appName="mitest")
ssc = StreamingContext(sc, 2)

brokers, topico = sys.argv[1:]
kvs = KafkaUtils.createDirectStream(ssc, [topico], {"metadata.broker.list": brokers})

dstream = kvs.map(lambda x: x[1])

dstream.pprint()
