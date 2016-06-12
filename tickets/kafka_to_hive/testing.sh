./kafka_parse_metrics show \
    --topic mt-burst-metrics \
    --timestamp $1 \
    --path ./test_data/data_$1/burst \
    --kafka kafka01-sc1.service.altiscale.com:9092

#./kafka_parse_metrics show \
#    --topic resourcemanager \
#    --timestamp $1 \
#    --clusters dogfood \
#    --path ./test_data/data_$1/resourcemanager \
#    --kafka kafka01-sc1.service.altiscale.com:9092
