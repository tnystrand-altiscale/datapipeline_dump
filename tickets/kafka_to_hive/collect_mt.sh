./kafka_parse_metrics show \
    --topic mt-burst-metrics \
    --timestamp $1 \
    --path ./data/data_$1/burst \
    --kafka kafka01-sc1.service.altiscale.com:9092
