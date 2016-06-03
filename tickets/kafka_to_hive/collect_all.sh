./kafka_parse_metrics show \
    --topic mt-burst-metrics \
    --timestamp $1 \
    --path ./data/data_$1/burst \
    --kafka kafka01-sc1.service.altiscale.com:9092

./kafka_parse_metrics show \
    --topic resourcemanager \
    --timestamp $1 \
    --cluster-queues dogfood:production owner:interactive glu:default ninthdecimal:production playfirst:default \
    --path ./data/data_$1/resourcemanager \
    --kafka kafka01-sc1.service.altiscale.com:9092

cd ./data/data_$1/resourcemanager/ && cat * > all_$1

python json2csv.py ./data/data_$1/burst/_$1
python json2csv.py ./data/data_$1/resourcemanager/all_$1

./data/data_$1/burst/mt_$1.csv ./data/mt_tohive.csv
./data/data_$1/resourcemanager/all_$1.csv ./data/rm_tohive.csv
