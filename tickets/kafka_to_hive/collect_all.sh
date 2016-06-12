# Check for input arguments
if [ "$#" -ne 1 ]
then
    echo "*** error ***"
    echo "Usage: ./collect_all.sh <timestamp_in_milliseconds>"
    exit 1
fi


logger=runlog.txt
workdir=./data/data_$1
echo Starting run for $1 at "'"`date`"'" >> $logger

# Remove prvious working directory if exists silently
rm -r $workdir 2>/dev/null

# Terminate if any command fails
set -e

./kafka_parse_metrics show \
    --topic mt-burst-metrics \
    --timestamp $1 \
    --path $workdir/burst \
    --kafka kafka01-sc1.service.altiscale.com:9092

./kafka_parse_metrics show \
    --topic resourcemanager \
    --timestamp $1 \
    --path $workdir/resourcemanager \
    --kafka kafka01-sc1.service.altiscale.com:9092

#    --cluster-queues dogfood:production owneriq:interactive glu:default ninthdecimal:production playfirst:default \

cat $workdir/resourcemanager/* > $workdir/resourcemanager/all_$1
cat $workdir/burst/* > $workdir/burst/all_$1

python ./json2csv.py $workdir/burst/all_$1
python ./json2csv.py $workdir/resourcemanager/all_$1

cp $workdir/burst/all_$1.csv ./data/mt_tohive.csv
cp $workdir/resourcemanager/all_$1.csv ./data/rm_tohive.csv

hive --hiveconf hive.execution.engine=tez \
     --hiveconf hive.tez.java.opts=-Xmx7096m \
     --hiveconf hive.tez.container.size=8000\
     --hiveconf tez.cpu.vcores=4 \
     --hiveconf tez.session.client.timeout.secs=10000000 \
     --hiveconf tez.session.am.dag.submit.timeout.secs=10000000 \
     --hiveconf tez.queue.name=research \
     --hiveconf local_data='./data/mt_tohive.csv' \
     "-f" "./hive/create_mt_burst_table.sql"

hive --hiveconf hive.execution.engine=tez \
     --hiveconf hive.tez.java.opts=-Xmx7096m \
     --hiveconf hive.tez.container.size=8000\
     --hiveconf tez.cpu.vcores=4 \
     --hiveconf tez.session.client.timeout.secs=10000000 \
     --hiveconf tez.session.am.dag.submit.timeout.secs=10000000 \
     --hiveconf tez.queue.name=research \
     --hiveconf local_data='./data/rm_tohive.csv' \
     "-f" "./hive/create_queue_metrics.sql"

rm -r $workdir
echo Finished run for $1 at "'"`date`"'" >> $logger
