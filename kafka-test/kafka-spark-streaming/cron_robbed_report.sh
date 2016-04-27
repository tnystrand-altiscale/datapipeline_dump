#!/bin/bash
# vim: set wrap:
# Usage:
# cron_robbed_jobs.sh 
#   <first source database> 
#   <second source database> 
#   <intermediate database for tmp tables>
#   <target database where table is stored>
#   <start date of period>
#   <end date of period>

# Add to crontab:
# crontab -e
# 19 19 * * * . $HOME/.bashrc && $HOME/datapipeline/robbed_jobs_report/cron_robbed_report.sh cluster_metrics_prod_2 cluster_metrics_prod_1 dp_tmp cluster_metrics_prod_2 >> $HOME/datapipeline/robbed_jobs_report/runlog.log 2>&1

echo '=============================================================================================' 
echo '=============================================================================================' 
echo "Robbed Jobs Cron Started: $(date)" 
# Exit script if anything fails
set -e

basedir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
sparkdir="${basedir}/sparkjob"
hivedir="${basedir}/hivequeries"

if [ -z "$1" ] || [ "$1" == "-"  ]; then
    source_db=cluster_metrics_prod_2
else
    source_db="$1"
fi

if [ -z "$2" ] || [ "$2" == "-"  ]; then
    source_db2=cluster_metrics_prod_1
else
    source_db2="$2"
fi

if [ -z "$3" ] || [ "$3" == "-"  ]; then
    intermed_db=dp_tmp
else
    intermed_db="$3"
fi

if [ -z "$4" ] || [ "$4" == "-"  ]; then
    target_db=cluster_metrics_prod_2
else
    target_db="$4"
fi

if [ -z "$5" ] || [ "$5" == "-"  ]; then
    start_date="$(date -d '-2 day' '+%Y-%m-%d')"
else
    start_date="$5"
fi

if [ -z "$6" ] || [ "$6" == "-"  ]; then
    end_date="$(date -d '-2 day' '+%Y-%m-%d')"
else
    end_date="$6"
fi

first_date=$(date "--date=${start_date}-7day" +%Y-%m-%d)
stamp="$(date '+%Y%m%d_%H%M%S_%N')"
tmp_table1="${stamp}_container_time_series_alloc_and_run_extend"
tmp_table2="${stamp}_state_perminute_job"
tmp_table3="${stamp}_request_assign_release_from_cf"
tmp_table4="${stamp}_job_categories_from_spark"

first_date="'"$first_date"'"
start_date="'"$start_date"'"
end_date="'"$end_date"'"

queue=production

extendcts=" \
        -hiveconf tez.queue.name=$queue \
        -hiveconf FIRST_DATE=$first_date \
        -hiveconf START_DATE=$start_date \
        -hiveconf END_DATE=$end_date \
        -hiveconf SERIES_TABLE=${source_db}.container_time_series \
        -hiveconf FACT_TABLE=${source_db}.container_fact \
        -hiveconf FACT_JOB=${source_db}.job_fact \
        -hiveconf OUT_TABLE=${intermed_db}.${tmp_table1} \
        -f $hivedir/container_time_series_alloc_and_run_extend.sql"

statestr=" \
        -hiveconf tez.queue.name=$queue \
        -hiveconf START_DATE=$start_date \
        -hiveconf END_DATE=$end_date \
        -hiveconf SERIES_TABLE=${intermed_db}.${tmp_table1} \
        -hiveconf OUT_TABLE=${intermed_db}.${tmp_table2} \
        -hiveconf QUEUE_DIM=${source_db}.queue_dim \
        -hiveconf RESOURCE_DIM=${source_db}.cluster_resource_dim \
        -f $hivedir/state_perminute_job.sql"

rarstr=" \
        -hiveconf tez.queue.name=$queue \
        -hiveconf FIRST_DATE=$first_date \
        -hiveconf START_DATE=$start_date \
        -hiveconf END_DATE=$end_date \
        -hiveconf FACT_TABLE=${source_db}.container_fact \
        -hiveconf FACT_JOB=${source_db}.job_fact \
        -hiveconf OUT_TABLE=${intermed_db}.${tmp_table3} \
        -f $hivedir/request_assign_release_from_cf.sql"

sparkstr="
(cd $sparkdir && 
./run.sh ${intermed_db}.${tmp_table2} ${intermed_db}.${tmp_table3} $intermed_db.$tmp_table4 0 $queue)
"

robbedstr=" \
        -hiveconf START_DATE=$start_date \
        -hiveconf END_DATE=$end_date \
        -hiveconf FACT_TABLE=${source_db}.container_fact \
        -hiveconf MEM_WASTE_TABLE=${source_db}.container_memory_fact \
        -hiveconf FACT_JOB=${source_db}.job_fact \
        -hiveconf SPARK_TABLE=${intermed_db}.${tmp_table4} \
        -hiveconf TGT_TABLE=${target_db}.robbed_jobs_report \
        -f $hivedir/robbed_jobs.sql"


deletestr="
        drop table ${intermed_db}.${tmp_table1};  
        drop table ${intermed_db}.${tmp_table2};
        drop table ${intermed_db}.${tmp_table3};
        drop table ${intermed_db}.${tmp_table4};
"

echo "" 
echo "Creating Robbed Jobs Table for dates: $start_date to $end_date from directory: $basedir"
echo '=============================================================================================' 
echo "" 

echo ""
echo "Creation Path: "
echo ""
echo "hive $extendcts" 
echo ""
echo "hive $statestr" 
echo ""
echo "hive $rarstr" 
echo ""
echo "$sparkstr" 
echo ""
echo "hive $robbedstr" 
echo ""
echo "hive -e \"$deletestr\"" 
echo ""

echo ""

echo ':::::::::: Create Temp Tables Needed For SPARK Job ::::::::::::::' 
echo '-----------------------------------------------------------------' 
(
    echo "hive $extendcts" 
    hive $extendcts 
    
    echo "hive $statestr" 
    hive $statestr 
) &
(
    echo "hive $rarstr" 
    hive $rarstr 
) &
trap "pkill -TERM -g $$" EXIT

wait

echo '::::::::::::::::::::::: Run SPARK Job :::::::::::::::::::::::::::' 
echo '-----------------------------------------------------------------' 


echo "$sparkstr" 
eval "$sparkstr" 

echo '::::::::::::::::::::: Robbed Jobs Table :::::::::::::::::::::::::' 
echo '-----------------------------------------------------------------' 


echo "hive $robbedstr" 
hive $robbedstr 

if [ -z "$7" ] || [ "$7" == "1"  ]; then
    echo '::::::::::::::::::::: Cleaning Up :::::::::::::::::::::::::::::::' 
    echo '-----------------------------------------------------------------' 
    
    echo "hive -e \"$deletestr\"" 
    hive -e "\"$deletestr\"" 
fi

echo ''
echo "Robbed jobs script finished: $(date)" 
echo ''
echo ''
