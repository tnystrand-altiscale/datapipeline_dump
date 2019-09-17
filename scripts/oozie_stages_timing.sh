# Example run:
# source oozie_stages_timing.sh 2016-12-12

# Note: sometimes two of the same workflow ran on the same day
# In this case we cannot separate the two and need to look in oozie GUI which to choose

# logs3sync is called the same for both stagning and prod clusters

input_date=$1
oozie jobs \
    -jobtype=coordinator -len 500 | grep RUNNING | awk '{print $1}' \
    | xargs -n 1 oozie job -info | grep "$input_date" | grep SUCCEEDED | awk '{print $3}' \
    | xargs -n 1 oozie job -info | grep -E "Workflow|Started|Ended" | awk '{if ($1=="Started"||$1=="Ended") print $3,$4; else print $4;}'
