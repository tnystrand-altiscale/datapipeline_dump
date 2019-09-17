input_date=$1
echo "Non-successfull workflows:"
oozie jobs \
    -jobtype=coordinator -len 500 | grep RUNNING | awk '{print $1}' \
    | xargs -n 1 oozie job -info | grep $input_date | grep -v SUCCEEDED | awk '{print $1,$2}' | xargs -n 2 | tee /dev/tty | awk '{print substr($1, 0, 36)}' \
    | xargs -n 1 oozie job -info | grep "Job Name" | awk '{print $4}'
