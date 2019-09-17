hiveq -e "select * from ziff_scratch.bts where partition_date>='2017-07-03' and (system='dogfood' or system='visiblemeasures' or system='airpush)'" > btst.tsv
