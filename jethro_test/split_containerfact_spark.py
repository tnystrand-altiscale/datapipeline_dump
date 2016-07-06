from __future__ import print_function
import sys, re, os
import shutil, time, argparse
import runcommand
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import Row

"""
0. jobid               	string
1. containerid         	string
2. requestedtime       	bigint
3. reqreservedtime        	bigint
4. allocatedtime       	bigint
5. acquiredtime        	bigint
6. expiredtime         	bigint
7. runningtime         	bigint
8. killedtime          	bigint
9. releasedtime        	bigint
10. completedtime       	bigint
11. memory              	bigint
12. vcores              	int
13. queue               	string
14. host                	string
15. priority            	int
16. account             	bigint
17. cluster_uuid        	string
18. principal_uuid      	string
19. user_key            	string
20. clustermemory       	bigint
21. numberapps          	bigint
22. cluster_vcores          bigint
23. system              	string
24. date                	date
"""

# TODO: Define column names in a header and share
def getrow(idstart,idend,name,row):
    return Row(jobid=row[0],
			   containerid=row[1], 
			   start=row[idstart], 
		       stop=row[idend],
		       duration=str(int(row[idend])-int(row[idstart])),
		       event=name,
               size=row[11],
               priority=row[15],
               hostname=row[14],
		       system=row[23],
		       date=row[24])
    
# data_filename should be filepath
def split_data(row):

    split_rows = []

    # REQUEST
    if int(row[2])>0 and int(row[4])>0:
    	split_rows.append(getrow(2,4,"request",row))
     
    # ALLCOATE
    if int(row[4])>0 and int(row[7])>0:
    	split_rows.append(getrow(4,7,"allocate",row))
    
    # RUNCOMPLETE
    if int(row[7])>0 and int(row[10])>0:
    	split_rows.append(getrow(7,10,"run",row))
    
    # ALLOCATED TO RELEASED
    if int(row[4])>0 and int(row[9])>0:
    	split_rows.append(getrow(4,9,"released",row))
    
    # RESERVED TO ALLOCATED
    if int(row[3])>0 and int(row[4])>0:
    	split_rows.append(getrow(3,4,"reserved",row))

    # RESERVED TO KILLED
    if int(row[3])>0 and int(row[8])>0:
    	split_rows.append(getrow(3,8,"reserved",row))
    
    # reserved to completed no run
    if int(row[3])>0 and int(row[10]>0 and int(row[2])==0 and int(row[4])==0)>0:
    	split_rows.append(getrow(3,10,"reserved",row))
    
    # KILLED
    if int(row[7])>0 and int(row[8])>0:
    	split_rows.append(getrow(7,8,"killed",row))

    return split_rows


def parser_arguments():
    parser = argparse.ArgumentParser(description='Split container_fact into separate events')
    
    parser.add_argument('-s', '--start_date', required=True, nargs=1, \
        help='Start date of interval')

    parser.add_argument('-e', '--end_date', required=True, nargs=1, \
        help='End date of interval')

    return parser.parse_args()


def main():
    args = parser_arguments()
    start_date = args.start_date[0]
    end_date = args.end_date[0]

    sc = SparkContext()
    hc = HiveContext(sc)


    select = """
        SELECT 
            * 
        FROM 
            cluster_metrics_prod_2.container_fact
        where 
            date between '{0}' and '{1}'
        """.format(start_date, end_date)

    df = hc.sql(select)

    header = {
            "jobid"         : "string",
            "containerid"   : "string", 
            "start"         : "bigint", 
            "stop"          : "bigint",
            "duration"      : "bigint",
            "event"         : "string", 
            "size"          : "double", 
            "priority"      : "int", 
            "hostname"      : "string", 
            "system"        : "string", 
            "date"          : "string"
            }
            

    all_rows = df.flatMap(split_data)
    schema_split_containers = hc.createDataFrame(all_rows)
    schema_split_containers.registerTempTable("split_containers")


    create_string = """
        create table if not exists cluster_metrics_prod_2.container_fact_event_flattened
            (
            jobid       string,
            containerid string,
            start       bigint,
            stop        bigint,
            duration    bigint,
            event       string,
            size        double,
            priority    int,
            hostname    string
            )
        partitioned by
            (
            system      string,
            date        string
            )
        stored as orc
    """

    set_dyn = "set hive.exec.dynamic.partition=true"
    set_nstat = "set hive.exec.dynamic.partition.mode=nonstrict"

    load_string = """
        insert overwrite table 
            cluster_metrics_prod_2.container_fact_event_flattened 
        partition
            (system, date) 
        select 
            jobid, 
            containerid, 
            start, 
            stop, 
            duration, 
            event, 
            size, 
            priority, 
            hostname, 
            system, 
            date 
        from 
            split_containers
    """

    print("Setting dynamic partition...")
    hc.sql(set_dyn)
    hc.sql(set_nstat)

    print("Creating Table...")
    hc.sql(create_string)
    print("Loading data into table...")
    hc.sql(load_string)
    print("DONE")


if __name__=="__main__":
    main()

