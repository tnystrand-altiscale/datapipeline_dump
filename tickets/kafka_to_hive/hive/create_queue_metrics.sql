--set hiveconf:local_data=/home/tnystrand/semi_serious/tickets/kafka_to_hive/data/mt_tohive.csv;

set hiveconf:target_table=dp_prod_1_metrics_archive.queue_metrics;
set hiveconf:tmp_table=dp_prod_1_metrics_archive.queue_metrics_tmp;

-- hive -e "select * from thomastest.queue_metrics limit 10"
drop table if exists ${hiveconf:target_table};
drop table if exists ${hiveconf:tmp_table};

-- Reading into temporary table since we want to save as orc
create table ${hiveconf:tmp_table}
    (
        AvailableMB         int,
        ActiveApplications  int,
        Hostname            string,
        AppsSubmitted       int,
        ReservedMB          int,
        AggregateRackLocalContainersAllocated   int,
        running_0           int,
        running_1440        int,
        running_300         int,
        AggregateOffSwitchContainersAllocated   int,
        AppsPending         int,
        hostname_dup        string,
        PendingVCores       int,
        PendingContainers   int,
        AllocatedMB         int,
        timestamp           double,
        ReservedVCores      int,
        ActiveUsers         int,
        AggregateContainersReleased             int,
        AppsCompleted       int,
        tags                string,
        AppsKilled          int,
        Queue               string,
        PendingMB           int,
        Context             string,
        date                date,
        AvailableVCores     int,
        AggregateNodeLocalContainersAllocated   int,
        name                string,
        AppsRunning         int,
        AggregateContainersAllocated            int,
        AllocatedVCores     int,
        AppsFailed          int,
        time                string,
        running_60          int,
        AllocatedContainers int,
        ReservedContainers  int
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    tblproperties("skip.header.line.count"="1");

-- Output table
create table if not exists ${hiveconf:target_table}
    (
        AvailableMB         int,
        AllocatedMB         int,
        PendingMB           int,
        ReservedMB          int,

        AvailableVCores     int,
        AllocatedVCores     int,
        PendingVCores       int,
        ReservedVCores      int,

        AllocatedContainers int,
        PendingContainers   int,
        ReservedContainers  int,

        ActiveApplications  int,
        AppsPending         int,
        AppsCompleted       int,
        AppsKilled          int,
        AppsSubmitted       int,
        AppsRunning         int,
        AppsFailed          int,

        AggregateRackLocalContainersAllocated   int,
        AggregateOffSwitchContainersAllocated   int,
        AggregateContainersReleased             int,
        AggregateNodeLocalContainersAllocated   int,
        AggregateContainersAllocated            int,

        running_0           int,
        running_60          int,
        running_300         int,
        running_1440        int,

        ActiveUsers         int,

        Hostname            string,
        timestamp           double,
        Queue               string,
        Context             string,
        metricName          string,
        time                string

    )
partitioned by
    (
        system              String,
        partition_date      String
    )
stored as orc;


load data local inpath '${hiveconf:local_data}' into table ${hiveconf:tmp_table};

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table 
    ${hiveconf:target_table}
partition 
    (system, partition_date)
select
    AvailableMB,
    AllocatedMB,
    PendingMB,
    ReservedMB,

    AvailableVCores,
    AllocatedVCores,
    PendingVCores,
    ReservedVCores,

    AllocatedContainers,
    PendingContainers,
    ReservedContainers,

    ActiveApplications,
    AppsPending,
    AppsCompleted,
    AppsKilled,
    AppsSubmitted,
    AppsRunning,
    AppsFailed,

    AggregateRackLocalContainersAllocated,
    AggregateOffSwitchContainersAllocated,
    AggregateContainersReleased,
    AggregateNodeLocalContainersAllocated,
    AggregateContainersAllocated,

    running_0,
    running_60,
    running_300,
    running_1440,

    ActiveUsers,

    Hostname,
    timestamp,
    -- Remove the 'root.' from root.<queuename>
    if (Queue != "root", substr(Queue, 6), Queue) as Queue,
    Context,
    name as metricName,
    time,

    tags as system,
    date as partition_date
from
    ${hiveconf:tmp_table}
