set hiveconf:target_table=thomastest.queue_metrics_2;

drop table if exists ${hiveconf:target_table};

create table ${hiveconf:target_table}
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
    LINES TERMINATED BY '\n';

load data local inpath '/home/tnystrand/semi_serious/tickets/join_abins_sources/data4/all_1463460898000_QueueMetrics.csv' into table ${hiveconf:target_table}
