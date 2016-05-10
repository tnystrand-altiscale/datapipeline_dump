set hiveconf:target_tables=thomastest.mt_burst;

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
        hostname            string,
        PendingVCores       int,
        PendingContainers   int,
        AllocatedMB         int,
        timestamp           int,
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
    );

load local data inpath '/home/tnystrand/semi_serious/tickets/join_abins_sources/data/mt_burst_1462147200000_processed.csv' into table ${hiveconf:target_table}
