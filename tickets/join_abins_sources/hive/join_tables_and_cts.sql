with
    queue_by_second
    as (
    select
        avg(int(timestamp/60000)*60) as timestamp,
        avg(pendingmb)/1000 as pendinggb,
        avg(allocatedmb)/1000 as allocatedgb,
        avg(availablemb)/1000 as availablegb,
        avg(reservedmb)/1000 as reservedgb,
        tags as cluster
    from
        thomastest.queue_metrics
    group by
        int(timestamp/60000)*60,
        tags
    )

    ,mt_burst_second
    as (
    select 
        avg(int(timestamp/60000)*60) as timestamp,
        avg(desired_capacity)*2.5 as desired_capacity,
        avg(fulfilled_capacity)*2.5 as fulfilled_capacity,
        avg(cluster_capacity)*2.5 as cluster_capacity,
        cluster
    from
        thomastest.mt_burst
    group by
        int(timestamp/60000)*60,
        cluster
    )

    ,clusters
    as (
    select distinct
        cluster
    from
        queue_by_second
    )

    ,cts_limit
    as (
    select
        cts.*
    from
        dp_derived.time_series_system_granularity_ext as cts
    join
        clusters as cls
    on
        cls.cluster = cts.system
    where
        cts.date between '2016-05-01' and '2016-05-10'
    )

select
    mt.timestamp as mt_timestamp,
    mt.desired_capacity as desired_capacity,
    mt.fulfilled_capacity as fulfilled_capacity,
    mt.cluster_capacity as cluster_capacity,
    mt.cluster as mt_cluster,

    qm.timestamp as qm_timestamp,
    qm.pendinggb as pendinggb,
    qm.allocatedgb as allcoatedgb,
    qm.availablegb as availablegb,
    qm.reservedgb as reservedgb,
    qm.cluster as qm_cluster,

    cts.memory,
    cts.memory_in_wait,
    cts.cluster_memory_capacity,
    cts.minute_start,
    cts.system
from
    cts_limit as cts
left outer join
    mt_burst_second mt
on
    cts.system = mt.cluster
    and cts.minute_start = mt.timestamp
left outer join
    queue_by_second as qm
on
    cts.system = qm.cluster
    and cts.minute_start = qm.timestamp
