with
    queue_by_second
    as (
    select
        avg(int(timestamp/1000)) as timestamp,
        avg(pendingmb)/1000 as pendinggb,
        avg(allocatedmb)/1000 as allocatedgb,
        avg(availablemb)/1000 as availablegb,
        avg(reservedmb)/1000 as reservedgb,
        tags as cluster
    from
        thomastest.queue_metrics
    group by
        int(timestamp/1000),
        tags
    )

    ,mt_burst_second
    as (
    select 
        avg(int(timestamp/1000)) as timestamp,
        avg(desired_capacity)*2.5 as desired_capacity,
        avg(fulfilled_capacity)*2.5 as fulfilled_capacity,
        avg(cluster_capacity)*2.5 as cluster_capacity,
        cluster
    from
        thomastest.mt_burst
    group by
        int(timestamp/1000),
        cluster
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
    qm.cluster as qm_cluster
from
    mt_burst_second mt
full outer join
    queue_by_second as qm
on
    qm.cluster = mt.cluster
    and qm.timestamp = mt.timestamp
