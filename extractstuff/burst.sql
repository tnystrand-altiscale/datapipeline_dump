create table ziff_scratch.bts as
select
    allocatedgb,
    pendinggb,
    mt_burst_queue,
    target_capacity,
    matched_inventory,
    requested_delta,
    minute_start,
    case when (not isnull(locked_by)) and (locked_by != 'None')
                    then 0
                when matched_inventory = 0
                    then 1
                else
                    2
                end as lock_status,
    cluster_capacity,
    queue,
    system,
    partition_date,
    safety_floor_min_slots,
    inflexible_slots,
    slot_plan
from
    cluster_metrics_prod_2.burst_time_series_patchjoin
where
    partition_date >= '2017-05-01';
