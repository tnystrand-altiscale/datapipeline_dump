set hiveconf:START_DATE='2015-01-04';
set hiveconf:END_DATE='2016-02-04';
set hiveconf:SYSTEM='firstdata';


with
    -- For each container, find its maximum memory use
    tmp_wasted_container_space_percontainer
    as (
    select
        jobid,
        container_id,
        system,
        max(date) as date,
        count(*) as num_containers,
        max(phys_mem_used) as max_phys_watermark,
        phys_mem_total,
        max(virt_mem_used) as max_virt_watermark,
        virt_mem_total
    from
        cluster_metrics_prod_2.container_memory_fact
    where
        system=${hiveconf:SYSTEM}
        and date between ${hiveconf:START_DATE} and ${hiveconf:END_DATE}
        --and (jobid='job_1446505469556_101984' or jobid='job_1446505469556_106189' or jobid='job_1446505469556_107493')
    group by
        jobid,
        system,
        container_id,
        phys_mem_total,
        virt_mem_total
    )

    -- Container end_status is needed for debugging over use of container-limits
    ,tmp_container_fact
    as (
    select
        jobid,
        containerid,
        case
            when killedtime>0 then 'Killed'
            when releasedtime>0 then 'Released'
            when expiredtime>0 then 'Expired'
            when completedtime>0 then 'Completed'
            else 'Unknown'
        end as container_status,
        system,
        date
    from
        cluster_metrics_prod_2.container_fact
    where
        system=${hiveconf:SYSTEM}
        and date between ${hiveconf:START_DATE} and ${hiveconf:END_DATE}
    )

select 
    wcs.*,
    cf.container_status
from
    tmp_wasted_container_space_percontainer as wcs,
    tmp_container_fact as cf
where
    wcs.system = cf.system
    and wcs.date = cf.date
    and wcs.container_id = cf.containerid
