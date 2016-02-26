set hiveconf:cluster='glu';
set hiveconf:start_date='2015-09-01';
set hiveconf:end_date='2015-09-31';
set hiveconf:queue_dim=cluster_metrics_prod_2.queue_dim;
set hiveconf:resource_dim=cluster_metrics_prod_2.cluster_resource_dim;

with
    -- Jobs with an added type split for long running jobs
    job_facts
    as (
    select
        jobid as job_id,
        max(
            case when requestedtime>0 and allocatedtime>0 then allocatedtime-requestedtime
                 when reservedtime>0 and allocatedtime>0 then allocatedtime-reservedtime
                 else NULL
                 end
        ) as max_waittime,
        min(
            case when requestedtime>0 then requestedtime
                 when reservedtime>0 then reservedtime
                 else NULL
                 end
            ) as job_start,
        max(
            case when completedtime>0 then completedtime
                 when killedtime>0 then killedtime
                 when releasedtime>0 then releasedtime
                 else NULL
                 end
            ) as job_end,
        system,
        min(date) as date
    from
        cluster_metrics_prod_2.container_fact
    where
        date between ${hiveconf:start_date} and ${hiveconf:end_date}
        and system='wikimedia'
    group by
        jobid,
        system
    )

    ,robbed_report_with_type
    as(
    select
        *,
        (job_end-job_start-max_waittime)/1000 as optimal_mr_time,
        (job_end-job_start)/1000 as duration,
        if((job_end-job_start-max_waittime)/1000>300,'long','short') as jobtype
    from
        job_facts
    )

    -- Each jobs total running and waiting memory each minute
    ,cts_by_job_and_min
    as(
    select
        job_id,
        minute_start,
        sum(container_wait_time) as jobwait,
        sum(memory) as runningmem,
        system,
        min(date) as date
    from
        cluster_metrics_prod_2.container_time_series
    where
        --system = ${hiveconf:cluster}
        --and date between ${hiveconf:start_date} and ${hiveconf:end_date}
        date between ${hiveconf:start_date} and ${hiveconf:end_date}
        and system='wikimedia'
    group by
        minute_start,
        job_id,
        system
    )

    -- Add jobtype to table with total job waiting and running mem per min
    ,cts_extended_with_type
    as(
    select
        rr.job_id,
        rr.jobtype,
        cts.minute_start,
        cts.jobwait,
        cts.runningmem,
        cts.system
    from
        robbed_report_with_type as rr
    join
        cts_by_job_and_min as cts
    on
        rr.job_id=cts.job_id
        and rr.system=cts.system
    )

    -- All long and short running jobs running memory per minute
    ,cts_job_run
    as (    
    select
        minute_start,
        system,
        sum(if(jobtype='long',runningmem,0)) as alllongrunningmem,
        sum(if(jobtype='short',runningmem,0)) as allshortrunningmem
    from
        cts_extended_with_type
    group by
        minute_start,
        system
    )

    -- Each job for each minute has some waiting mem attributed to its own running mem
    -- and some to the longrunning jobs mem
    ,cts_with_ratios
    as (
    select
        cts.job_id,
        cts.minute_start,
        if(jobtype='long',
            runningmem/(runningmem+allshortrunningmem),runningmem/(runningmem+alllongrunningmem)) as selfratio,
        if(jobtype='long',
            allshortrunningmem/(runningmem+allshortrunningmem),alllongrunningmem/(runningmem+alllongrunningmem)) as otherratio,
        jobwait,
        cts.system
    from
        cts_extended_with_type as cts
    join
        cts_job_run as ctsgp
    on
        ctsgp.minute_start=cts.minute_start
        and ctsgp.system=cts.system
    )

    -- Sum each minutes job wait split
    ,wait_split
    as(
    select
        job_id,
        sum(jobwait*selfratio) as self_caused_wait,
        sum(jobwait*otherratio) as other_caused_wait,
        system
    from
       cts_with_ratios as cts
    group by
        job_id,
        system
    )

    -- Divide optimal time based on whether the wait is attributed to self or other
    ,optimal_time_split
    as (
    select
        duration-optimal_mr_time as potential_improvement,
        (duration-optimal_mr_time)*self_caused_wait/(self_caused_wait+other_caused_wait) as self_imporve_optmrtime,
        (duration-optimal_mr_time)*other_caused_wait/(self_caused_wait+other_caused_wait) as other_imporve_optmrtime,
        rr.*
    from
        wait_split as cts
    join
        robbed_report_with_type as rr
    on
        cts.job_id=rr.job_id
        and cts.system=rr.system
    )


select * from optimal_time_split
