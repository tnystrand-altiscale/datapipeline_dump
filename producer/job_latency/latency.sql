set hiveconf:TGT_TABLE='job_latency';
set hiveconf:START_DATE='2016-03-01';
set hiveconf:END_DATE='2016-03-04';
set hiveconf:FACT_TABLE='container_fact';



create table if not exists
    ${hiveconf:TGT_TABLE} (
        job_id                                      String,
        launchtime                                  Double,
        jobruntime                                  Double,
        wait_on_jobstart                            Double,

        jobfinishtime                               Double,
        duration                                    Double,

        max_wait_time                               Double,
        max_run_time                                Double,

        total_wait_time                             Double,
        total_run_time                              Double,
        total_lifecycle_time                        Double,

        average_container_wait_ratio                Double,
        total_wait_mbmin                            Double,
        total_run_mbmin                             Double,

        num_containers_waiting_over10               Int,
        num_containers_waiting_over30               Int,

        optimal_mr_time                             Double
    )
partitioned by (
    system string,
    date string
    )
stored as
    orc;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

with

    -- Padding columns to container_fact
    tmp_container_state_times_and_inefficiency
    as (
    select
        containerid,
        memory,
        vcores,
        requestedtime,
        completedtime,
        jobid,
        queue,
        priority,
        system,
        date,
    
        -- Inefficiency. Not including scheduling overheads.
        -- Killed/released/expired containers are counted as normal running contaienrs
        -- The order of when clauses matters
        case 
            when completedtime>0 and allocatedtime>0 and requestedtime>0 then
                (allocatedtime-requestedtime)/(completedtime-requestedtime)
            when completedtime>0 and allocatedtime>0 and killedtime>0 then
                (allocatedtime-requestedtime)/(killedtime-requestedtime)
            when completedtime>0 and allocatedtime>0 and releasedtime>0 then
                (allocatedtime-requestedtime)/(releasedtime-requestedtime)
            when completedtime>0 and allocatedtime>0 and expiredtime>0 then
                (allocatedtime-requestedtime)/(expiredtime-requestedtime)
            when requestedtime>0 then
                1
            else
                0
        end as inefficiency,

        -- Complete lifecycle time. The order of when clauses matters
        -- Total time a container has been in a recognised YARN container state.
        case
            when(completedtime>0 and requestedtime>0) then
                (completedtime-requestedtime)/1000
            when(completedtime>0 and reservedtime>0) then
                (completedtime-reservedtime)/1000
            when(expiredtime>0 and requestedtime>0) then
                (expiredtime-requestedtime)/1000
            when(expiredtime>0 and reservedtime>0) then
                (expiredtime-reservedtime)/1000
            when(killedtime>0 and requestedtime>0) then
                (killedtime-requestedtime)/1000
            when(killedtime>0 and reservedtime>0) then
                (killedtime-reservedtime)/1000
            when(releasedtime>0 and requestedtime>0) then
                (releasedtime-requestedtime)/1000
            when(releasedtime>0 and reservedtime>0) then
                (releasedtime-reservedtime)/1000
            else
                null
        end as fulltime,


        -- Container end time
        case
            when completedtime>0  then
                completedtime/1000
            when releasedtime>0  then
                releasedtime/1000
            when killedtime>0  then
                killedtime/1000
            when expiredtime>0 then
                expiredtime
            else
                null
        end as maxtime,
    

        -- Waiting time. (Excluding scheduling time)
        case when allocatedtime>0 and requestedtime>0 then
            (allocatedtime-requestedtime)/1000
        else
            0
        end as waitingtime,

        -- Waiting time. (Including scheduling time). Use for initial job wait.
        case when runningtime>0 and requestedtime>0 then
            (runningtime-requestedtime)/1000
        else
            0
        end as waitingtime,


        -- Completedtime. Normal 'alive' time for a container.
        case when completedtime>0 and requestedtime>0 then
            (completedtime-requestedtime)/1000
        else
            0
        end as completingtime,
        
        
        -- Running time. (Excluding scheduling time)
        case 
            when completedtime>0 and acquiredtime>0 then
                (completedtime-acquiredtime)/1000
            when releasedtime>0 and acquiredtime>0 then
                (releasedtime-acquiredtime)/1000
            when killedtime>0 and acquiredtime>0 then
                (killedtime-acquiredtime)/1000
            when expiredtime>0 and acquiredtime>0 then
                (expiredtime-acquiredtime)/1000
            else
                0
        end as runningtime,
   
        -- Time wasted in scheduling overheads
        case when allocatedtime>0 and acquiredtime>0 then
            (acquiredtime-allocatedtime)/1000
        else
            0
        end as schedulingtime,
        
    
        -- Realease time. 'Wasted' running time on release containers.
        case when releasedtime>0 and acquiredtime>0 then
            (releasedtime-acquiredtime)/1000
        else
            0
        end as wasted_releasetime,
        
        -- Killed time. 'Wasted' running time on killed containers.
        case when killedtime>0 and acquiredtime>0 then
            (killedtime-acquiredtime)/1000
        else
            0
        end as wasted_killedtime,
    
        -- Expired time. 'Wasted' time on expired overheads.
        case when allocatedtime>0 and expiredtime>0 then
            (expiredtime-allocatedtime)/1000
        else
            0
        end as wasted_expiredtime,
       
        case when killedtime>0 then 1 else 0 end as killed,
        case when releasedtime>0 then 1 else 0 end as released,
        case when expiredtime>0 then 1 else 0 end as expired,
        case when completedtime>0 then 1 else 0 end as completed,

        -- For MapReduce jobs priority is 10 and 20
        -- Used to find hypotehtical optimal mapreduce execution time
        if(priority=10 or priority=20,
            case when allocatedtime>0 and completedtime>0 then
                (completedtime-allocatedtime)/1000
            else
                null
            end,
            null) as mapreduceruntime,

        if(requestedtime>0,requestedtime/1000,NULL) as starttime,
        if(runningtime>0,runningtime/1000,NULL) as runtime
    from
        ${hiveconf:FACT_TABLE}
    where
        date between ${hiveconf:START_DATE} and ${hiveconf:END_DATE}
    ),

    tmp_job_ineff_stats 
    as (
    select 
        jobid,
        priority,
        min(queue) as queue,

        count(*) as num_containers,
        sum(killed) as num_killed,
        sum(released) as num_released,
        sum(expired) as num_expired,
        sum(completed) as num_completed,

        max(waitingtime) as max_wait_time,
        avg(inefficiency) as inefficiency,
        sum(waitingtime) as waitingtime,
        sum(runningtime) as runningtime,
        sum(completingtime) as completingtime,
        max(maxtime) as finishtime,
        min(starttime) as starttime,
        min(runtime) as runtime,
        sum(fulltime) as totaltime,
        sum(waitingtime/60*memory) as waitingmemtime,
        sum(waitingtime/60*vcores) as waitingvcrtime,
        sum(runningtime/60*memory) as runningmemtime,
        sum(fulltime/60*memory) as totalmemtime,
        sum(if(waitingtime>10,1,0)) as waitingcontainers10,
        sum(if(waitingtime>30,1,0)) as waitingcontainers30,

        system,
        max(date) as date
    from
        tmp_container_state_times_and_inefficiency
    group by
        jobid,
        system,
    ),

    
insert overwrite table ${hiveconf:TGT_TABLE}
partition(system,date)

select
    *
from
    tmp_job_ineff_stats
