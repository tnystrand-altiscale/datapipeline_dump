set hiveconf:START_DATE='2016-01-01';	
set hiveconf:END_DATE='2016-02-04';
set hiveconf:SYSTEM='firstdata';


with
    -- Select part of container_fact
    reduced_cf
    as (
    select
        *,
        -- Running time. (Including scheduling time)
        case
            when completedtime>0 and allocatedtime>0 then
                (completedtime-allocatedtime)/1000
            when releasedtime>0 and allocatedtime>0 then
                (releasedtime-allocatedtime)/1000
            when killedtime>0 and allocatedtime>0 then
                (killedtime-allocatedtime)/1000
            when expiredtime>0 and allocatedtime>0 then
                (expiredtime-allocatedtime)/1000
            else
                0
        end as container_runtime,
        if(allocatedtime>0,allocatedtime,null) as start_time,
        case
            when completedtime>0 then
                completedtime
            when releasedtime>0 then
                releasedtime
            when killedtime>0 then
                killedtime
            when expiredtime>0 then
                expiredtime
            else
                null
        end as finish_time

    from
        cluster_metrics_prod_2.container_fact
    where
        system=${hiveconf:SYSTEM}
        and date between ${hiveconf:START_DATE} and ${hiveconf:END_DATE}
    )

    -- Select part of container memory fact
    ,reduced_wcs
    as (
    select
        jobid,
        timestamp,
        process_id,
        container_id,
        phys_mem_used/1000/1000/1000 as phys_mem_used,
        phys_mem_total/1000/1000/1000 as phys_mem_total,
        virt_mem_used/1000/1000/1000 as virt_mem_used,
        virt_mem_total/1000/1000/1000 as virt_mem_total,
        system,
        date
    from
        cluster_metrics_prod_2.container_memory_fact as wcs
    where
        system=${hiveconf:SYSTEM}
        and date between ${hiveconf:START_DATE} and ${hiveconf:END_DATE}
    )

    -- Select part of job fact
    ,reduced_jobfact
    as (
    select
        *
    from
        cluster_metrics_prod_2.job_fact as jf
    where
        system=${hiveconf:SYSTEM}
        and date between ${hiveconf:START_DATE} and ${hiveconf:END_DATE}
    )

    -----------------------------------------------------------
    -- FINDING HIGHWATERMARK FOR EACH GROUP OF COINTAINER SIZE
    -----------------------------------------------------------

    -- Grouping series of memory data per container
    -- Maximum physical watermark is most interesteing
    ,tmp_wasted_container_space_percontainer
    as (
    select
        jobid,
        container_id,
        system,
        max(date) as date,
        count(*) as numhits, -- Number of timesteps
        max(phys_mem_used) as max_phys_watermark,
        avg(phys_mem_total) as phys_mem_total, -- Skew in time series
        max(virt_mem_used) as max_virt_watermark,
        avg(virt_mem_total) as virt_mem_total -- Skrew in time series
    from
        reduced_wcs
    group by
        jobid,
        system,
        container_id
    )

    -- Need container run lenght to calculate wasted container GBh (see blow)
    -- Need priority for separating mappers, reducers and app master (e.g. same size app master and mappers must be separated)
    -- app master: priority=0
    -- mappers: priority=20
    -- reducers: priority=10
    -- Need to join in container_fact at this point since
    -- the join key would otherwise be based on memory (which is not consistent between the tables)
    ,tmp_waste_with_runningtime
    as (
    select
        wcs.*,
        cf.container_runtime,
        cf.start_time,
        cf.finish_time,
        cf.priority
    from
       reduced_cf as cf
    join
        tmp_wasted_container_space_percontainer as wcs
    on
        cf.containerid = wcs.container_id
        and cf.system = wcs.system
        and cf.date = wcs.date
    )
    
    -- Grouping by cointainer size
    -- Note: can only modify container size by reducer and mappers
    ,tmp_wasted_container_space_persize
    as (
    select
        jobid,
        system,
        priority,
        max(date) as date,
        count(*) as numhits, -- Number of containers with certain size
        max(max_phys_watermark) as max_phys_watermark,
        phys_mem_total,
        max(virt_mem_total) as max_virt_watermark,
        virt_mem_total
    from
        tmp_waste_with_runningtime
    group by
        jobid,
        system,
        phys_mem_total,
        virt_mem_total,
        priority
    )

    -- Figure out number of different types of containers (based on size and priority)
    -- If this is bigger than three, we cannot really tune the job (only account for mappers, reducers and app master)
    ,tmp_num_types
    as (
    select
        count(*) as num_of_different_container_types,
        system,
        jobid,
        max(date) as date
    from 
        tmp_wasted_container_space_persize
    group by
        system,
        jobid
    )


    -----------------------------------------------------------
    -- JOIN IN MAX WATERMARK TO CONTAINER TABLES 
    -----------------------------------------------------------

    -- Join the initial table
    ,tmp_runningtime_and_watermark
    as (
    select
        wcs.*,
        wcsps.max_phys_watermark as maxest_phys_watermark,
        wcsps.max_virt_watermark as maxest_virt_watermark
    from
        tmp_wasted_container_space_persize as wcsps
    join
        tmp_waste_with_runningtime as wcs 
    on
        wcs.system = wcsps.system
        and wcs.date = wcsps.date
        and wcs.jobid = wcsps.jobid
        and wcs.phys_mem_total = wcsps.phys_mem_total
        and wcs.priority = wcsps.priority
    )


    -- Summing over total waste for each job, based on max improvement for each size group
    -- Not vaild for all apps though, such as SPARK which hides some of the details from the user
    ,tmp_wasted_container_space_perjob
    as (
    select
        jobid,
        system,
        max(date) as date,
        min(start_time) as start_time,
        max(finish_time) as finish_time,
        max(finish_time)-min(start_time) as duration,
        sum(container_runtime/1000/3600*(phys_mem_total-maxest_phys_watermark)) as total_phys_waste,
        sum(container_runtime/1000/3600*(virt_mem_total-maxest_virt_watermark)) as total_virt_waste,
        sum(container_runtime/1000/3600*phys_mem_total) as slot_hours_phys,
        sum(container_runtime/1000/3600*virt_mem_total) as slot_hours_virt,
        max(phys_mem_total) as max_container_size,
        max(if(priority==0,maxest_phys_watermark,null)) as app_master_highwatermark,-- App master hack: app mater always have priority 0
        max(if(priority==20,maxest_phys_watermark,null)) as map_highwatermark, -- Mappers hack: mappers always have priority 20
        max(if(priority==10,maxest_phys_watermark,null)) as reducer_highwatermark,  -- Reducers hack: reducers always have prority 10
        max(if(priority==0,phys_mem_total,null)) as app_master_size,-- App master hack: app mater always have priority 0
        max(if(priority==20,phys_mem_total,null)) as map_size, -- Mappers hack: mappers always have priority 20
        max(if(priority==10,phys_mem_total,null)) as reducer_size, -- Reducers hack: reducers always have prority 10
        count(*) as number_of_containers
    from
        tmp_runningtime_and_watermark
    group by
        jobid,
        system
    )

    -----------------------------------------------------------
    -- JOIN IN JOBSTATUS FOR (MANUAL) FILTERING OF JOBS WE CANNOT HELP
    -----------------------------------------------------------
    ,tmp_waste_and_jobstatus
    as (
    select
        wcs.*,
        nt.num_of_different_container_types,
        ---------------
        -- Don't trust these fields. totalmaps+totalreducers != number of containers
        -- It can! But most often wont.
        jf.totalmaps,
        jf.totalreduces,
        ---------------
        jf.username,
        total_phys_waste/slot_hours_phys as relative_phys_waste,
        total_virt_waste/slot_hours_virt as relative_virt_waste,
        (CASE when app = 'scalding' THEN 'scalding' --scalding detected by processing XML conf. in rm-jhist
                    when app = 'cascalog' THEN 'cascalog' --cascalog detected by processing XML conf. in rm-jhist
                    when jobname RLIKE "^[sS][eE][lL][eE][cC][tT] " THEN "hive_map_reduce"
                    when jobname RLIKE "^[cC][rR][eE][aA][tT][eE] " THEN "hive_map_reduce"
                    when jobname RLIKE "^[iI][nN][sS][eE][rR][tT] " THEN "hive_map_reduce"
                    when jobname RLIKE "\(Stage-[0-9]\)" THEN "hive_map_reduce"
                    when jobname LIKE "HIVE-%" THEN "hive_tez"
                    when jobname LIKE "H2O%" THEN "H2O"
                    when jobname LIKE "PigLatin%" THEN "pig"
                    when jobname = "distcp" THEN "distcp"
                    when jobname RLIKE "^[Ss]park" THEN "spark"
                    when jobname RLIKE "^[Pp]y[Ss]park" THEN "pyspark"
                    when jobname LIKE "MSMP%" THEN "marketshare_mp"
                    when jobname LIKE "streamjob%" THEN "streaming_map_reduce"
                    when jobname LIKE 'vmc-camus.jar%' THEN 'camus'
                    when jobname LIKE 'Camus Job%' THEN 'camus'
                    when jobname LIKE 'oozie:launcher%' THEN 'oozie_launcher'
                    when jobname RLIKE '^[Ss]qoop' THEN 'sqoop'
                    when jobname LIKE 'oozie:action:T=map-reduce%' THEN 'other_map_reduce'
                    when jobname LIKE 'oozie:action:T\\=map-reduce%' THEN 'other_map_reduce'
                    when jobname LIKE 'oozie:action:T=sqoop%' THEN 'sqoop'
                    when jobname LIKE 'oozie:action:T\\=sqoop%' THEN 'sqoop'
                    when jobname RLIKE '^\\[[A-F0-9]+/' THEN 'cascading'
                    when totalmaps IS NOT NULL THEN 'other_map_reduce'
                    ELSE 'unknown' END
            ) as app,
        jf.jobstatus
    from
        tmp_wasted_container_space_perjob as wcs
    join
        reduced_jobfact as jf
    on
        wcs.system = jf.system
        and wcs.date = jf.date
        and wcs.jobid = jf.jobid
    join
        tmp_num_types as nt
    on
        wcs.system = nt.system
        and wcs.date = nt.date
        and wcs.jobid = nt.jobid
    )

--select * from tmp_wasted_container_space_persize  --tmp_wasted_container_space_perjob
select * from tmp_waste_and_jobstatus
--select * from tmp_runningtime_and_watermark
