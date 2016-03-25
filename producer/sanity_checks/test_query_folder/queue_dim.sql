-- Selects x rows from each system on a given day

with
    ranked
    as (
    select
        *,
        row_number() over(partition by queue_system order by queue_date) as system_number
    from
        cluster_metrics_prod_2.queue_dim
    where
        queue_date='${hivevar:date}'
    )
select * from ranked where system_number<11
