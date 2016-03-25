-- Selects x rows from each system on a given day

with
    ranked
    as (
    select
        *,
        row_number() over(partition by cluster_system order by cluster_date) as system_number
    from
        cluster_metrics_prod_2.cluster_resource_dim
    where
        cluster_date='${hivevar:date}'
    )
select * from ranked where system_number<11
