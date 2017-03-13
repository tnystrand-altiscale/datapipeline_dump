        create table if not exists cluster_metrics_prod_2.container_fact_event_flattened
            (
            jobid       string,
            containerid string,
            start       bigint,
            stop        bigint,
            duration    bigint,
            event       string,
            size        double,
            priority    int,
            hostname    string
            )
        partitioned by
            (
            system      string,
            date        string
            )
        stored as orc
