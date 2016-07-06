create index container_fact_jobid_index on table thomastest.container_fact_test (jobid) as 'compact' WITH DEFERRED REBUILD;
alter index container_fact_jobid_index on thomastest.container_fact_test rebuild;
-- drop index container_fact_jobid_index on thomastest.container_fact_test rebuild;
