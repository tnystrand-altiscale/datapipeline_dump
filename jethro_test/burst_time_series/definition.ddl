create table container_fact_event_flattened
(
mt_timestamp        	timestamp,
matched_inventory   	double,
desired_capacity    	double,
requested_delta     	double,
cluster_capacity    	double,
locked_by           	string,
mt_cluster          	string,
qm_timestamp        	timestamp,
pendinggb           	double,
allocatedgb         	double,
availablegb         	double,
reservedgb          	double,
qm_cluster          	string,
qm_queue            	string,
memory              	double,
memory_in_wait      	double,
cluster_memory_capacity	double,
minute_start        	timestamp,
cts_queue           	string,
fulfilled_capacity  	double,
flow_status         	string,
flow_duration       	double,
partition_date      	timestamp,
system              	string
)
PARTITION BY RANGE (date) EVERY (INTERVAL '3' month)
