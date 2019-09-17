create table container_fact
(
jobid               	string              	                    ,
containerid         	string              	                    ,
requestedtime       	timestamp              	                    ,
reservedtime        	timestamp              	                    ,
allocatedtime       	timestamp              	                    ,
acquiredtime        	timestamp              	                    ,
expiredtime         	timestamp              	                    ,
runningtime         	timestamp              	                    ,
killedtime          	timestamp              	                    ,
releasedtime        	timestamp              	                    ,
completedtime       	timestamp              	                    ,
memory              	bigint              	                    ,
vcores              	int                 	                    ,
queue               	string              	                    ,
host                	string              	                    ,
priority            	int                 	                    ,
account             	bigint              	                    ,
cluster_uuid        	string              	                    ,
principal_uuid      	string              	                    ,
user_key            	string              	                    ,
clustermemory       	bigint              	                    ,
numberapps          	bigint              	                    ,
system              	string              	                    ,
date                	timestamp              	                    
)
PARTITION BY RANGE (date) EVERY (INTERVAL '3' month)
;
