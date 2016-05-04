from kafka import KafkaProducer
from kafka.errors import KafkaError
import json



# produce json messages
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'), bootstrap_servers=['kafka01-us-west-1.test.altiscale.com:9092'])
future = producer.send('test_json', {'marketshare': {"hostname": "106-01-07.sc1.verticloud.com", "timestamp": 1462209001751, "date": "2016-05-02","time": "05:10:01","tags": {"cluster": "mtperf"},"name": "QueueMetrics" , "Queue":  "root.interactive", "Context":  "yarn", "Hostname":  "106-01-07.sc1.verticloud.com", "running_0":  "0", "running_60":  "0", "running_300":  "0", "running_1440":  "0", "AppsSubmitted":  "0", "AppsRunning":  "0", "AppsPending":  "0", "AppsCompleted":  "0", "AppsKilled":  "0", "AppsFailed":  "0", "AllocatedMB":  "0", "AllocatedVCores":  "0", "AllocatedContainers":  "0", "AggregateContainersAllocated":  "0", "AggregateNodeLocalContainersAllocated":  "0", "AggregateRackLocalContainersAllocated":  "0", "AggregateOffSwitchContainersAllocated":  "0", "AggregateContainersReleased":  "0", "AvailableMB":  "21934", "AvailableVCores":  "7", "PendingMB":  "0", "PendingVCores":  "0", "PendingContainers":  "0", "ReservedMB":  "0", "ReservedVCores":  "0", "ReservedContainers":  "0", "ActiveUsers":  "0", "ActiveApplications":  "0"}})
future = producer.send('test_json', {'iheartradio': {"hostname": "106-01-07.sc1.verticloud.com", "timestamp": 1462209001751, "date": "2016-05-02","time": "05:10:01","tags": {"cluster": "mtperf"},"name": "QueueMetrics" , "Queue":  "root.interactive", "Context":  "yarn", "Hostname":  "106-01-07.sc1.verticloud.com", "running_0":  "0", "running_60":  "0", "running_300":  "0", "running_1440":  "0", "AppsSubmitted":  "0", "AppsRunning":  "0", "AppsPending":  "0", "AppsCompleted":  "0", "AppsKilled":  "0", "AppsFailed":  "0", "AllocatedMB":  "0", "AllocatedVCores":  "0", "AllocatedContainers":  "0", "AggregateContainersAllocated":  "0", "AggregateNodeLocalContainersAllocated":  "0", "AggregateRackLocalContainersAllocated":  "0", "AggregateOffSwitchContainersAllocated":  "0", "AggregateContainersReleased":  "0", "AvailableMB":  "21934", "AvailableVCores":  "7", "PendingMB":  "0", "PendingVCores":  "0", "PendingContainers":  "0", "ReservedMB":  "0", "ReservedVCores":  "0", "ReservedContainers":  "0", "ActiveUsers":  "0", "ActiveApplications":  "0"}})
future = producer.send('test_json', {'dogfood': {"hostname": "106-01-07.sc1.verticloud.com", "timestamp": 1462209001751, "date": "2016-05-02","time": "05:10:01","tags": {"cluster": "mtperf"},"name": "QueueMetrics" , "Queue":  "root.interactive", "Context":  "yarn", "Hostname":  "106-01-07.sc1.verticloud.com", "running_0":  "0", "running_60":  "0", "running_300":  "0", "running_1440":  "0", "AppsSubmitted":  "0", "AppsRunning":  "0", "AppsPending":  "0", "AppsCompleted":  "0", "AppsKilled":  "0", "AppsFailed":  "0", "AllocatedMB":  "0", "AllocatedVCores":  "0", "AllocatedContainers":  "0", "AggregateContainersAllocated":  "0", "AggregateNodeLocalContainersAllocated":  "0", "AggregateRackLocalContainersAllocated":  "0", "AggregateOffSwitchContainersAllocated":  "0", "AggregateContainersReleased":  "0", "AvailableMB":  "21934", "AvailableVCores":  "7", "PendingMB":  "0", "PendingVCores":  "0", "PendingContainers":  "0", "ReservedMB":  "0", "ReservedVCores":  "0", "ReservedContainers":  "0", "ActiveUsers":  "0", "ActiveApplications":  "0"}})
record_metadata = future.get(timeout=10)
print (record_metadata.topic)
print (record_metadata.partition)
print (record_metadata.offset)

## produce asynchronously
#for _ in range(100):
#    producer.send('my-topic', b'msg')

# block until all async messages are sent
producer.flush()

