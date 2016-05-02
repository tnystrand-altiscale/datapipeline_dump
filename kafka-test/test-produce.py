from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(bootstrap_servers=['kafka01-us-west-1.test.altiscale.com:2181'])

# Asynchronous by default
future = producer.send('test_json', b'raw_bytes')

# Block for 'synchronous' sends
try:
    record_metadata = future.get(ticmeout=10)
except KafkaError:
    # Decide what to do if produce request failed...
    log.exception()
    pass

# Successful result returns assigned partition and offset
print (record_metadata.topic)
print (record_metadata.partition)
print (record_metadata.offset)

## produce keyed messages to enable hashed partitioning
#producer.send('test_json', key=b'dogfood', value=b'badass_long_message')
#
## encode objects via msgpack
#producer = KafkaProducer(value_serializer=msgpack.dumps)
#producer.send('msgpack-topic', {'key': 'value'})

# produce json messages
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
producer.send('test_json', {'dogfood': 'really_long_message'})

## produce asynchronously
#for _ in range(100):
#    producer.send('my-topic', b'msg')

# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(retries=5)
