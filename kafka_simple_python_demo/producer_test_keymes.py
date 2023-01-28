from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Asynchronous by default
future = producer.send('test', b'DNP VNU', key=bytes('DuPham', 'utf-8'))

# Block for 'synchronous' sends
try:
    record_metadata = future.get(timeout=100)
except KafkaError:
    # Decide what to do if produce request failed...
    log.exception()
    pass

# Successful result returns assigned partition and offset
print ("topic name: " + str(record_metadata.topic))
print ("partition: " + str(record_metadata.partition))
print ("offset: " + str(record_metadata.offset))