from kafka import KafkaProducer
import time
import random
import json
from kafka.admin import KafkaAdminClient, NewTopic


bootstrap_servers = ['localhost:9092']
topicName = 'myTopic'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer()

#create topics
admin_client = KafkaAdminClient (
    bootstrap_servers="localhost:9092",
    client_id='test'
)

topicList = []
topicList.append(NewTopic(name = 'exampleTopic', num_partitions=3, replication_factor=1))
admin_client.create_topics(new_topics=topicList, validate_only=False)
#####


counter = 0

while True:
    ack = producer.send(topicName, key=json.dumps(str(counter)).encode('utf-8'), value=json.dumps(str(random.randint(0,10))).encode('utf-8'))
    counter += 1
    metadata = ack.get()
    print(metadata.topic)
    print(metadata.partition)
    time.sleep(1)


'''
ack = producer.send(topicName, 'Hello World!!!!!!!!')




metadata = ack.get()
print(metadata.topic)
print(metadata.partition)
'''

