from kafka import KafkaProducer
import time
import random
import json
from kafka.admin import KafkaAdminClient, NewTopic



kafkaProducer = KafkaProducer(bootstrap_servers='localhost:9092')
topicName = 'exampleTopic'


counter = 0

while True:
    ack = kafkaProducer.send(topicName, key=json.dumps(str(counter)).encode('utf-8'), value=json.dumps(str(random.randint(0,10))).encode('utf-8'))
    counter += 1
    metadata = ack.get()
    print(metadata.topic)
    print(metadata.partition)
    time.sleep(1)

