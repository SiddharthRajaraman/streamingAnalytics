from kafka import KafkaProducer
import time
import random
import json
from kafka.admin import KafkaAdminClient, NewTopic
import psutil


kafkaProducer = KafkaProducer(bootstrap_servers='localhost:9092')
topicName = 'myTopic'



counter = 0

while True:
    if counter % 15 == 0 and counter != 0:
        ack = kafkaProducer.send(topicName, key=str(counter).encode('utf-8'), value=str(99).encode("utf-8"))
        counter += 1
    elif counter % 14 == 0 and counter != 0:
        ack = kafkaProducer.send(topicName, key=str(counter).encode('utf-8'), value=str(0).encode("utf-8"))
        counter += 1
    else:
        ack = kafkaProducer.send(topicName, key=str(counter).encode('utf-8'), value=str(psutil.cpu_percent()).encode("utf-8"))
    counter += 1
    metadata = ack.get()
    print(metadata.topic)
    print(metadata.partition)
    time.sleep(.3)

