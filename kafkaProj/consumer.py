from kafka import KafkaConsumer
import sys

import kafka

topicName = 'exampleTopic'

kafkaConsumer = KafkaConsumer(topicName, bootstrap_servers='localhost:9092', auto_offset_reset = 'earliest')


print(kafkaConsumer.topics())


for message in kafkaConsumer:
    print("key: {}, value: {}, topic: {}, partition: {}".format(message.key, message.value, message.topic, message.partition))

