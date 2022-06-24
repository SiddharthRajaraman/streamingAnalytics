'''
gets topic data
- num. partitions
- list of topics
'''



from kafka import KafkaConsumer

def get_partitions_number(server, topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=server
    )
    partitions = consumer.partitions_for_topic(topic)
    return len(partitions)


print(get_partitions_number('localhost:9092', 'myTopic'))