'''
gets topic data
- num. partitions
- list of topics
'''

from ensurepip import bootstrap
from http import server
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic


kafkaConsumer = KafkaConsumer(bootstrap_servers = 'localhost:9092')
adminClient = KafkaAdminClient(bootstrap_servers = 'localhost:9092')

def createTopic(topicName: str, numPartitions: int, replicationFactor: int):
    #check if topic exists
    if topicName in kafkaConsumer.topics():
        print('Topic: {} already exists'.format(topicName))
    else:
        '''
        create Topic
        num_partitions: number of partitions created
        replicationFactor: how many copies of the partition are stored; must be 1 since only 1 broker exists
        '''
        adminClient.create_topics(new_topics=[NewTopic(name=topicName, num_partitions=numPartitions, replication_factor=replicationFactor)])
        print('Topic: {} successfully created'.format(topicName))

def deleteTopic(topicName: str):
    #check if topic exists
    if topicName in kafkaConsumer.topics():
        adminClient.delete_topics(topics=[topicName])
        print('Topic: {} successfully deleted'.format(topicName))        
    else:
        print('Topic: {} does not exists'.format(topicName))

#get number of partitions for a given topic
def getPartitions(topicName: str) -> int:
    partitions = kafkaConsumer.partitions_for_topic(topicName)
    return len(partitions)

'''
createTopic(topicName='test', numPartitions=3, replicationFactor=1)
print(kafkaConsumer.topics())
deleteTopic(topicName='test')
'''
'''
deleteTopic('myTopic')
deleteTopic('exampleTopic')
'''

print(kafkaConsumer.topics())
deleteTopic('myTopic')

#createTopic(topicName = "myTopic", numPartitions=1, replicationFactor=1)


#print(getPartitions('exampleTopic'))

#list all topics in cluster
print(kafkaConsumer.topics())















