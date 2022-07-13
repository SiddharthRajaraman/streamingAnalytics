from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
import matplotlib.pyplot as plt


kafkaConsumer = KafkaConsumer('myTopic', bootstrap_servers='localhost:9092', auto_offset_reset = 'earliest')

spark = SparkSession.builder.master("local[2]").appName("OutlierDetection").getOrCreate()
#.builder - > builder API configures Spark session
#.master() -> Where the program will run - locally on 2 cores
#.appName() -> names spark method
#.getOrCreate() -> gets or creates a new Spark session based on appName

#function to convert lists to spark dataframes
def convertToDf(metricList):
    metricsDf = spark.createDataFrame(metricList, FloatType())
    metricsDf = metricsDf.withColumnRenamed('value', 'Metrics')

    return metricsDf


#checks if value is an outlier
def isOutlier(metricsDf, metric, columnName):
    q1 = metricsDf.approxQuantile(columnName, [0.25], relativeError = 0)
    q3 = metricsDf.approxQuantile(columnName, [0.75], relativeError = 0)

    iqr = q3[0] - q1[0]
    
    lessq1 = q1[0] - 1.5*iqr
    moreq3 = q3[0] + 1.5*iqr


    if metric < lessq1 or metric > moreq3:
        return True
    
    return False

metricList = []
metricsDf = convertToDf(metricList)

plt.ion()
for message in kafkaConsumer:
    if len(metricList) < 2:
        print("Key: {}, Value: {}, Outlier: Not enough data".format(message.key.decode('utf-8'), message.value.decode('utf-8')))
        metricList.append(float(message.value.decode('utf-8')))
        plt.plot(metricList, color = 'black')
        plt.scatter(len(metricList)-1, metricList[-1], color = 'black')
        plt.draw()
        plt.pause(.1)
    else:
        if isOutlier(metricsDf, float(message.value.decode('utf-8')), 'Metrics'):
            metricList.append(float(message.value.decode('utf-8')))
            plt.plot(metricList, color = 'black')
            plt.scatter(len(metricList)-1, metricList[-1], color = 'red')
            plt.draw()
            plt.pause(.1)
        else:
            metricList.append(float(message.value.decode('utf-8')))
            plt.plot(metricList, color = 'black')
            plt.scatter(len(metricList)-1, metricList[-1], color = 'black')
            plt.draw()
            plt.pause(.1)

        print("Key: {}, Value: {}, Outlier: {}".format(message.key.decode('utf-8'), message.value.decode('utf-8'), isOutlier(metricsDf, float(message.value.decode('utf-8')), 'Metrics')))
    
    metricsDf = convertToDf(metricList)
