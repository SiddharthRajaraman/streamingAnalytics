import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import pandas
from pyspark.sql import functions as f

spark = SparkSession.builder.master("local[2]").appName("OutlierDetection").getOrCreate()
#.builder - > builder API configures Spark session
#.master() -> Where the program will run - locally on 2 cores
#.appName() -> names spark method
#.getOrCreate() -> gets or creates a new Spark session based on appName



def getOutliers(data, columnName: str):
    #data is a spark column
    q1 = data.approxQuantile(columnName, [0.25], relativeError = 0)
    q3 = data.approxQuantile(columnName, [0.75], relativeError = 0)

    iqr = q3[0] - q1[0]

    lessq1 = q1[0] - 1.5*iqr
    moreq3 = q3[0] + 1.5*iqr

    columns = data.columns

    columnData = [] #list of single column data

    for value in data.collect():
        columnData.append(value[columnName])
    
    #columnData.append(900.5)


    outlierData = []
    for value in columnData:
        if value < lessq1 or value > moreq3:
            outlierData.append(1)
        else:
            outlierData.append(0)
    
    return outlierData



    return columnData
    



data = spark.read.csv('clusterMetrics.csv', inferSchema=True, header=True)

print(getOutliers(data, 'api_server_requests'))


