import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import pandas

spark = SparkSession.builder.master("local[2]").appName("Learning_Spark").getOrCreate()
#.builder - > builder API configures Spark session
#.master() -> Where the program will run - locally on 2 cores
#.appName() -> names spark method
#.getOrCreate() -> gets or creates a new Spark session based on appName

data = spark.read.csv('clusterMetrics.csv', inferSchema=True, header=True)


data = data.select('Cpu_usage').toPandas()





print(type(data))

plt.plot(data)
plt.show()

