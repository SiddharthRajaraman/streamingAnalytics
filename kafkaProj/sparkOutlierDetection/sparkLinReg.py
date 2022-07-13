from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.master("local[2]").appName("Learning_Spark").getOrCreate()
#.builder - > builder API configures Spark session
#.master() -> Where the program will run - locally on 2 cores
#.appName() -> names spark method
#.getOrCreate() -> gets or creates a new Spark session based on appName

data = spark.read.csv('clusterMetrics.csv', inferSchema=True, header=True)
'''
print(data.count())
print(len(data.columns))
data.show(5)
data.printSchema()
data.describe().show()
'''

inputcols = ['Cpu_usage','Memory_usage(bytes)','Memory_capacity(bytes)']

assembler = VectorAssembler(inputCols=inputcols, outputCol = "predictors")

predictors = assembler.transform(data)
predictors.show(5, truncate=False)

modelData = predictors.select("predictors", "api_server_requests")
modelData.show(5, truncate=False)

trainData,testData = modelData.randomSplit([0.8,0.2])

linReg = LinearRegression(featuresCol = 'predictors', labelCol = 'api_server_requests')

linRegModel = linReg.fit(trainData)
pred = linRegModel.evaluate(testData)

pred.predictions.show(15)

