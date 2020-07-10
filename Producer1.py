from pyspark import SparkContext, SparkConf # Spark
from pyspark.sql import SparkSession # Spark SQL
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext.getOrCreate()

if (sc is None):
    sc = SparkContext(master="local[3]", appName="Introduction to Apache Spark")
spark = SparkSession(sparkContext=sc)

#Nodes TABLE
scNodes = StructType([
StructField("date",StringType()),
StructField("US",FloatType()),
StructField("EU",FloatType()),
StructField("GBP",FloatType()),
StructField("id",IntegerType())
])

#Nodes DATA
dataNodes = sc.textFile('Data.csv')
dataNodes = dataNodes.map(lambda x: x.split(','))
dataNodes = dataNodes.map(lambda x: [x[0],float(x[1]), float(x[2]),float(x[3]),int(x[4])])
dfNodes = spark.createDataFrame(dataNodes,schema=scNodes) 
dfNodes.createOrReplaceTempView("dataNodes")
from time import sleep
from json import dumps
from kafka import KafkaProducer
import random

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))
idx = 1
key = "data1"
for e in range(dfNodes.count()):
    result = dfNodes.select("US","date").filter("id=" + str(idx))
    txt = "US" + "," + str(result.first().US) + "," + str(result.first().date)
    data = txt
    print(data)
    producer.send(key, value=data)
    idx=idx+1
    sleep(1)

