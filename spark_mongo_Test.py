
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, functions as F
from pyspark.sql.functions import *

#https://hangmortimer.medium.com/65-how-to-connect-pyspark-to-mongodb-eliminate-the-bug-of-not-finding-source-mongo-c59052832a32
spark = SparkSession \
.builder \
.master("local") \
.appName("ABC") \
.config("spark.mongodb.read.connection.uri", "mongodb+srv://{USER}:{PWD}@{DBHOST}:{DBPORT}/{DB_NAME}?authSource=admin") \
.config("spark.mongodb.write.connection.uri", "mongodb+srv://{USER}:{PWD}@{DBHOST}:{DBPORT}/{DB_NAME}?authSource=admin") \
.config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.2.0') \
.getOrCreate()

df = spark.read \
.format("mongodb") \
.option("uri", "mongodb+srv://{USER}:{PWD}@{DBHOST}:{DBPORT}/{DB_NAME}?authSource=admin") \
.option("database", "sensor_data") \
.option("collection", "sensor") \
.load()
df.printSchema()

print(df.show())
print(df.count())
