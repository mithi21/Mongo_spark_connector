#bin/spark-shell --conf "spark.driver.extraJavaOptions=-Dhttp.proxyHost=<proxyHost> -Dhttp.proxyPort=<proxyPort> -Dhttps.proxyHost=<proxyHost> -Dhttps.proxyPort=<proxyPort>" --packages <somePackage>
#https://stackoverflow.com/questions/36676395/how-to-resolve-external-packages-with-spark-shell-when-behind-a-corporate-proxy
#https://stackoverflow.com/questions/36676395/how-to-resolve-external-packages-with-spark-shell-when-behind-a-corporate-proxy
#proxyPort=<proxyPort> -Dhttps.proxyHost=<proxyHost> -Dhttps.proxyPort=<proxyPort>
#to $SPARK_HOME/conf/spark-defaults.conf works for me.
#https://rangareddy.github.io/SparkSubmitProxyConfiguration/
#https://docs.informatica.com/integration-cloud/data-integration/current-version/transformations/machine-learning-transformation/configuring-an-api-proxy/configuring-a-spark-proxy.html
#https://docs.cloudera.com/machine-learning/saas/spark/topics/ml-proxy-setup.html
#https://docs.cloudera.com/machine-learning/saas/spark/topics/ml-proxy-setup.html
#https://sparkbyexamples.com/spark/spark-set-jvm-options-to-driver-executors/
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
