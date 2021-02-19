from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
from pyspark.sql.functions import count

import os
spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()
schema = StructType([
                    StructField("userID", IntegerType(), True), 
                    StructField("movieID", IntegerType(), True), 
                    StructField("rating", IntegerType(), True),
                    StructField("timestamp", LongType(), True)])
# Read file as dataframe
curwd = os.getcwd()
df = spark.read.option("sep", "\t").schema(schema).csv(f"file:///{curwd}/datasets/ml-100k/u.data")
print("printing schema: ")
df.printSchema()
# topMoviesIDs = df.groupBy("movieID").count().orderBy(func.desc("count"))
topMoviesIDs = df.groupBy("movieID").agg(count("timestamp").alias("countzer")).orderBy(func.desc("countzer"))
topMoviesIDs.show(10)
# kill session
spark.stop()


