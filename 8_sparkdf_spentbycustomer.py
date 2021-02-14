from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()
schema = StructType([
                    StructField("custID", IntegerType(), True), 
                    StructField("notImportant", IntegerType(), True), 
                    StructField("spent", FloatType(), True)])
curwd = os.getcwd()
# Read file as dataframe
df = spark.read.schema(schema).csv(f"file:///{curwd}/datasets/8-customer-orders.csv")
print("printing schema: ")
df.printSchema()

# grouping By the amount by custID
groupedByResults = df.select("custID","spent") \
                            .groupBy("custID") \
                            .agg(func.round(func.sum("spent"),2) \
                            .alias("sumspent")) \
                            .sort("sumspent")
groupedByResults.show(groupedByResults.count()) # inside show is number of rows to print



