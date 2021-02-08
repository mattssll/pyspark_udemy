from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import os
spark = SparkSession.builder.appName("SparkSQLDataframes").getOrCreate()
curwd = os.getcwd()
people = spark.read.option("header", "true").option("inferSchema", "true").csv(f"file:///{curwd}/fakefriends-header.csv")
print("log: check our inferred schema: ")
people.printSchema()

print("showing only name column from data")
people.select("age","friends").show(10)
print("group by age with average of friends")
people.select("age","friends").groupBy("age").avg("friends").sort("age").show(5)
print("doing the same with agg and with round: ")
people.select("age","friends") \
        .groupBy("age") \
        .agg(func.round(func.avg("friends"),2)) \
        .alias("n_friends_avg") \
        .sort("age") \
        .show(5)

spark.stop()
