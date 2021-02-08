from pyspark.sql import SparkSession
import os
spark = SparkSession.builder.appName("SparkSQLDataframes").getOrCreate()
curwd = os.getcwd()
people = spark.read.option("header", "true").option("inferSchema", "true").csv(f"file:///{curwd}/fakefriends-header.csv")
print("log: check our inferred schema: ")
people.printSchema()
print("showing only name column from data")
people.select("name").show(5)
print("filter out anyove over 21: ") 
people.filter(people.age > 21).show(5)
print("Group By Age:")
people.groupBy("age").count().show(5)
print("Make everyone 50 years older")
people.select(people.name, people.age + 50).show(5)

spark.stop()
