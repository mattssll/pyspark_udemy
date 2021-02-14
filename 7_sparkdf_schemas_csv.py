from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()
schema = StructType([
                    StructField("stationID", StringType(), True), 
                    StructField("date", IntegerType(), True), 
                    StructField("measure_type", StringType(), True), 
                    StructField("temperature", FloatType(), True)])
curwd = os.getcwd()
# Read file as dataframe
df = spark.read.schema(schema).csv(f"file:///{curwd}/datasets/7-temps-1800.csv")
print("printing schema: ")
df.printSchema()

# Filter out all but TMIN entries
minTemps = df.filter(df.measure_type == "TMIN")
# Select only stationID and temperature
stationTemps = df.select("stationID","temperature")
# Aggregate to find minimum temperature for every station
minTempsStation = df.groupBy("stationID").min("temperature") # col name will be "min(temperature)"
minTempsStation.show()

# Convert temperature to fahrenheit and sort the dataset
minTempsStationF = minTempsStation.withColumn("tempF", # with "min(temperature)" create "tempF"
                                                  func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0)\
                                                  .select("stationID", "tempF").sort("tempF")
print("printing F values with show")
minTempsStationF.show()
# Print results with collect()
results = minTempsStationF.collect()
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
spark.stop()

