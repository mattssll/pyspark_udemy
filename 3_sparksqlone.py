from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create spark SparkSession
spark = SparkSession.builder.appName("sparksqlone").getOrCreate()

def mapper(line):
    fields = line.split(",") # since it's a csv
    # To be able to build the DataFrame, first we need "Rows"
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), 
            age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("file:////Users/mateus.leao/Documents/mattssll/spark/udemy-spark-frank/datasets/idnameagefriends.csv")

people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table
teenagers = spark.sql("SELECT * from people WHERE age >= 13 AND age <= 25")
# Results of SQL queries are RDDs
for teen in teenagers.collect():
    print(teen)

print("showing group by of dataframe/view")
schemaPeople.groupBy("age").count().orderBy("age").show()
spark.stop()