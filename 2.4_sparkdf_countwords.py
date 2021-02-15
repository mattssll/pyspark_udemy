from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import os
spark = SparkSession.builder.appName("WordCount").getOrCreate()
# Read each line of book into a dataframe
curwd = os.getcwd()
inputDF = spark.read.text(f"file:///{curwd}/datasets/6-book.txt")

# Split using a regular expression that extract words - explode will make each word a row
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")
# Normalize everything to lowercase
lowerCaseWords = words.select(func.lower(words.word).alias("word"))
# Count up occurrences of each word
groupByWords = lowerCaseWords.groupBy("word").count()
# Sort by Counts
wordCountSorted = groupByWords.sort("count")
# Show results
wordCountSorted.show(wordCountSorted.count()) # the internal parameter is to show the number of rows in DF