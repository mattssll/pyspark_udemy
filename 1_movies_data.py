
from pyspark import SparkConf, SparkContext
import collections
# SparkConf and SparkContext
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext (conf = conf)
# read data
lines = sc.textFile("file:////Users/mateus.leao/Documents/mattssll/spark/udemy-spark-frank/datasets/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2]) # 2 is making reference for the column with grade
result = ratings.countByValue() # returns a tuple (pair value of key and aggregation)

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print(f"rating: {key}, value: {value}")