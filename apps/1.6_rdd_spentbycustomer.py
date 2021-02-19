from pyspark import SparkConf, SparkContext
import collections
import os
import re # regular expressions 

def myMapper(data):
    data = data.split(",")
    custId = int(data[0])
    valueSpent = float(data[2])
    return (custId, valueSpent)


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower()) # break text based on words '\W+'

curwd = os.getcwd()
# SparkConf and SparkContext
conf = SparkConf().setMaster("local").setAppName("TMinTemps")
sc = SparkContext (conf = conf)
# read data
input = sc.textFile(f"file:///{curwd}/datasets/8-customer-orders.csv")
data = input.map(myMapper)
# sum values by key
myReducedData = data.reduceByKey(lambda x, y : x + y)
# now sort by result
mySortedData = myReducedData.map(lambda x : (x[1], x[0])).sortByKey()
results = mySortedData.collect()
for result in results:
    print(result)