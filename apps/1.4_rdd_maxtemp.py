from pyspark import SparkConf, SparkContext
import collections
import os
curwd = os.getcwd()
# ITE00100554,18000101,TMAX,-75,,,E,
def myMapper(data):
    data = data.split(",")
    ids = data[0]
    maxornot = data[2]
    temp = data[3]
    return (ids, maxornot, temp)
def printResults(results):
    results = results.collect()
    for result in results:
        print (result[0], result[1])

# SparkConf and SparkContext
conf = SparkConf().setMaster("local").setAppName("TMinTemps")
sc = SparkContext (conf = conf)
# read data
lines = sc.textFile(f"file:///{curwd}/datasets/7-temps-1800.csv")
data = lines.map(myMapper)
maxTemps = data.filter(lambda x : "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
print("station temps: ")
printResults(stationTemps)
print("station temps reducedbykey: ")
printResults(maxTemps)



