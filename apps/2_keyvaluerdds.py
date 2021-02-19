from pyspark import SparkConf, SparkContext
import collections
import os
curwd = os.getcwd()
def parseData(data):
    fieldsList = data.split(',')
    age = int(fieldsList[2])
    numFriends = int(fieldsList[3])
    return (age, numFriends) # returning a key value pair in a tuple
def printResults(results):
    results = results.collect()
    for result in results:
        print (result)

# SparkConf and SparkContext (sc)
conf = SparkConf().setMaster("local").setAppName("countbykey")
sc = SparkContext (conf = conf)
print("log: reading data")
lines = sc.textFile(f"file:///{curwd}/datasets/idnameagefriends.csv")
print("original data in a rdd (sc.textFile): ")
printResults(lines)
rdd = lines.map(parseData)
print("first rdd - lines.map(fx): ")
printResults(rdd)
# mapping a "value" column with mapValues - (z, (x,1)) - our col is (x,1)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# result is (reduction):
# (33, (443, 1))
# (23, (953, 2)) # 953 = 4xx + 4xx, -- 2 = 1+1
# (51, (321, 1))
# (59, (123, 1))
# in reduceByKey it sums by keys: the keys are handled by the method itself
# and x, and y are the two rows that will be summed, we'll be adding same columns from different records
print("totalsByAge (rdd.mapValues(fx).reduceByKey(fx)): ")
printResults(totalsByAge)
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
print("averages (totalsByAge.mapValues(fx)): ")
printResults(averagesByAge)

