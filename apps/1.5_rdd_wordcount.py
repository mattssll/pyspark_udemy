from pyspark import SparkConf, SparkContext
import collections
import os
import re # regular expressions 

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower()) # break text based on words '\W+'

curwd = os.getcwd()
# SparkConf and SparkContext
conf = SparkConf().setMaster("local").setAppName("TMinTemps")
sc = SparkContext (conf = conf)
# read data
input = sc.textFile(f"file:///{curwd}/datasets/6-book.txt")
# v1
words = input.flatMap(lambda x: x.split()) # it's splitting each line into multiple lines (one row per word)
wordCounts = words.countByValue()
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord, count)
# v2
words2 = input.flatMap(normalizeWords)
wordCounts2 = words2.countByValue()
for word, count in wordCounts2.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord, count)

# v3, sorted results - implementing in a lower level
input = sc.textFile(f"file:///{curwd}/datasets/6-book.txt")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y) # it will reduce (merge) the keys, and sum values
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
