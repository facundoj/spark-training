from pyspark import SparkContext, SparkConf

import sys

conf = SparkConf().setMaster("local").setAppName("Word Count")
sc = SparkContext(conf=conf)

if len(sys.argv) < 2:
    print("Usage: wordcount <file>")
    exit(-1)

words = sc.textFile(sys.argv[1]) \
    .flatMap(lambda line: line.encode("utf-8", "ignore").split(" ")) \
    .map(lambda word: [word, 1]) \
    .reduceByKey(lambda c1, c2: c1 + c2) \
    .collect()

for (word, count) in words:
    print("%s:\t%i" % (word, count))

sc.stop()
# /home/facundo.jauregui/PycharmProjects/SparkTest/resources/file1.txt

words = sc.textFile("/home/facundo.jauregui/PycharmProjects/SparkTest/resources/file1.txt") \
    .flatMap(lambda line: line.encode("utf-8", "ignore").split(" ")) \
    .map(lambda word: [word, 1]) \
    .reduceByKey(lambda c1, c2: c1 + c2)