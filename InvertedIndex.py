from pyspark import SparkContext

# Nooop
counter = 0


def count_line(line):
    global counter
    counter += 1
    return [counter, line]

sc = SparkContext("local", "Inverted Index")
sc.textFile("/user/datasets/books/dracula.txt") \
    .map(count_line) \
    .map(lambda line: "%d\t%s" % (line[0], line[1])) \
    .foreach() \
    .saveAsTextFile("/user/facundo.jauregui/spark-out/")
