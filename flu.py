from pyspark import SparkContext, Row
import re
import datetime

sc = SparkContext('local', 'Flu in Arg')

FluRecord = Row('week_start', 'arg', 'bsas', 'caba', 'mdza', 'stafe', 'cba')

monthNames = sc.broadcast(['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dev'])

def parseLine(line):
    LINE_PATTERN = r'^(\d{4})-(\d{2})-(\d{2}),(\d*),(\d*),(\d*),(\d*),(\d*),(\d*)'

    result = re.search(LINE_PATTERN, line)

    if result is None:
        return (line, -1)

    time = datetime.datetime(int(result.group(1)), int(result.group(2)), int(result.group(3)))

    return (FluRecord(time, result.group(4), result.group(5), result.group(6),
                      result.group(7), result.group(8), result.group(9)), 1)


rawRDD = sc.textFile('/home/facundo.jauregui/PycharmProjects/SparkTest/resources/flu-ar-data.csv')
# (FluRecordRow)
validRecordsRDD = rawRDD \
    .map(parseLine) \
    .filter(lambda (record, code): code == 1) \
    .map(lambda (record, code): record)

# Avg flue per month - Arg
argRecordsPerMonth = validRecordsRDD \
    .map(lambda record: (record.week_start.month, record.arg))

print argRecordsPerMonth \
    .filter(lambda record: bool(record)) \
    .mapValues(lambda record: (int(record), 1)) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .mapValues(lambda (total, count): float(total) / count) \
    .sortBy(lambda (m, fluAvg): -fluAvg) \
    .map(lambda (m, flu): (monthNames.value[m], flu)) \
    .collectAsMap()
