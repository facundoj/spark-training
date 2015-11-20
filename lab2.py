import re
import datetime
import matplotlib.pylab as pylab
import matplotlib.pyplot as plt
import numpy as np

from pyspark import SparkContext
from pyspark.sql.types import Row


# Parses string to Pythpn datetime
def parse_apache_time(time_string):
    # 01/Aug/1995:00:00:01 -0400
    datetime_pattern = r'(\d{2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) (\S{5})'
    months_map = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                  'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}

    res = re.search(datetime_pattern, time_string)

    if not res:
        return

    return datetime.datetime(
        int(res.group(3)),
        months_map[res.group(2)],
        int(res.group(1)),
        int(res.group(4)),
        int(res.group(5)),
        int(res.group(6))
    )


# Parses log line to Row object representing the line
def parse_log_line(line):
    # http://httpd.apache.org/docs/1.3/logs.html#common
    # LogFormat "%h %l %u %t \"%r\" %>s %b" common
    log_pattern = r'(\S+) (\S+) (\S+) \[(.+)\] "([A-Z]*) *(\S+) *(.*)" (\d+) (\S+)'

    res = re.search(log_pattern, line)

    if not res:
        return line, 0

    info = []
    for i in range(1, 9):
        info.append(res.group(i))

    size = res.group(9)
    if size == '-':
        size = 0
    else:
        size = float(size)

    return (LogLine(res.group(1), res.group(2), res.group(3), parse_apache_time(res.group(4)),
                   res.group(5), res.group(6), res.group(7), res.group(8), size), 1)


LogLine = Row('host', 'remote_identity', 'local_identity', 'time',
              'method', 'endpoint', 'protocol', 'status_code', 'size')

# Exercises *********************************************************************************************
sc = SparkContext('local', 'Lab 2')

# Raw logs
inputRDD = sc.textFile('/home/facundo.jauregui/PycharmProjects/SparkTest/resources/access_log_Jul95')

# Parsed (LogLine, b); b = 0|1, LogLine = Row
parsedInputRDD = inputRDD.map(parse_log_line)

# Removed log lines that don't match pattern
cleanedInputRDD = parsedInputRDD.filter(lambda x: x[1] == 1).persist()

# # (2a) ******************************
# sizesRDD = cleanedInputRDD.map(lambda t: t[0].size).cache()
# total = sizesRDD.reduce(lambda x, y: x + y)
# count = sizesRDD.count()
#
# print 'Min: %f' % sizesRDD.min()
# print 'Max: %f' % sizesRDD.max()
# print 'Avg: %f' % (total / float(count))
#
# sizesRDD.unpersist()

# # (2b) ******************************
# statusCountRDD = cleanedInputRDD \
#     .map(lambda t: (t[0].status_code, 1)) \
#     .reduceByKey(lambda x, y: x + y)
#
# requestCount = float(cleanedInputRDD.count())
#
# codes = []
# fractions = []
# for status in statusCountRDD.collect():
#     codes.append(status[0])
#     fractions.append(status[1] / requestCount)
#     print '%s: %d (%f%%)\n' % (status[0], status[1], status[1] / requestCount * 100)
#
# pylab.figure(1, figsize=(6, 6))
# pylab.pie(fractions, labels=codes)
#
# pylab.show()

# # (2d) ******************************
# print cleanedInputRDD \
#     .map(lambda (line, _): (line.host, 1)) \
#     .reduceByKey(lambda x, y: x + y) \
#     .filter(lambda (host, times): times > 10) \
#     .map(lambda (host, _): host) \
#     .take(20)

# (2e) ******************************
# Create RDD with visits count per endpoint
endpointVisitsRDD = cleanedInputRDD \
    .map(lambda (log, _): (log.endpoint.encode(encoding='utf-8'), 1)) \
    .reduceByKey(lambda x, y: x + y) \

# plt.figure()

# Collecting data to plot
endpointVisits = endpointVisitsRDD.collect()
allEndpoints = []
allVisits = []
for (endpoint, visits) in endpointVisits:
    allEndpoints.append(endpoint)
    allVisits.append(visits)
    # plt.bar(endpoint, visits)
    print '%s\t%d' % (endpoint, visits)

# plt.show()

