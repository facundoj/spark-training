from pyspark import SparkContext

sc = SparkContext('local', 'Lab 2')


# 199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
# unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985
# 199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085

LOG_PATTERN = r'(\S+) (\S+) (\S+) \[(.+)\] "(\S+) (\S+) (\S+)" (\d+) (\d+)'
