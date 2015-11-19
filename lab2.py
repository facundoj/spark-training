import re
import datetime

from pyspark import SparkContext


def parse_apache_time(time_string):
    # 01/Aug/1995:00:00:01 -0400
    # todo: implement
    res = re.search(r'(\d{2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) (\S{5})', time_string)

    if not res:
        return

    return datetime.datetime(
        int(res.group(3)),
        1,
        int(res.group(1)),
        int(res.group(4)),
        int(res.group(5)),
        int(res.group(6))
    )


class LogLine(object):
    def __init__(self, host, availability, userid, time, method, resource, protocol, status_code, size):
        self.host = host
        self.client_ident = availability
        self.user_id = userid
        self.time = parse_apache_time(time)
        self.method = method
        self.resource = resource
        self.protocol = protocol
        self.status_code = status_code
        self.size = size

# http://httpd.apache.org/docs/1.3/logs.html#common
# LogFormat "%h %l %u %t \"%r\" %>s %b" common
LOG_PATTERN = r'(\S+) (\S+) (\S+) \[(.+)\] "(\S+) (\S+) (\S+)" (\d+) (\d+)'


def parseLogLine(line):
    res = re.search(LOG_PATTERN, line)

    if not res:
        return

    info = []
    for i in range(1, 10):
        info.append(res.group(i))

    return LogLine(*info)

l1 = '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245'
l2 = 'unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985'
l3 = '199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085'

print parseLogLine(l1).time
print parseLogLine(l2).time

# sc = SparkContext('local', 'Lab 2')

