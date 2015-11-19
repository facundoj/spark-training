from pyspark import SparkContext

import re


def parse_apache_time(time):
    # todo: implement
    return time


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
    else:
        info = []
        for i in range(1, 10):
            info.append(res.group(i))

        return LogLine(*info)

sc = SparkContext('local', 'Lab 2')

