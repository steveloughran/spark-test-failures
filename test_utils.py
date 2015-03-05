#!/usr/bin/env python

# ------------------------------------------------ #
#  Utility methods used by fetch_test_failures.py  #
# ------------------------------------------------ #

import datetime
import gspread
import json
import re
import time
from urllib2 import *

from test_settings import *

JENKINS_URL_BASE = "https://amplab.cs.berkeley.edu/jenkins/job"
JSON_URL_SUFFIX = "api/json"

OUTPUT_FILE_NAME = "output.csv"
OUTPUT_FLUSH_INTERVAL = 1
OUTPUT_DELIMITER = ";"

_output_writer = open(OUTPUT_FILE_NAME, "w")
_output_lines_written = 0
def write_output(line):
    global _output_writer
    global _output_lines_written
    _output_writer.write(line + "\n")
    _output_lines_written += 1
    if _output_lines_written % OUTPUT_FLUSH_INTERVAL == 0:
        _output_writer.flush()

def flush_output():
    _output_writer.flush()

def close_output():
    _output_writer.close()

_indent = ""
def increase_indent():
    global _indent
    _indent = "    %s" % _indent
def decrease_indent():
    global _indent
    _indent = _indent[4:]

def log_debug(msg):
    if DEBUG:
        print _indent + msg

def log_info(msg):
    print _indent + msg

def log_warning(msg):
    print "%s:*** WARNING: %s" % (_indent, msg)

def log_error(msg):
    print "%s!!! ERROR: %s" % (_indent, msg)

def shorten(url):
    return url.replace(JENKINS_URL_BASE, "")

def fetch_json(url):
    '''
    Fetch a JSON from the specified URL and return the JSON as string.
    If an exception occurs in the process, return None.
    '''
    try:
        raw_content = urlopen(url).read()
        return json.loads(raw_content, strict = False)
    except HTTPError as he:
        if he.code == 404:
            # 404's are very common, e.g. if the build doesn't even compile
            # In this case it's not really an error, so we should tone down
            # the severity of the log message
            log_info("Unable to find JSON at %s" % shorten(url))
        else:
            he_msg = _indent + str(he)
            log_error("Encountered HTTP error when fetching JSON from %s:\n%s"\
                % (shorten(url), he_msg))
    except Exception as e:
        e_msg = _indent + str(e)
        log_error("Failed to fetch JSON from %s:\n%s" % (shorten(url), e_msg))
    return None

def is_pull_request_builder(project_name):
    return "pullrequestbuilder" in project_name.lower()

def date_to_utc_timestamp(date_time):
    '''
    Convert the date time string using the format "yy-mm-dd_hh-mm-ss"
    to a UTC timestamp in seconds.
    '''
    # e.g. 2015-02-18_04-26-29 -> 1424262389
    (my_date, my_time) = date_time.split("_")
    (year, month, day) = [int(x) for x in my_date.split("-")]
    (hour, minute, second) = [int(x) for x in my_time.split("-")]
    dt = datetime.datetime(year, month, day, hour, minute, second)
    return long(time.mktime(dt.timetuple()))

def parse_hadoop_profile(url):
    split_keys = ["AMPLAB_JENKINS_BUILD_PROFILE", "hadoop.version", "HADOOP_PROFILE"]
    for split_key in split_keys:
        if split_key in url:
            return url.split("%s=" % split_key)[1].split(",")[0]
    return "N/A"

def get_hadoop_version(hadoop_profile):
    if re.match("hadoop[-]*1\.0", hadoop_profile): return "1.0.4"
    if re.match("hadoop[-]*2\.0", hadoop_profile): return "2.0.0"
    if re.match("hadoop[-]*2\.2", hadoop_profile): return "2.2.0"
    if re.match("hadoop[-]*2\.3", hadoop_profile): return "2.3.0"
    if re.match("hadoop[-]*2\.4", hadoop_profile): return "2.4.0"
    # e.g. 1.0.4 -> 1.0.4
    # e.g. 2.0.0-mr1-cdh4.1.2 -> 2.0.0
    match = re.search("1\.[0-9]\.[0-9]", hadoop_profile)
    if match: return match.group(0)
    match = re.search("2\.[0-9]\.[0-9]", hadoop_profile)
    if match: return match.group(0)
    return hadoop_profile

