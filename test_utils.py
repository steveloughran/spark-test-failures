#!/usr/bin/env python

# ------------------------------------------------ #
#  Utility methods used by fetch_test_failures.py  #
# ------------------------------------------------ #

import gspread
import json
import time
from urllib2 import *

from test_settings import *

JENKINS_URL_BASE = "https://amplab.cs.berkeley.edu/jenkins/job"
JSON_URL_SUFFIX = "api/json"

# Google spreadsheet rows are not zero indexed, so the first index starts at 1
# However, the first row of every worksheet is the title, so we start at 2
FIRST_WRITABLE_CELL_INDEX = 2

# The next cell index (row number) to use on the parent spreadsheet when we encounter
# a test failure we haven't seen before. Because only aggregate statistics are reported
# on the parent spreadsheet, we only need to keep track of one index here.
_parent_next_cell_index = FIRST_WRITABLE_CELL_INDEX
def parent_next_cell_index():
    global _parent_next_cell_index
    index = _parent_next_cell_index
    _parent_next_cell_index += 1
    return index

# Project name -> next cell indices on the corresponding worksheet.
# On project worksheets we maintain both the aggregate statistics and a list of all
# test failures we have observed so far. In the latter case, we append to the list
# whether or not we have seen the test failure occur in this project before.
_project_next_cell_index = { }
_project_next_agg_cell_index = { }
def project_next_cell_index(project_name):
    if project_name not in _project_next_cell_index:
        _project_next_cell_index[project_name] = FIRST_WRITABLE_CELL_INDEX
    index = _project_next_cell_index[project_name]
    _project_next_cell_index[project_name] += 1
    return index
def project_next_agg_cell_index(project_name):
    if project_name not in _project_next_agg_cell_index:
        _project_next_agg_cell_index[project_name] = FIRST_WRITABLE_CELL_INDEX
    index = _project_next_agg_cell_index[project_name]
    _project_next_agg_cell_index[project_name] += 1
    return index

# Project name -> Google worksheet instance
project_worksheets = { }

# Client for populating the configured Google spreadsheet
gc = None
def refresh_gspread_client():
    global gc
    gc = gspread.login(SPREADSHEET_LOGIN, SPREADSHEET_PASSWORD)
    gc = gc.open(SPREADSHEET_TITLE)

# In this script, each project in SPARK_PROJECTS is represented in its own
# worksheet within the configured spreadsheet. To avoid conflicts from a
# previous invocation of this script, we must refresh all relevant worksheets.
def refresh_worksheets():
    '''
    Delete all existing worksheets in the spreadsheet and add a new one for
    the parent and for each configured project. This must be called before we
    begin to populate the spreadsheet.
    '''
    # Delete all worksheets that existed previously.
    # Note that we need to create a dummy worksheet because Google spreadsheet
    # requires us to keep around at least one worksheet in the process.
    refresh_gspread_client()
    dummy_title = "not-used-%d" % int(round(time.time()))
    add_worksheet(dummy_title, quiet = True)
    delete_worksheets(lambda ws: ws.title != dummy_title)
    # Create a new worksheet for the parent and for each project configured
    add_worksheet("main")
    for project_name in SPARK_PROJECTS:
        add_worksheet(project_name)
    # Delete the dummy worksheet we created in the beginning
    delete_worksheets(lambda ws: ws.title == dummy_title, quiet = True)

def add_worksheet(title, quiet = False):
    '''
    Add a new worksheet to the configured spreadsheet.
    '''
    if not quiet:
        log_info("Adding new worksheet %s..." % title)
    gc.add_worksheet(title, rows = 1000, cols = 10)

def delete_worksheets(filter_function, quiet = False):
    '''
    Delete all matching worksheets from the configured spreadsheet.
    '''
    refresh_gspread_client()
    for ws in gc.worksheets():
        if filter_function(ws):
            if not quiet:
                log_info("Deleting existing worksheet %s..." % ws.title)
            gc.del_worksheet(ws)
            # We must use a new client because the old one becomes unusable
            # after deleting a worksheet. This is a limitation in gspread that
            # unnecessarily complicates the code here.
            delete_worksheets(filter_function)
            break

def update_parent_cell(cell, value):
    '''
    Update a cell in the parent worksheet.
    This assumes that the parent worksheet is always the first one.
    '''
    gc.sheet1.update_acell(cell, value)

def all_worksheets():
    return gc.worksheets()

# When we encounter a test we have never seen before, we assign it a cell index in the
# parent worksheet. Similarly, when we encounter a test we have never seen before in a
# particular project, we assign it a cell index in the corresponding project worksheet.
# To find the right cells in each worksheet to update, we keep track of the cell indices
# we assign to each failed test in the following class. 
class FailedTestInfo:
    '''
    Simple class for storing information about failed tests.

    More specifically, this keeps track of the cell indices specific to this test
    suite in all relevant worksheets, and the number of occurrences in each project.
    The suite name acts as the unique identifier for each instance of this class.
    '''
    def __init__(self, suite_name, parent_cell_index):
        self.suite_name = suite_name
        self.parent_cell_index = parent_cell_index
        self.project_cell_indices = { } # project name -> cell index
        self.project_counts = { } # project name -> occurrences

    def in_project(self, project_name):
        '''
        Return true if this test has failed in the specified project, false otherwise.
        '''
        return project_name in self.project_cell_indices

    def count(self):
        '''
        Return the total number of occurrences of this failed test across all projects.
        '''
        return sum(self.project_counts.values())

def new_distinct_failed_suite(suite_name, project_name):
    '''
    Callback invoked when we encounter a failed test we have never seen before.
    This updates the parent worksheet and returns the relevant FailedTestInfo object.
    '''
    parent_cell_index = parent_next_cell_index()
    update_parent_cell("A%d" % parent_cell_index, suite_name)
    update_parent_cell("B%d" % parent_cell_index, 1)
    return FailedTestInfo(suite_name, parent_cell_index)

def new_failed_suite(test_info, num_failed_tests, url, date, project_name):
    '''
    Callback invoked when we encounter a failed suite.
    
    This updates the aggregate statistics on both the parent worksheet and the
    child worksheet that corresponds to the project. In the latter case, we also
    report information that is specific to this instance of the failed test.
    '''
    if not test_info.in_project(project_name):
        # This is the first time this suite failed in this project so
        # we assign it the next cell index in the relevant worksheet
        test_info.project_cell_indices[project_name] =\
            project_next_agg_cell_index(project_name)
        test_info.project_counts[project_name] = 0
    test_info.project_counts[project_name] += 1
    # Update parent agg count
    update_parent_cell("B%d" % test_info.parent_cell_index, test_info.count())
    # Update project-specific worksheet
    suite_name = test_info.suite_name
    project_cell_index = project_next_cell_index(project_name)
    project_agg_cell_index = test_info.project_cell_indices[project_name]
    project_count = test_info.project_counts[project_name]
    project_ws = project_worksheets[project_name]
    project_ws.update_acell("A%d" % project_agg_cell_index, suite_name)
    project_ws.update_acell("B%d" % project_agg_cell_index, project_count)
    project_ws.update_acell("D%d" % project_cell_index,\
        "=HYPERLINK(\"%s\"; \"%s\")" % (url, suite_name))
    project_ws.update_acell("E%d" % project_cell_index, num_failed_tests)
    project_ws.update_acell("F%d" % project_cell_index, parse_hadoop(url))
    project_ws.update_acell("G%d" % project_cell_index, date)

# ---------------------- #
#  Other helper methods  #
# ---------------------- #

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

def parse_hadoop(url):
    split_keys = ["AMPLAB_JENKINS_BUILD_PROFILE", "hadoop.version", "HADOOP_PROFILE"]
    for split_key in split_keys:
        if split_key in url:
            return url.split("%s=" % split_key)[1].split(",")[0]
    return "N/A"

