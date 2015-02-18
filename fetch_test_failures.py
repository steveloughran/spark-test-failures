#!/usr/bin/env python
#
# This file represents the main entry point of fetching test results from Jenkins.
#
# There are two separate reporting mechanisms: the first one populates a Google
# spreadsheet while thie script is running, and the other reports aggregate statistics
# to the console at the very end.
#
# The high level workflow is as follows. First, we initialize the spreadsheet and
# other necessary state using the configurations specified in test_settings.py.
# Then, we fetch test reports for each configured project, posting information for
# any test failures we encounter in the process to the spreadsheet. For each project,
# there are many builds, of which we only consider the ones that are recent enough
# (we determine this using a configurable threshold).
#
# The existing first-cut implementation is heavily tied to the use of Google
# spreadsheets. In the future, if we decide to use another mechanism of reporting,
# we may need to refactor the layout a little.

import time

from test_settings import *
from test_utils import *

# Suite name -> FailedTestInfo
failed_suites = { }

# Workflow: initialize() -> run() -> report()
def initialize():
    '''
    Initialize the spreadsheet by creating the appropriate
    worksheets and populating them with the appropriate headers.
    '''
    refresh_worksheets()
    # The parent worksheet only has aggregate statistics
    update_parent_cell("A1", "Suite name")
    update_parent_cell("B1", "Runs failed")
    for ws in all_worksheets():
        title = ws.title
        if title in SPARK_PROJECTS:
            # The project-specific worksheet has both aggregate statistics
            ws.update_acell("A1", "Suite name")
            ws.update_acell("B1", "Runs failed")
            # ... and information for individual test failures
            ws.update_acell("D1", "Suite name")
            ws.update_acell("E1", "Tests failed")
            ws.update_acell("F1", "Hadoop version")
            ws.update_acell("G1", "Date and time")
            project_worksheets[title] = ws
    refresh_gspread_client()

def run():
    '''
    Fetch test failures from each configured build project.
    '''
    for project in SPARK_PROJECTS:
        handle_project(project)

def report():
    '''
    Report on the console the aggregate statistics of all test failures encountered.
    Note that separately we have already been reporting to the Google spreadsheet.
    '''
    log_info("===== Test failures =====")
    failed_test_occurrences = sorted(failed_suites.items(), key=lambda x: -x[1].count())
    for (k, v) in failed_test_occurrences:
        log_info("%s: %s" % (k, v.count()))

def handle_project(project_name):
    '''
    Fetch and report failed tests for all filtered builds in the project.
    This assumes a highly specific JSON format exposed by Jenkins.
    '''
    log_debug("===== Fetching test results from project %s =====" % project_name)
    # e.g. https://amplab.cs.berkeley.edu/jenkins/job/Spark-1.3-SBT/api/json
    project_url = "%s/%s/%s" % (JENKINS_URL_BASE, project_name, JSON_URL_SUFFIX)
    project = fetch_json(project_url)
    builds = project["builds"]
    for build in builds:
        handle_build(build, project_name)

def handle_build(build, project_name):
    '''
    Fetch and report failed tests for all runs in the build,
    where each run is configured with a different hadoop profile.
    '''
    increase_indent()
    build_number = build["number"]
    # e.g. https://amplab.cs.berkeley.edu/jenkins/job/Spark-1.3-SBT/80/api/json
    build_url = "%s/%s/%s/%s" % (JENKINS_URL_BASE, project_name, build_number, JSON_URL_SUFFIX)
    # Refresh the gspread client every build to avoid HTTP exceptions
    refresh_gspread_client()
    build = fetch_json(build_url)
    if build and filter_build(build):
        build_date = build["id"]
        # Each build in the pull request builder only has one run, so use the build URL directly
        if is_pull_request_builder(project_name):
            run_url = "%s/%s/%s" % (JENKINS_URL_BASE, project_name, build_number)
            handle_run(run_url, build_date, project_name)
        else:
            for run in build["runs"]:
                # e.g. https://amplab.cs.berkeley.edu/jenkins/job/Spark-1.3-SBT/
                # AMPLAB_JENKINS_BUILD_PROFILE=hadoop1.0,label=centos/80/
                run_url = run["url"]
                if run_url.endswith("/"):
                    run_url = run_url[:-1]
                handle_run(run_url, build_date, project_name)
    decrease_indent()

def filter_build(build):
    '''
    Return true if we should fetch the test results of this build, and false otherwise.
    This filters out old builds based on a threshold configured by the user.
    '''
    timestamp = int(build["timestamp"]) / 1000 # s
    now = time.time()
    age = now - timestamp
    return age > 0 and age <= MAX_BUILD_AGE

def handle_run(run_url, date, project_name):
    '''
    Fetch and report failed tests for a particular run.
    '''
    increase_indent()
    log_debug("Handle run %s" % shorten(run_url))
    test_report_url = "%s/%s/%s" % (run_url, "testReport", JSON_URL_SUFFIX)
    test_report_url_short = test_report_url.replace(JENKINS_URL_BASE, "")
    test_report = fetch_json(test_report_url)
    if test_report:
        num_failed_suites = int(test_report["failCount"])
        if num_failed_suites > 0:
            s = "s" if num_failed_suites > 1 else ""
            log_info("Found %s failed suite%s" % (num_failed_suites, s))
            # Suite name -> number of occurrences in this run
            failed_suite_counts = { }
            for suite in test_report["suites"]:
                bad_statuses = ["FAILED", "REGRESSION"]
                bad_cases = [c for c in suite["cases"] if c["status"] in bad_statuses]
                bad_suite_names = [c["className"] for c in bad_cases]
                for suite_name in bad_suite_names:
                     if suite_name not in failed_suite_counts:
                         failed_suite_counts[suite_name] = 0
                     failed_suite_counts[suite_name] += 1
            for (suite_name, count) in failed_suite_counts.items():
                handle_suite(suite_name, count, run_url, date, project_name)
    decrease_indent()

def handle_suite(suite_name, num_failed_tests, url, date, project_name):
    '''
    Report a failed test on the spreadsheet.
    '''
    increase_indent()
    try:
        if num_failed_tests == 1:
            log_info(suite_name)
        else:
            log_info("%s (%s)" % (suite_name, num_failed_tests))
        if suite_name not in failed_suites:
            failed_suites[suite_name] = new_distinct_failed_suite(suite_name, project_name)
        test_info = failed_suites[suite_name]
        new_failed_suite(test_info, num_failed_tests, url, date, project_name)
    except Exception as e:
        log_error("Exception when handling suite %s: %s" % (suite_name, e.message))
    decrease_indent()

# Do the fetching!
initialize()
run()
report()

