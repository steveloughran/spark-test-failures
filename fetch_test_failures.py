#!/usr/bin/env python
#
# This script produces a CSV file where each line represents a failed instance
# of a test suite.
#
# For each Spark Jenkins project, there are many builds, of which we only consider
# the ones that are recent enough (we determine this using a configurable threshold).
# Then, within each build there are many runs, and within each run there can be many
# failed suites. For each failed suite, we log one line of information that describes
# this particular instance of the failure to the output file.

import time

from test_settings import *
from test_utils import *

# Workflow: initialize() -> run() -> report()

schema = ["Suite name", "Project name", "Tests failed",
  "Hadoop profile", "Hadoop version", "Timestamp", "URL"]

def initialize():
    '''
    Write the schema as the first line of the output file.
    '''
    write_output(OUTPUT_DELIMITER.join(schema))
    flush_output()

def run():
    '''
    Fetch test failures from each configured build project.
    '''
    for project in SPARK_PROJECTS:
        handle_project(project)

def report():
    log_info("===== See results in %s =====" % OUTPUT_FILE_NAME)
    close_output()

def handle_project(project_name):
    '''
    Fetch and report failed suites for all filtered builds in the project.
    This assumes a highly specific JSON format exposed by Jenkins.
    '''
    log_debug("===== Fetching test results from project %s =====" % project_name)
    # e.g. https://amplab.cs.berkeley.edu/jenkins/job/Spark-1.3-SBT/api/json
    project_url = "%s/%s/%s" % (JENKINS_URL_BASE, project_name, JSON_URL_SUFFIX)
    project = fetch_json(project_url)
    if project:
        builds = project["builds"]
        for build in builds:
            handle_build(build, project_name)

def handle_build(build, project_name):
    '''
    Fetch and report failed suites for all runs in the build, where
    each run is configured with a different hadoop profile.
    '''
    increase_indent()
    build_number = build["number"]
    # e.g. https://amplab.cs.berkeley.edu/jenkins/job/Spark-1.3-SBT/80/api/json
    build_url = "%s/%s/%s/%s" % (JENKINS_URL_BASE, project_name, build_number, JSON_URL_SUFFIX)
    # Refresh the gspread client every build to avoid HTTP exceptions
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
    Return true if we should process this build; false otherwise.
    This filters out old builds based on a threshold configured by the user.
    '''
    timestamp = int(build["timestamp"]) / 1000 # s
    now = time.time()
    age = now - timestamp
    return age > 0 and age <= MAX_BUILD_AGE

def handle_run(run_url, date, project_name):
    '''
    Fetch and report failed suites for a particular run.
    '''
    log_debug("Handle run %s" % shorten(run_url))
    increase_indent()
    test_report_url = "%s/%s/%s" % (run_url, "testReport", JSON_URL_SUFFIX)
    test_report_url_short = test_report_url.replace(JENKINS_URL_BASE, "")
    test_report = fetch_json(test_report_url)
    if test_report:
        num_failed_tests = int(test_report["failCount"])
        if num_failed_tests > 0:
            s = "s" if num_failed_tests > 1 else ""
            log_info("Found %s failed test%s" % (num_failed_tests, s))
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
    Report a failed suite by logging its information to the output file.
    '''
    increase_indent()
    if num_failed_tests == 1:
        log_info(suite_name)
    else:
        log_info("%s (%s)" % (suite_name, num_failed_tests))

    # Extract fields from parameters
    if is_pull_request_builder(project_name):
        hadoop_profile = "2.3.0"
    else:
        hadoop_profile = parse_hadoop_profile(url)
    hadoop_version = get_hadoop_version(hadoop_profile)
    timestamp = date_to_utc_timestamp(date)

    # Populate the output file with information about this failed suite
    row = [suite_name, project_name, num_failed_tests,
            hadoop_profile, hadoop_version, timestamp, url]
    row = [str(x).replace(OUTPUT_DELIMITER, "_") for x in row]
    row = OUTPUT_DELIMITER.join(row)
    write_output(row)
    decrease_indent()

# Do the fetching!
initialize()
run()
report()

