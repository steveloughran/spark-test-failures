#!/usr/bin/env python

# ----------------------------------------- #
#  Settings used by fetch_test_failures.py  #
# ----------------------------------------- #

# List of projects to fetch failed test results from
SPARK_PROJECTS = []
SPARK_PROJECTS += ["SparkPullRequestBuilder"]
SPARK_PROJECTS += ["Spark-Master-Maven-with-YARN"]
SPARK_PROJECTS += ["Spark-Master-SBT"]
SPARK_PROJECTS += ["Spark-1.3-SBT"]
SPARK_PROJECTS += ["Spark-1.3-Maven-pre-YARN"]
SPARK_PROJECTS += ["Spark-1.3-Maven-with-YARN"]
SPARK_PROJECTS += ["Spark-1.2-SBT"]
SPARK_PROJECTS += ["Spark-1.2-Maven-pre-YARN"]
SPARK_PROJECTS += ["Spark-1.2-Maven-with-YARN"]
SPARK_PROJECTS += ["Spark-1.1-SBT"]
SPARK_PROJECTS += ["Spark-1.1-Maven-pre-YARN"]
SPARK_PROJECTS += ["Spark-1.1-Maven-with-YARN"]
SPARK_PROJECTS += ["Spark-1.0-SBT"]
SPARK_PROJECTS += ["Spark-1.0-Maven-pre-YARN"]
SPARK_PROJECTS += ["Spark-1.0-Maven-with-YARN"]

# Whether to enable debug logging
DEBUG = True

# Google spreadsheet settings
SPREADSHEET_LOGIN = "testingtester1700@gmail.com"
SPREADSHEET_PASSWORD = "tester1700"
SPREADSHEET_TITLE = "Spark Test Failures"

# How far back to go before a build is considered too old (seconds)
MAX_BUILD_AGE = 3600 * 24 * 7

