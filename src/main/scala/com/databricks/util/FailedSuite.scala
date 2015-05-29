package com.databricks.util

import org.apache.spark.sql.Row

/**
 * A simple struct for keeping track of information about
 * a particular failure instance of a test suite.
 */
case class FailedSuite(
    suiteName: String,
    projectName: String,
    numTestsFailed: Int,
    hadoopProfile: String,
    hadoopVersion: String,
    timestamp: Long,
    url: String)

object FailedSuite {

  // Human readable schema that describes the fields of a FailedSuite
  val schema = Seq("Suite name", "Project name", "Tests failed",
    "Hadoop profile", "Hadoop version", "Timestamp", "URL")

  /**
   * Return a FailedSuite from a complete SQL row that contains
   * all the fields in the original row in the output.
   */
  def fromCompleteRow(row: Row): FailedSuite = {
    FailedSuite(
      row.getString(0), // suiteName
      row.getString(1), // projectName
      row.getInt(2),    // numTestsFailed
      row.getString(3), // hadoopProfile
      row.getString(4), // hadoopVersion
      row.getLong(5),   // timestamp
      row.getString(6)) // url
 }
}

