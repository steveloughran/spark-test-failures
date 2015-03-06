package com.databricks.reporter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
 * An abstract class that reports Spark test failures from a local file.
 */
private[reporter] abstract class TestFailureReporter {
  import TestFailureReporter._

  private val tableName = "failedSuites"
  private val sqlContext = initialize()

  /**
   * Register failed test information in a Spark SQL table.
   * Return the SQLContext that can be used to issue queries against this table.
   */
  private def initialize(): SQLContext = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Best app ever")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Set up failed suites table
    val failedSuitesRDD = sc.textFile(OUTPUT_FILE_NAME)
      .filter { line => !line.startsWith("Suite name") }
      .map { line => line.split(OUTPUT_DELIMITER) }
      .map { items =>
        FailedSuite(items(0), items(1), items(2).toInt,
          items(3), items(4), items(5).toLong, items(6))
      }
    val failedSuitesDF = failedSuitesRDD.toDF()
    failedSuitesDF.registerTempTable(tableName)
    sqlContext
  }

  /** Return the number of failed suite instances. */
  def aggregateCount(): Long = {
    sqlContext.sql(s"SELECT suiteName FROM $tableName").count()
  }
}

/**
 * A simple struct for keeping track of information about
 * a particular failure instance of a test suite.
 */
private case class FailedSuite(
    suiteName: String,
    projectName: String,
    numTestsFailed: Int,
    hadoopProfile: String,
    hadoopVersion: String,
    timestamp: Long,
    url: String)

private object TestFailureReporter {
  private val OUTPUT_FILE_NAME = "output.csv"
  private val OUTPUT_DELIMITER = ";"
}

