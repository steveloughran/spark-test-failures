package com.databricks.reporter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

import com.databricks.util.FailedSuite

/**
 * An abstract class that reports Spark test failures from a local file.
 */
private[reporter] abstract class AbstractReporter(
    allJenkinsProjects: Seq[String],
    outputFileName: String,
    outputFileDelimiter: String) {

  private val tableName = "failedSuites"
  private val sqlContext = initialize()

  /**
   * Return the number of occurrences of each failed suite aggregated across all projects.
   * The returned list contains 2-tuples (suiteName, count) in descending order.
   */
  def getAggregateCounts: Seq[(String, Long)] = {
    sqlContext.sql("SELECT suiteName, COUNT(*) AS cnt FROM " +
        s"$tableName GROUP BY suiteName ORDER BY cnt DESC")
      .collect()
      .map { row => (row.getString(0), row.getLong(1)) }
  }

  /**
   * Return the number of occurrences of each failed suite aggregated across a project.
   * The returned list contains 2-tuples (suiteName, count) in descending order.
   */
  def getAggregateCountsByProject(projectName: String): Seq[(String, Long)] = {
    sqlContext.sql(s"SELECT suiteName, COUNT(*) AS cnt FROM $tableName WHERE " +
        s"projectName='$projectName' GROUP BY suiteName ORDER BY cnt DESC")
      .collect()
      .map { row => (row.getString(0), row.getLong(1)) }
  }

  /** Return all failed suites across all projects. */
  def getAllRows: Seq[FailedSuite] = {
    sqlContext.sql(s"SELECT * FROM $tableName SORT BY time DESC")
      .collect()
      .map(FailedSuite.fromCompleteRow)
  }

  /** Returned all failed suites in the given project. */
  def getRowsByProject(projectName: String): Seq[FailedSuite] = {
    sqlContext.sql(s"SELECT * FROM $tableName WHERE " +
        s"projectName='$projectName' SORT BY time DESC")
      .collect()
      .map(FailedSuite.fromCompleteRow)
  }

  /** Return the number of failed suite instances. */
  def aggregateCount(): Long = {
    sqlContext.sql(s"SELECT suiteName FROM $tableName").count()
  }

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
    val delimiter = outputFileDelimiter
    val failedSuitesRDD = sc.textFile(outputFileName)
      .filter { line => !line.startsWith(FailedSuite.schema.head) }
      .map { line => line.split(delimiter) }
      .map { items =>
        FailedSuite(items(0), items(1), items(2).toInt,
          items(3), items(4), items(5).toLong, items(6))
      }
    val failedSuitesDF = failedSuitesRDD.toDF()
    failedSuitesDF.registerTempTable(tableName)
    sqlContext
  }

}

