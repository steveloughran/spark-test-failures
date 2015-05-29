package com.databricks.fetcher

import java.io.{FileNotFoundException, FileWriter}
import java.net.URL

import scala.collection.JavaConversions._
import scala.collection.mutable

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.commons.io.IOUtils

import com.databricks.util.{FailedSuite, PropertiesReader}

/**
 * A class that fetches test reports from Jenkins and logs test failure
 * information to a CSV output file. Each line in output file represents
 * a single instance of a failed suite.
 */
class JenkinsFetcher(
    val allJenkinsProjects: Seq[String],
    val outputFileName: String,
    val outputFileDelimiter: String,
    maxTestAgeSeconds: Long) {
  import JenkinsFetcher._

  private val jsonMapper = new ObjectMapper
  private val writer = new FileWriter(outputFileName)
  private var indent = ""
  private val maxTestAgeMillis = Option(maxTestAgeSeconds * 1000)
    .filter(_ >= 0)
    .getOrElse(Long.MaxValue)

  log(s"Fetching Jenkins test reports from ${JENKINS_URL_BASE}/[...]")
  log(s"Logging test failure information to ${outputFileName}\n")

  // Write the header
  writer.write(FailedSuite.schema.mkString(outputFileDelimiter) + "\n")
  writer.flush()

  /**
   * Fetch test reports for each of the configured Jenkins projects
   * and log information about any test failures to the output file.
   */
  def run(): Unit = {
    allJenkinsProjects.foreach { projectName =>
      val projectUrl = s"${JENKINS_URL_BASE}/$projectName"
      handleProject(projectUrl, projectName)
    }
  }

  /** Stop the fetcher and close any resources used in the process. */
  def stop(): Unit = {
    writer.close()
  }

  // e.g. https://amplab.cs.berkeley.edu/jenkins/job/SparkPullRequestBuilder
  // e.g. https://amplab.cs.berkeley.edu/jenkins/job/Spark-Master-SBT
  // e.g. https://amplab.cs.berkeley.edu/jenkins/job/Spark-Master-Maven-pre-YARN/
  private def handleProject(projectUrl: String, projectName: String): Unit = {
    log(s"========== Fetching test reports for project $projectName ==========")
    val projectJson = fetchJson(projectUrl).getOrElse { return }
    val firstBuild = projectJson.get("firstBuild").get("number").asInt
    val lastBuild = projectJson.get("lastBuild").get("number").asInt
    assert(lastBuild >= firstBuild)
    (firstBuild to lastBuild).reverse.foreach { buildNumber =>
      val buildUrl = s"${projectUrl}/$buildNumber"
      handleBuild(buildUrl, projectName)
    }
  }

  // e.g. .../SparkPullRequestBuilder/28354
  // e.g. .../Spark-Master-SBT/1847
  // e.g. .../Spark-Master-Maven-pre-YARN/1760
  private def handleBuild(buildUrl: String, projectName: String): Unit = {
    val buildJson = fetchJson(buildUrl).getOrElse { return }
    val buildTimestamp = buildJson.get("timestamp").asLong
    val buildTooOld = System.currentTimeMillis - buildTimestamp > maxTestAgeMillis
    if (!buildTooOld) {
      // Each build in the pull request builder only has one run, so use the build URL directly
      if (isPullRequestBuilder(projectName)) {
        handleRun(buildUrl, projectName)
      } else {
        val runsJson = buildJson.get("runs")
        runsJson.iterator.foreach { runJson =>
          val runUrl = runJson.get("url").asText.stripSuffix("/")
          handleRun(runUrl, projectName)
        }
      }
    }
  }

  // e.g. .../SparkPullRequestBuilder/28354
  // e.g. .../Spark-Master-SBT/1847/AMPLAB_JENKINS_BUILD_PROFILE=hadoop1.0,label=centos/
  // e.g. .../Spark-Master-Maven-pre-YARN/1760/hadoop.version=1.0.4,label=centos/
  private def handleRun(runUrl: String, projectName: String): Unit = wrapForIndentation {
    log(s"Handle run ${shorten(runUrl)}")
    val runJson = fetchJson(runUrl).getOrElse { return }
    val runTimestamp = runJson.get("timestamp").asLong
    val testReportUrl = s"$runUrl/testReport"
    val testReportJson = fetchJson(testReportUrl).getOrElse { return }
    val numTestsFailed = testReportJson.get("failCount").asInt
    if (numTestsFailed > 0) {
      wrapForIndentation {
        log(s"Found $numTestsFailed failed test(s)")
        // Suite name -> number of failed occurrences in this run
        val failedSuiteCounts = new mutable.HashMap[String, Int]
        val failedSuiteStatuses = Set("FAILED", "REGRESSION")
        val suitesJson = testReportJson.get("suites")
        suitesJson.iterator.foreach { suiteJson =>
          val failedSuiteNames = suiteJson.get("cases").iterator
            .filter { caseJson => failedSuiteStatuses.contains(caseJson.get("status").asText) }
            .map { _.get("className").asText }
            .filter { _.startsWith("org.apache.spark") }
          failedSuiteNames.foreach { suiteName =>
            if (!failedSuiteCounts.contains(suiteName)) {
              failedSuiteCounts(suiteName) = 0
            }
            failedSuiteCounts(suiteName) += 1
          }
        }
        failedSuiteCounts.foreach { case (suiteName, numTestsFailed) =>
          handleSuite(suiteName, numTestsFailed, runUrl, runTimestamp, projectName)
        }
        if (failedSuiteCounts.isEmpty) {
          log("Warning: No failed suites beginning with org.apache.spark.* found.")
        }
      }
    }
  }

  /** Log one line that describes a particular failed suite instance to the output file. */
  private def handleSuite(
      suiteName: String,
      numTestsFailed: Int,
      runUrl: String,
      runTimestamp: Long,
      projectName: String): Unit = wrapForIndentation {
    if (numTestsFailed == 1) {
      log(suiteName)
    } else {
      log(s"$suiteName ($numTestsFailed)")
    }

    val hadoopProfile =
      if (isPullRequestBuilder(projectName)) {
        PULL_REQUEST_BUILDER_HADOOP_PROFILE
      } else {
        parseHadoopProfile(runUrl)
      }
    val hadoopVersion = getHadoopVersion(hadoopProfile)
    val row = Seq(suiteName, projectName, numTestsFailed,
      hadoopProfile, hadoopVersion, runTimestamp, runUrl)
        .map { _.toString.replaceAll(outputFileDelimiter, "_") }
        .mkString(outputFileDelimiter)
    writer.write(row + "\n")
    writer.flush()
  }

  /** Return true if this project refers to the Spark pull request builder. */
  private def isPullRequestBuilder(projectName: String): Boolean = {
    projectName.contains("SparkPullRequestBuilder")
  }

  /** Fetch the content of the specified URL and parse it as a JsonNode. */
  private def fetchJson(url: String): Option[JsonNode] = wrapForIndentation {
    try {
      val jsonUrl = url.stripSuffix("/") + "/" + JSON_URL_SUFFIX
      val jsonString = IOUtils.toString(new URL(jsonUrl).openStream)
      return Some(jsonMapper.readValue(jsonString, classOf[JsonNode]))
    } catch {
      case fnf: FileNotFoundException =>
        log(s"Unable to find JSON at ${shorten(url)}")
        log("It is likely that this build failed style tests or did not compile.")
      case e: Exception =>
        log(s"Exception when fetching JSON from ${shorten(url)}")
        wrapForIndentation { log(e.toString) }
    }
    None
  }

  /** Return a shorter, relative URL that does not include the Jenkins base URL. */
  private def shorten(url: String): String = {
    url.stripPrefix(JENKINS_URL_BASE)
  }

  /** Parse the Hadoop profile from the run URL. */
  private def parseHadoopProfile(url: String): String = {
    val splitKeys = Seq("AMPLAB_JENKINS_BUILD_PROFILE", "hadoop.version", "HADOOP_PROFILE")
    splitKeys.foreach { key =>
      if (url.contains(key)) {
        return url.split(s"$key=")(1).split(",")(0)
      }
    }
    return "N/A"
  }

  /** Translate the given hadoop profile to an expected Hadoop version. */
  private def getHadoopVersion(hadoopProfile: String): String = {
    if (hadoopProfile.matches("hadoop[-]*1\\.0")) { return "1.0.4" }
    if (hadoopProfile.matches("hadoop[-]*2\\.0")) { return "2.0.0" }
    if (hadoopProfile.matches("hadoop[-]*2\\.2")) { return "2.2.0" }
    if (hadoopProfile.matches("hadoop[-]*2\\.3")) { return "2.3.0" }
    if (hadoopProfile.matches("hadoop[-]*2\\.4")) { return "2.4.0" }
    // e.g. 1.0.4 -> 1.0.4
    // e.g. 2.0.0-mr1-cdh4.1.2 -> 2.0.0
    val oneRegex = "(1\\.[0-9]\\.[0-9]).*".r
    val twoRegex = "(2\\.[0-9]\\.[0-9]).*".r
    hadoopProfile match {
      case oneRegex(v) => v
      case twoRegex(v) => v
      case _ => hadoopProfile
    }
  }

  /** Log a message with the appropriate indentation. */
  private def log(msg: String): Unit = {
    println(indent + msg)
  }

  /** Execute the given code block at appropriate level of indentation. */
  private def wrapForIndentation[T](closure: => T): T = {
    val increment = "    " // 4 spaces
    indent += increment
    try {
      closure
    } finally {
      indent = indent.stripSuffix(increment)
    }
  }
  
}

private object JenkinsFetcher {
  private val JENKINS_URL_BASE = "https://amplab.cs.berkeley.edu/jenkins/job"
  private val JSON_URL_SUFFIX = "api/json"
  private val PULL_REQUEST_BUILDER_HADOOP_PROFILE = "2.3.0"

  def main(args: Array[String]): Unit = {
    println("\n=== *~ Welcome to Spark test failure fetcher v1.0! ~* ===\n")
    PropertiesReader.loadSystemProperties()
    val allJenkinsProjects = sys.props("spark.test.jenkinsProjects")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
    val outputFileName = sys.props("spark.test.outputFileName")
    val outputFileDelimiter = sys.props("spark.test.outputFileDelimiter")
    val maxTestAgeSeconds = sys.props("spark.test.fetcher.maxTestAge.seconds").toLong
    val fetcher = new JenkinsFetcher(
      allJenkinsProjects,
      outputFileName,
      outputFileDelimiter,
      maxTestAgeSeconds)
    fetcher.run()
    fetcher.stop()
  }
}

