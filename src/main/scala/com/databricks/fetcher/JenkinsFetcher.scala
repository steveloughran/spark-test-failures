package com.databricks.fetcher

import java.io.{IOException, FileNotFoundException, FileWriter}
import java.net.URL
import java.util.Date

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable

import com.databricks.util.{FailedSuite, PropertiesReader}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.util.ExitUtil.ExitException

import org.apache.spark.Logging

/**
 * A class that fetches test reports from Jenkins and logs test failure
 * information to a CSV output file. Each line in output file represents
 * a single instance of a failed suite.
 */
class JenkinsFetcher(
    val urlbase: String,
    val allJenkinsProjects: Seq[String],
    val outputFileName: String,
    val outputFileDelimiter: String,
    maxTestAgeSeconds: Long,
    val packages: String,
    val emptyString: String = "N/A") extends Logging{
  import JenkinsFetcher._



  private val jsonMapper = new ObjectMapper
  private val writer = new FileWriter(outputFileName)
  private var indent = ""
  private val maxTestAgeMillis = Option(maxTestAgeSeconds * 1000)
    .filter(_ >= 0)
    .getOrElse(Long.MaxValue)

  log(s"Fetching Jenkins test reports for packages under $packages from ${urlbase}/[...]")
  log(s"Logging test failure information to ${outputFileName}\n")

  // Write the header
  writer.write(FailedSuite.schema.mkString(outputFileDelimiter) + "\n")
  writer.flush()

  /**
   * Fetch test reports for each of the configured Jenkins projects
   * and log information about any test failures to the output file.
   */
  def run(): (Int, Int) = {
    var successCount = 0
    var attemptCount = 0
    allJenkinsProjects.foreach { projectName =>
      val projectUrl = s"${urlbase}/$projectName"
      val (attempts, successes) = handleProject(projectUrl, projectName)
      attemptCount += attempts
      successCount += successes
    }
    (attemptCount, successCount)
  }

  /** Stop the fetcher and close any resources used in the process. */
  def stop(): Unit = {
    writer.close()
  }

  // e.g. https://amplab.cs.berkeley.edu/jenkins/job/SparkPullRequestBuilder
  // e.g. https://amplab.cs.berkeley.edu/jenkins/job/Spark-Master-SBT
  // e.g. https://amplab.cs.berkeley.edu/jenkins/job/Spark-Master-Maven-pre-YARN/
  private def handleProject(projectUrl: String, projectName: String): (Int, Int) = {
    log(s"========== Fetching test reports for project $projectName ==========")
    val projectJson = fetchJson(projectUrl).getOrElse { return (0, 0) }
    val firstBuild = projectJson.get("firstBuild").get("number").asInt
    val lastBuild = projectJson.get("lastBuild").get("number").asInt
    assert(lastBuild >= firstBuild)
    log(s"number of test runs: ${lastBuild - firstBuild}")
    var successCount = 0
    var attemptCount = 0
    (firstBuild to lastBuild).reverse.foreach { buildNumber =>
      val buildUrl = s"${projectUrl}/$buildNumber"
      val (attempts, successes) = handleBuild(buildUrl, projectName)
      attemptCount += attempts
      successCount += successes
    }
    (attemptCount, successCount)
  }

  // e.g. .../SparkPullRequestBuilder/28354
  // e.g. .../Spark-Master-SBT/1847
  // e.g. .../Spark-Master-Maven-pre-YARN/1760
  private def handleBuild(buildUrl: String, projectName: String): (Int, Int) = {
    val buildJson = fetchJson(buildUrl).getOrElse { return  (0, 0) }
    val buildTimestamp = buildJson.get("timestamp").asLong
    val buildTooOld = System.currentTimeMillis - buildTimestamp > maxTestAgeMillis
    var successCount = 0
    var attemptCount = 0
    if (!buildTooOld) {
      // Each build in the pull request builder only has one run, so use the build URL directly
      if (isPullRequestBuilder(projectName)) {
        logDebug("Pull request Builder")
        attemptCount += 1
        successCount += (if (handleRun(buildUrl, projectName)) 1 else 0)
      } else {
        val runsJson = buildJson.get("runs")
        if (runsJson.size()> 0) {

          runsJson.iterator.foreach { runJson =>
            attemptCount += 1
            val runUrl = runJson.get("url").asText.stripSuffix("/")
            successCount += (if (handleRun(buildUrl, projectName)) 1 else 0)
          }
        } else {
          logInfo(s"Project $projectName has no runs")
        }
      }
    } else {
      logInfo(s"Build ${shorten(buildUrl)} is too old: ${new Date(buildTimestamp) }")
    }
    (attemptCount, successCount)
  }

  private def getJsonField(url: String, document: JsonNode, field:String): JsonNode = {
    document.get(field) match {
      case null =>
        val text = s"No field '$field' in JSON at $url"
        logDebug(text + "\n" + document.toString)
        throw new RuntimeException(text)
      case node => node
    }
  }

  @tailrec
  private def getJsonFields(url: String, document: JsonNode, fields: List[String]): JsonNode = {
    fields match {
      case Nil =>
        document
      case (field :: tail) =>
        val doc2 = getJsonField (url, document, field)
        getJsonFields(url +":" + field, doc2, tail)
    }
   }

  // e.g. .../SparkPullRequestBuilder/28354
  // e.g. .../Spark-Master-SBT/1847/AMPLAB_JENKINS_BUILD_PROFILE=hadoop1.0,label=centos/
  // e.g. .../Spark-Master-Maven-pre-YARN/1760/hadoop.version=1.0.4,label=centos/
  private def handleRun(runUrl: String, projectName: String): Boolean = wrapForIndentation {
    log(s"Handle run ${runUrl}")
    val jsonURL = toJsonURL(runUrl)
    val runJson = fetchJson(runUrl).getOrElse {
      logDebug(s"No run report at $runUrl")
      return false
    }
    val runTimestamp = getJsonField(jsonURL, runJson, "timestamp").asLong
    val testReportUrl = s"$runUrl/testReport"
    val testReportJsonURL = toJsonURL(testReportUrl)
    val testReportJson = fetchJson(testReportUrl).getOrElse {
      logDebug(s"No test report at $testReportUrl ")
      return false
    }
    val numTestsFailed = getJsonField(testReportJsonURL, testReportJson, "failCount").asInt
    if (numTestsFailed > 0) {
      wrapForIndentation {
        log(s"Found $numTestsFailed failed test(s)")
        // Suite name -> number of failed occurrences in this run
        val failedSuiteCounts = new mutable.HashMap[String, Int]
        val failedSuiteStatuses = Set("FAILED", "REGRESSION")
        val childReports = getJsonFields(testReportJsonURL, testReportJson,
            List("childReports"))
        // >1 child can come back. For now, pick #1
        if (childReports.size() == 0) {
          logDebug("No child reports")
          return false;
        }
        val suitesJson = getJsonFields(testReportJsonURL + ":childReports",
            childReports.get(0),
            List("result", "suites"))

        suitesJson.iterator.foreach { suiteJson =>
          val failedSuiteNames = getJsonField(testReportJsonURL, suiteJson, "cases").iterator
            .filter { caseJson =>
              failedSuiteStatuses.contains(getJsonField(testReportJsonURL, caseJson, "status").asText) }
            .map { getJsonField(testReportJsonURL, _, "className").asText }
            .filter { _.startsWith(packages) }
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
          log(s"Warning: No failed suites beginning with '$packages' found.")
        }
      }
    } else {
      logInfo("No test failures")
    }
    true
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
        pull_request_builder_hadoop_profile
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
      val jsonUrl = toJsonURL(url)
      val src = new URL(jsonUrl)
      log(s"GET $src")
      val jsonString = IOUtils.toString(src.openStream)
      // read operation closes stream afterwards
      return Some(jsonMapper.readValue(jsonString, classOf[JsonNode]))
    } catch {
      case fnf: FileNotFoundException =>
        log(s"Unable to find JSON at $url")
        log("It is likely that this build failed style tests, did not compile" +
            " -or is still executing.")
      case e: Exception =>
        log(s"Exception when fetching JSON from ${shorten(url)}")
        wrapForIndentation { log(e.toString) }
    }
    None
  }

  def toJsonURL(url: String): String = {
    url.stripSuffix("/") + "/" + JSON_URL_SUFFIX
  }

  /** Return a shorter, relative URL that does not include the Jenkins base URL. */
  private def shorten(url: String): String = {
    url.stripPrefix(urlbase)
  }

  /** Parse the Hadoop profile from the run URL. */
  private def parseHadoopProfile(url: String): String = {
    val splitKeys = Seq("AMPLAB_JENKINS_BUILD_PROFILE", "hadoop.version", "HADOOP_PROFILE")
    splitKeys.foreach { key =>
      if (url.contains(key)) {
        url.split(s"$key=")(1).split(",")(0)
      }
    }
    emptyString
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

private object JenkinsFetcher extends Logging {
  private val JSON_URL_SUFFIX = "api/json"
  private val pull_request_builder_hadoop_profile = "2.3.0"

  val JENKINS_URL_BASE = "spark.test.jenkins.url.base"

  val USAGE =
    s"""
      | Jenkins Fetcher
      | ---------------
      | '$JENKINS_URL_BASE' base URL, e.g https://amplab.cs.berkeley.edu/jenkins/job
      | 'spark.test.jenkinsProjects' list of projects, e.g. SparkPullRequestBuilder,Spark-Master-SBT
      | 'spark.test.outputFileName' path for output file, e.g. jenkins.csv
      | 'spark.test.outputFileDelimiter, delimiter between entries, e.g ', '
      | 'spark.test.packages' packages to scan, e.g. 'org.apache.spark'
      | 'spark.test.fetcher.maxTestAge.seconds' maximum age of test runs, e.g. '600'
    """.stripMargin
  
  def requiredProperty(key: String): String = {
    sys.props(key) match {
      case null => throw new ExitException(1, s"Unset property '$key'\n$USAGE")
      case "" => throw new ExitException(1, s"Empty property '$key'\n$USAGE")
      case prop => prop
    }
  }

  /**
   * optional property, reverting to `defVal` if the property is not defined.
   * @param key key to look for
   * @param defVal default value
   * @return a string
   */
  def optionalProperty(key: String, defVal: String): String = {
    sys.props(key) match {
      case null => defVal
      case prop => prop
    }
  }
  
  def innerMain(args: Array[String]): Int = {
    println("\n=== *~ Welcome to Spark test failure fetcher v1.0! ~* ===\n")
    PropertiesReader.loadSystemProperties()
    val allJenkinsProjects = requiredProperty("spark.test.jenkinsProjects")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)

    val jenkins_url_base = requiredProperty(JENKINS_URL_BASE)
    val outputFileName = requiredProperty("spark.test.outputFileName").trim
    val outputFileDelimiter = requiredProperty("spark.test.outputFileDelimiter")
    val packages = requiredProperty("spark.test.packages").trim
    val beforeEntry = optionalProperty("spark.test.delimiter.before","\"")
    val afterEntry = optionalProperty("spark.test.delimiter.after","\"")
    val emptyEntry = optionalProperty("spark.test.empty.column","")

    val maxTestAgeSeconds = requiredProperty("spark.test.fetcher.maxTestAge.seconds").toLong
    val fetcher = new JenkinsFetcher(
      jenkins_url_base,
      allJenkinsProjects,
      outputFileName,
      outputFileDelimiter,
      maxTestAgeSeconds,
      packages,
      emptyEntry)
    val (attempts, successes) = fetcher.run()
    fetcher.stop()
    println(s"$attempts attempts; $successes successes")
    if (successes > 0) 0 else 1
  }

  
  def main(args: Array[String]): Unit = {
    try {
      val exitCode = innerMain(args)
      sys.exit(exitCode)
    } catch {
      case ee: ExitException =>
        println(ee.toString)
        sys.exit(ee.status)
      case err: Throwable =>
        println(err)
        err.printStackTrace(System.err)
        sys.exit(-1)
    }
  }
}

