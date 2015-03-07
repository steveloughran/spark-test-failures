package com.databricks.reporter

import java.io.File
import java.net.URL
import java.text.DateFormat
import java.util.Date

import scala.collection.JavaConversions._
import scala.collection.mutable

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.drive.{Drive, DriveScopes}
import com.google.api.services.drive.model.{Permission, File => GoogleFile}
import com.google.gdata.client.spreadsheet.SpreadsheetService
import com.google.gdata.data.PlainTextConstruct
import com.google.gdata.data.spreadsheet._

import com.databricks.util.{FailedSuite, PropertiesReader}

/**
 * A reporter that populates a Google spreadsheet with test failure information.
 *
 * Each reporter is associated with exactly one spreadsheet.
 * If no existing spreadsheets are specified, then a new one is created.
 */
private[reporter] class GoogleSpreadsheetReporter(
    allJenkinsProjects: Seq[String],
    outputFileName: String,
    outputFileDelimiter: String,
    serviceAccountId: String,
    privateKeyPath: String,
    existingSpreadsheetId: Option[String],
    newSpreadsheetOwners: Seq[String])
  extends AbstractReporter(allJenkinsProjects, outputFileName, outputFileDelimiter) {

  import GoogleSpreadsheetReporter._

  private val httpTransport = new NetHttpTransport
  private val jsonFactory = new JacksonFactory
  private val credentials = setupCredentials()
  private val drive = new Drive.Builder(httpTransport, jsonFactory, credentials)
    .setApplicationName(DEFAULT_APPLICATION_NAME)
    .build()
  private val spreadsheetId = existingSpreadsheetId.getOrElse(createSpreadsheet())
  private val spreadsheetUrl = new URL(s"${GOOGLE_SPREADSHEET_FEED_URL}/$spreadsheetId")
  private val spreadsheetService = new SpreadsheetService(DEFAULT_APPLICATION_NAME)
  private val projectToFeed = new mutable.HashMap[String, CellFeed]

  println(
    s"""
      |You may access the spreadsheet at
      |
      |  ${GOOGLE_SPREADSHEET_VIEW_URL}/${spreadsheetId}
    """.stripMargin)

  // Set up the spreadsheet with the correct credentials.
  spreadsheetService.setOAuth2Credentials(credentials)
  updateSpreadsheetOwners()
  initializeWorksheets()

  /**
   * Populate the spreadsheet with test failure information.
   * This assumes that the worksheets are initialized properly.
   */
  def run(): Unit = {
    println("Populating schema in all worksheets...")
    populateHeaders()

    // Write aggregate statistics in parent worksheet
    println("Populating aggregate statistics across all projects...")
    val parentFeed = getParentFeed
    var row = FIRST_NON_HEADER_ROW
    getAggregateCounts.foreach { case (suiteName, count) =>
      parentFeed.insert(new CellEntry(row, 1, suiteName))
      parentFeed.insert(new CellEntry(row, 2, count.toString))
      row += 1
    }

    // Write aggregate statistics in each project worksheet
    allJenkinsProjects.foreach { project =>
      row = FIRST_NON_HEADER_ROW
      println(s"Populating aggregate statistics in ${project}...")
      val feed = projectToFeed(project)
      getAggregateCountsByProject(project).foreach { case (suiteName, count) =>
        feed.insert(new CellEntry(row, 1, suiteName))
        feed.insert(new CellEntry(row, 2, count.toString))
        row += 1
      }
    }

    // Write individual failed suite information in each project worksheet
    allJenkinsProjects.foreach { project =>
      row = FIRST_NON_HEADER_ROW
      println(s"Populating information for individual failure instances in ${project}...")
      val feed = projectToFeed(project)
      getRowsByProject(project).foreach { failedSuite =>
        val linkedSuiteName = s"""=HYPERLINK("${failedSuite.url}"; "${failedSuite.suiteName}")"""
        feed.insert(new CellEntry(row, 4, linkedSuiteName))
        feed.insert(new CellEntry(row, 5, failedSuite.numTestsFailed.toString))
        feed.insert(new CellEntry(row, 6, failedSuite.hadoopProfile))
        feed.insert(new CellEntry(row, 7, failedSuite.hadoopVersion))
        val dateTime = DateFormat.getDateTimeInstance(
          DateFormat.LONG, DateFormat.LONG).format(failedSuite.time)
        feed.insert(new CellEntry(row, 8, dateTime))
        row += 1
      }
    }

  }

  /** Set up credentials to connect to the Google services API. */
  private def setupCredentials(): GoogleCredential = {
    val serviceScopes = Seq(
      DriveScopes.DRIVE,
      "https://spreadsheets.google.com/feeds",
      "https://docs.google.com/feeds")
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(serviceAccountId)
      .setServiceAccountPrivateKeyFromP12File(new File(privateKeyPath))
      .setServiceAccountScopes(serviceScopes)
      .build()
  }

  /**
   * Create a new spreadsheet and return its ID.
   * This is only called if an existing spreadsheet is not provided.
   */
  private def createSpreadsheet(): String = {
    val newSpreadsheet = new GoogleFile()
      .setTitle(DEFAULT_SPREADSHEET_NAME)
      .setMimeType("application/vnd.google-apps.spreadsheet")
    drive.files().insert(newSpreadsheet).execute().getId
  }

  /**
   * Grant the specified users ownership of the spreadsheet.
   */
  private def updateSpreadsheetOwners(): Unit = {
    newSpreadsheetOwners.foreach { email =>
      val permission = new Permission()
        .setRole("owner")
        .setType("user")
        .setValue(email)
      drive.permissions.insert(spreadsheetId, permission).execute()
    }
  }

  /**
   * Return a fresh worksheet feed for the configured spreadsheet.
   * It is necessary to refresh the feed after adding, removing, or renaming worksheets.
   */
  private def getWorksheetFeed: WorksheetFeed = {
    val spreadsheetEntry = spreadsheetService.getEntry(spreadsheetUrl, classOf[SpreadsheetEntry])
    spreadsheetService.getFeed(spreadsheetEntry.getWorksheetFeedUrl, classOf[WorksheetFeed])
  }

  /**
   * Delete all existing worksheets and add a new one for each Jenkins project.
   * Additionally, create a parent worksheet for reporting aggregate statistics across all projects.
   */
  private def initializeWorksheets(): Unit = {
    val worksheetFeed = getWorksheetFeed
    val existingWorksheets = worksheetFeed.getEntries

    println(s"Refreshing worksheets...")

    // Delete all existing worksheets. Note that we must create a dummy worksheet in the
    // process because Google requiers us to keep around a minimum of one worksheet.
    val dummyWorksheetTitle = "DUMMY_WORKSHEET_" + System.currentTimeMillis
    val dummyWorksheet = new WorksheetEntry(1, 1)
    dummyWorksheet.setTitle(new PlainTextConstruct(dummyWorksheetTitle))
    worksheetFeed.insert(dummyWorksheet)
    existingWorksheets.foreach { ws =>
      println(s" * Deleting existing worksheet ${ws.getTitle.getPlainText}")
      ws.delete()
    }

    // Create a new worksheet for each Jenkins project
    val newWorksheetTitles = Seq(PARENT_WORKSHEET_NAME) ++ allJenkinsProjects
    newWorksheetTitles.foreach { title =>
      println(s" * Adding new worksheet $title")
      val newWorksheet = new WorksheetEntry()
      newWorksheet.setTitle(new PlainTextConstruct(title))
      newWorksheet.setRowCount(1000)
      newWorksheet.setColCount(26)
      worksheetFeed.insert(newWorksheet)
    }

    // Delete the dummy worksheet we created in the beginning
    // Note that we must use a new feed here
    getWorksheetFeed.getEntries
      .find(_.getTitle.getPlainText == dummyWorksheetTitle)
      .foreach(_.delete())

    // Populate mapping from project name to cell feed
    getWorksheetFeed.getEntries.foreach { ws =>
      val title = ws.getTitle.getPlainText
      val feed = spreadsheetService.getFeed(ws.getCellFeedUrl, classOf[CellFeed])
      projectToFeed(title) = feed
    }
  }

  /** Return the feed used for populating the parent worksheet. */
  private def getParentFeed: CellFeed = projectToFeed(PARENT_WORKSHEET_NAME)

  /** Populate the header row in all worksheets. */
  private def populateHeaders(): Unit = {
    val parentFeed = getParentFeed
    parentFeed.insert(new CellEntry(1, 1, "Failed suite"))
    parentFeed.insert(new CellEntry(1, 2, "Runs failed"))
    projectToFeed.values
      .filter(_ != parentFeed)
      .foreach { childFeed =>
        childFeed.insert(new CellEntry(1, 1, "Failed suite"))
        childFeed.insert(new CellEntry(1, 2, "Runs failed"))
        // blank column
        childFeed.insert(new CellEntry(1, 4, "Individual failed suite"))
        childFeed.insert(new CellEntry(1, 5, "Tests failed"))
        childFeed.insert(new CellEntry(1, 6, "Hadoop profile"))
        childFeed.insert(new CellEntry(1, 7, "Hadoop version"))
        childFeed.insert(new CellEntry(1, 8, "Date and time"))
      }
  }

}

private object GoogleSpreadsheetReporter {
  private val DEFAULT_APPLICATION_NAME = "Spark Test Failures"
  private val DEFAULT_SPREADSHEET_NAME = DEFAULT_APPLICATION_NAME
  private val PARENT_WORKSHEET_NAME = "Parent"
  private val GOOGLE_DEVELOPER_CONSOLE_URL = "https://console.developers.google.com"
  private val GOOGLE_SPREADSHEET_FEED_URL = "https://spreadsheets.google.com/feeds/spreadsheets"
  private val GOOGLE_SPREADSHEET_VIEW_URL = "https://docs.google.com/spreadsheets/d"
  private val FIRST_NON_HEADER_ROW = 2

  def main(args: Array[String]): Unit = {
    println("\n=== *~ Welcome to Spark test failure reporter v1.0! ~* ===\n")
    PropertiesReader.loadSystemProperties()
    val allJenkinsProjects = sys.props("spark.test.jenkinsProjects")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
    val outputFileName = sys.props("spark.test.outputFileName")
    val outputFileDelimiter = sys.props("spark.test.outputFileDelimiter")
    val serviceAccountId =
      sys.props.get("spark.test.reporter.google.serviceAccountId").getOrElse {
        throw new IllegalArgumentException(
          """
            |
            |  Service account ID is missing! Please set this through
            |  'spark.test.reporter.google.serviceAccountId'. If you do not already have
            |  one, please create a project through the Google developer console at
            |  $GOOGLE_DEVELOPER_CONSOLE_URL. The ID to use here should look something
            |  like [...]@developer.gserviceaccount.com.
            |
          """.stripMargin)
      }
    val privateKeyPath =
      sys.props.get("spark.test.reporter.google.privateKeyPath").getOrElse {
        throw new IllegalArgumentException(
          """
            |
            |  Path to your google services private key is missing! Please set this
            |  through 'spark.test.reporter.google.privateKeyPath'. If you do not already
            |  have one, please create a project through the Google developer console
            |  at $GOOGLE_DEVELOPER_CONSOLE_URL.
            |
          """.stripMargin)
      }
    val existingSpreadsheetId = sys.props.get("spark.test.reporter.google.spreadsheetId")
    val newSpreadsheetOwners =
      sys.props.get("spark.test.reporter.google.spreadsheetOwners")
        .getOrElse("")
        .split(",")
        .filter(_.nonEmpty)
    val reporter = new GoogleSpreadsheetReporter(
      allJenkinsProjects,
      outputFileName,
      outputFileDelimiter,
      serviceAccountId,
      privateKeyPath,
      existingSpreadsheetId,
      newSpreadsheetOwners)
    reporter.run()
  }
}

