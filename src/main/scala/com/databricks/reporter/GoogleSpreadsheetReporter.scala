package com.databricks.reporter

import java.io.File
import java.net.URL

import scala.collection.JavaConversions._

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.drive.{Drive, DriveScopes}
import com.google.api.services.drive.model.{Permission, File => GoogleFile}
import com.google.gdata.client.spreadsheet.SpreadsheetService
import com.google.gdata.data.PlainTextConstruct
import com.google.gdata.data.spreadsheet._

/**
 * A reporter that populates a Google spreadsheet with test failure information.
 *
 * Each reporter is associated with exactly one spreadsheet.
 * If no existing spreadsheets are specified, then a new one is created.
 */
private[reporter] class GoogleSpreadsheetReporter(
    serviceAccountId: String,
    privateKeyPath: String,
    existingSpreadsheetId: Option[String],
    newSpreadsheetOwners: Seq[String])
  extends TestFailureReporter {

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

  // Set up the spreadsheet with the correct credentials.
  spreadsheetService.setOAuth2Credentials(credentials)
  updateSpreadsheetOwners()
  initializeWorksheets()

  /** Populate the spreadsheet with test failure information. */
  def run(): Unit = {
    val worksheetFeed = getWorksheetFeed
    val worksheets = worksheetFeed.getEntries
    val cellFeed = spreadsheetService.getFeed(worksheets.get(0).getCellFeedUrl, classOf[CellFeed])
    val cellEntry = new CellEntry(1, 1, "xxxfefexxx")
    cellFeed.insert(cellEntry)
    println(s"You may access the spreadsheet at ${GOOGLE_SPREADSHEET_VIEW_URL}/$spreadsheetId")
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

    println(s"Refreshing worksheets in spreadsheet $spreadsheetId. This may take a while...")

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
    val jenkinsProjects = Seq("SparkPullRequestBuilder", "Spark-Some-Maven-Build", "Spark-Some-SBT-Build")
    val newWorksheetTitles = Seq("Parent") ++ jenkinsProjects
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
  }

}

object GoogleSpreadsheetReporter {
  private val DEFAULT_APPLICATION_NAME = "Spark Test Failures"
  private val DEFAULT_SPREADSHEET_NAME = DEFAULT_APPLICATION_NAME
  private val GOOGLE_DEVELOPER_CONSOLE_URL = "https://console.developers.google.com"
  private val GOOGLE_SPREADSHEET_FEED_URL = "https://spreadsheets.google.com/feeds/spreadsheets"
  private val GOOGLE_SPREADSHEET_VIEW_URL = "https://docs.google.com/spreadsheets/d"

  // Identifier used by Google to set up credentials necessary for populating spreadsheets
  private val serviceAccountId =
    sys.props.get("spark.reporter.google.serviceAccountId").getOrElse {
      throw new IllegalArgumentException(
        """
          |
          |  Service account ID is missing! Please set this through
          |  'spark.reporter.google.serviceAccountId'. If you do not already have
          |  one, please create a project through the Google developer console at
          |  $GOOGLE_DEVELOPER_CONSOLE_URL. The ID to use here should look something
          |  like [...]@developer.gserviceaccount.com.
          |
        """.stripMargin)
    }

  // Path to your .p12 private key file for setting up Google credentials
  private val privateKeyPath =
    sys.props.get("spark.reporter.google.privateKeyPath").getOrElse {
      throw new IllegalArgumentException(
        """
          |
          |  Path to your google services private key is missing! Please set this
          |  through 'spark.reporter.google.privateKeyPath'. If you do not already
          |  have one, please create a project through the Google developer console
          |  at $GOOGLE_DEVELOPER_CONSOLE_URL.
          |
        """.stripMargin)
    }

  // An existing spreadsheet the user would like to populate
  // If this is not set, a new spreadsheet will be created
  private val existingSpreadsheetId = sys.props.get("spark.reporter.google.spreadsheetId")

  // Give the following comma-delimited list of email addresses owner permission to the spreadsheet
  private val newSpreadsheetOwners =
    sys.props.get("spark.reporter.google.spreadsheetOwners")
      .getOrElse("")
      .split(",")
      .filter(_.nonEmpty)

  def main(args: Array[String]): Unit = {
    val reporter = new GoogleSpreadsheetReporter(
      serviceAccountId, privateKeyPath, existingSpreadsheetId, newSpreadsheetOwners)
    reporter.run()
  }
}

