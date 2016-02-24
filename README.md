# A tool for tracking Spark test failures

Steps to use:

  1. Create a project through Google developer console: https://console.developers.google.com
  2. Configure conf/setting.properties
  3. `mvn clean package`
  4. java -cp target/[the jar] com.databricks.fetcher.JenkinsFetcher
  5. java -cp target/[the jar] com.databricks.reporter.GoogleSpreadsheetReporter
  6. secrets can go into `secrets.properties` for the test run

Questions? Contact andrew@databricks.com.

