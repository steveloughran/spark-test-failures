package com.databricks.util

import java.io.{FileNotFoundException, File, FileInputStream}
import java.util.Properties

import scala.collection.JavaConversions._

private[databricks] object PropertiesReader {

  val SPARK_TEST_CONF_DIR = "spark.test.conf.dir"

  /**
   * For each ".properties" file in the "conf" directory, load the properties
   * set in that file and set them in this JVM's system properties.
   */
  def loadSystemProperties(): Unit = {
    val confdirName = System.getProperty(SPARK_TEST_CONF_DIR, new File("conf").getAbsolutePath)
    val confDir = new File(confdirName)
    if (!confDir.isDirectory) {
      throw new FileNotFoundException(s"Could not find the directory defined in" +
          s" $SPARK_TEST_CONF_DIR: - $confDir")
    }
    confDir.listFiles()
      .filter { f => f.getName.endsWith(".properties") }
      .foreach { pf => loadSystemProperties(pf.getPath) }
  }

  /**
   * Load properties from the specified file and set them in
   * this JVM's system properties.
   */
  def loadSystemProperties(propertiesFile: String): Unit = {
    val properties = new Properties
    val in = new FileInputStream(propertiesFile)
    try {
      properties.load(in)
      properties.foreach { case (k, v) => sys.props.update(k, v) }
    } finally {
      in.close()
    }
  }
}

