package com.databricks.util

import java.io.{FileNotFoundException, File, FileInputStream}
import java.util.{Locale, Properties}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration

private[databricks] object PropertiesReader {

  val SPARK_TEST_CONF_DIR = "spark.test.conf.dir"

  /**
   * For each ".properties" file in the "conf" directory, load the properties
   * set in that file and merge them.
   * Files are sported by their case-insensitive filenames
   */
  def loadTestProperties(): Configuration = {
    val confdirName = System.getProperty(SPARK_TEST_CONF_DIR, new File("conf").getAbsolutePath)
    val confDir = new File(confdirName)
    if (!confDir.isDirectory) {
      throw new FileNotFoundException(s"Could not find the directory defined in" +
          s" $SPARK_TEST_CONF_DIR: - $confDir")
    }
    val dest = new Configuration(false)
    confDir.listFiles()
        .filter { f => f.getName.endsWith(".properties") }
        .sortWith ( (l, r) =>
          (l.getAbsolutePath.toLowerCase(Locale.ENGLISH)
          .compareTo(r.getAbsolutePath.toLowerCase(Locale.ENGLISH))) < 0)
        .foreach { pf => loadProperties(pf, dest) }
    // apply to all
    dest
  }

  /**
   * Load properties from the specified file and set them in
   * the dest file
   */
  def loadProperties(propertiesFile: File, dest: Configuration): Unit = {
    val properties = new Properties
    val in = new FileInputStream(propertiesFile)
    try {
      properties.load(in)
      properties.asScala.foreach { entry => dest.set(entry._1, entry._2) }
    } finally {
      in.close()
    }
  }
}

