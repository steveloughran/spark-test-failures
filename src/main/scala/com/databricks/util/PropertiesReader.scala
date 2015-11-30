package com.databricks.util

import java.io.{FileNotFoundException, File, FileInputStream}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.conf.Configuration

private[databricks] object PropertiesReader {

  val SPARK_TEST_CONF_DIR = "spark.test.conf.dir"

  /**
   * For each ".properties" file in the "conf" directory, load the properties
   * set in that file and set them in this JVM's system properties.
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
      .foreach { pf => loadProperties(pf, dest) }
    // apply to all
    val map = dest.asScala
    map.foreach { case (k, v) => sys.props.update(k, v) }
    return dest
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
      properties.asScala.foreach { case (k, v) => dest.set(k, v) }
    } finally {
      in.close()
    }
  }
}

