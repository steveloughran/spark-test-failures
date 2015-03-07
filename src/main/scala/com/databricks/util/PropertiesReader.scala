package com.databricks.util

import java.io.{File, FileInputStream}
import java.util.Properties

import scala.collection.JavaConversions._

private[databricks] object PropertiesReader {

  /**
   * For each ".properties" file in the "conf" directory, load the properties
   * set in that file and set them in this JVM's system properties.
   */
  def loadSystemProperties(): Unit = {
    val confDir = new File("conf")
    if (confDir.exists && confDir.isDirectory) {
      confDir.listFiles()
        .filter { f => f.getName.endsWith(".properties") }
        .foreach { pf => loadSystemProperties(pf.getPath) }
    }
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

