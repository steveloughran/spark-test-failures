
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

case class FailedSuite(
    suiteName: String,
    projectName: String,
    numTestsFailed: Int,
    hadoopProfile: String,
    hadoopVersion: String,
    timestamp: Long,
    url: String)

object Main {
  private val OUTPUT_FILE_NAME = "output.csv"
  private val OUTPUT_DELIMITER = ";"

  def main(args: Array[String]) {

    // Set up Spark and SQL contexts
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Best app ever")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Set up failed suites table
    val failedSuitesRDD = sc.textFile(OUTPUT_FILE_NAME)
      .filter { line => !line.startsWith("Suite name") }
      .map { line => line.split(OUTPUT_DELIMITER) }
      .map { items =>
        FailedSuite(items(0), items(1), items(2).toInt,
          items(3), items(4), items(5).toLong, items(6))
      }
    val failedSuitesDF = failedSuitesRDD.toDF()
    failedSuitesDF.registerTempTable("failedSuites")

    val result = sqlContext.sql("SELECT * FROM failedSuites")
    println(result.count())
    println("Done.")
  }
}
