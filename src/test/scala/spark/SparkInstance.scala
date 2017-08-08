package spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.io.Source

object SparkInstance {
  val sparkSession = SparkSession.builder
    .master("local[2]")
    .appName("sparky")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .config("spark.cassandra.auth.username", "cassandra")
    .config("spark.cassandra.auth.password", "cassandra")
    .getOrCreate()
  val license = Source.fromInputStream(getClass.getResourceAsStream("/license.mit")).getLines.toSeq
  val kafkaProducerProperties = loadProperties("/kafka.producer.properties")
  val kafkaConsumerProperties = toMap(loadProperties("/kafka.consumer.properties"))
  val kafkaTopic = "license"

  def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }

  def toMap(properties: Properties): Map[String, String] = {
    import scala.collection.JavaConverters._
    properties.asScala.toMap
  }
}