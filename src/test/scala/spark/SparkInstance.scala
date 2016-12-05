package spark

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object SparkInstance {
  val conf = new SparkConf(loadDefaults = true)
    .setMaster("local[2]")
    .setAppName("sparky")
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.cassandra.auth.username", "cassandra")
    .set("spark.cassandra.auth.password", "cassandra")
  val context = new SparkContext(conf)
  val license = Source.fromInputStream(getClass.getResourceAsStream("/license.mit")).getLines.toSeq
  val kafkaProducerProperties = loadProperties("/kafka.producer.properties")
  val kafkaTopic = "license"

  private def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }
}