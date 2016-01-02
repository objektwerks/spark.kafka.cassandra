package spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object SparkInstance {
  val conf = new SparkConf().setMaster("local[2]").setAppName("sparky").set("spark.cassandra.connection.host", "127.0.0.1")
  val context = new SparkContext(conf)
  val license = Source.fromInputStream(getClass.getResourceAsStream("/license.mit")).getLines.toSeq
}