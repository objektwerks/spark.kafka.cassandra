package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

object SparkApp extends App {
  val conf = new SparkConf().setAppName("sparky").setMaster("local[*]")
  val context = new SparkContext(conf)

  run

  def run(): Unit = {
    val streamingContext = new StreamingContext(context, Milliseconds(500))
    streamingContext.checkpoint("./target/output/main/checkpoint")
    val queue = mutable.Queue[RDD[String]]()
    val ds = streamingContext.queueStream(queue)
    val seq = Source.fromInputStream(getClass.getResourceAsStream("/license.mit")).getLines.toSeq
    queue += context.makeRDD(seq)
    val wordCountDs = ds.flatMap(l => l.split("\\P{L}+")).filter(_.nonEmpty).map(_.toLowerCase).map(w => (w, 1)).reduceByKey(_ + _)
    wordCountDs.saveAsTextFiles("./target/output/main/ds")
    wordCountDs.foreachRDD(rdd => {
      rdd.saveAsTextFile("./target/output/main/rdd")
    })
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(1000)
    streamingContext.stop(stopSparkContext = true, stopGracefully = true)
  }
}