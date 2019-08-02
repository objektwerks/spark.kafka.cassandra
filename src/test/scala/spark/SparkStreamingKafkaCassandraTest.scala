package spark

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable

class SparkStreamingKafkaCassandraTest extends FunSuite with Matchers {
  import SparkInstance._

  test("stateless spark streaming") {
    val streamingContext = new StreamingContext(sparkContext, Milliseconds(1000))
    val queue = mutable.Queue[RDD[String]]()
    val ds = streamingContext.queueStream(queue)
    queue += sparkContext.makeRDD(license)
    val wordCountDs = countWords(ds)
    wordCountDs.saveAsTextFiles("./target/test/ds1")
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(1000)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }

  test("streaming cassandra write") {
    import com.datastax.spark.connector.streaming._
    val streamingContext = new StreamingContext(sparkContext, Milliseconds(1000))
    val queue = mutable.Queue[RDD[String]]()
    val ds = streamingContext.queueStream(queue)
    queue += sparkContext.makeRDD(license)
    val wordCountDs = countWords(ds)
    wordCountDs.repartitionByCassandraReplica(keyspaceName = "streaming", tableName = "words", partitionsPerHost = 2)
    wordCountDs.saveToCassandra("streaming", "words", SomeColumns("word", "count"))
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(1000)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }

  test("streaming cassandra read") {
    import com.datastax.spark.connector.streaming._
    val streamingContext = new StreamingContext(sparkContext, Milliseconds(1000))
    val rdd = streamingContext.cassandraTable("streaming", "words").select("word", "count").cache
    rdd.count shouldBe 96
    rdd.map(_.getInt("count")).sum shouldBe 169.0
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }

  test("kafka spark streaming") {
    val streamingContext = new StreamingContext(sparkContext, Milliseconds(1000))
    val kafkaParams = kafkaConsumerProperties
    val kafkaTopics = Set(licenseTopic)
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](kafkaTopics, kafkaParams)
    )
    stream.saveAsTextFiles("./target/test/ds2")
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(1000)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }
}