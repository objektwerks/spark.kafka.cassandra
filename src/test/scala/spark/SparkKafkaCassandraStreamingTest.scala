package spark

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable

class SparkKafkaCassandraStreamingTest extends FunSuite with BeforeAndAfterAll {
  val conf = SparkInstance.conf
  val context = SparkInstance.context

  override protected def beforeAll(): Unit = {
    super.beforeAll
    createKafkaTopic()
    sendKafkaProducerMessages()
    val connector = CassandraConnector(conf)
    connector.withSessionDo { session =>
      session.execute("DROP KEYSPACE IF EXISTS streaming;")
      session.execute("CREATE KEYSPACE streaming WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };")
      session.execute("CREATE TABLE streaming.words(word text PRIMARY KEY, count int);")
    }
  }

  test("stateless spark streaming") {
    val streamingContext = new StreamingContext(context, Milliseconds(1000))
    val queue = mutable.Queue[RDD[String]]()
    val ds = streamingContext.queueStream(queue)
    queue += context.makeRDD(SparkInstance.license)
    val wordCountDs = countWords(ds)
    wordCountDs.saveAsTextFiles("./target/output/test/ds")
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(1000)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }

  test("streaming cassandra write") {
    import com.datastax.spark.connector.streaming._
    val streamingContext = new StreamingContext(context, Milliseconds(1000))
    val queue = mutable.Queue[RDD[String]]()
    val ds = streamingContext.queueStream(queue)
    queue += context.makeRDD(SparkInstance.license)
    val wordCountDs = countWords(ds)
    wordCountDs.repartitionByCassandraReplica(keyspaceName = "streaming", tableName = "words", partitionsPerHost = 2)
    wordCountDs.saveToCassandra("streaming", "words", SomeColumns("word", "count"))
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(1000)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }

  test("streaming cassandra read") {
    import com.datastax.spark.connector.streaming._
    val streamingContext = new StreamingContext(context, Milliseconds(1000))
    val rdd = streamingContext.cassandraTable("streaming", "words").select("word", "count").cache
    assert(rdd.count == 95)
    assert(rdd.map(_.getInt("count")).sum == 168)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }

  test("kafka spark streaming") {
    val streamingContext = new StreamingContext(context, Milliseconds(1000))
    val kafkaParams = SparkInstance.kafkaConsumerProperties
    val kafkaTopics = Set(SparkInstance.kafkaTopic)
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](kafkaTopics, kafkaParams)
    )
    stream.saveAsTextFiles("./target/output/test/text/dstream")
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(1000)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }

  private def sendKafkaProducerMessages(): Unit = {
    val producer = new KafkaProducer[String, String](SparkInstance.kafkaProducerProperties)
    val rdd = context.makeRDD(SparkInstance.license)
    val wordCounts = countWords(rdd).collect()
    val messages = new AtomicInteger()
    wordCounts.foreach { wc =>
      val record = new ProducerRecord[String, String](SparkInstance.kafkaTopic, 0, wc._1, wc._2.toString)
      producer.send(record)
      messages.incrementAndGet()

    }
    producer.close(3000L, TimeUnit.MILLISECONDS)
    println(s"Sent ${messages.get} messages to Kafka topic: ${SparkInstance.kafkaTopic}.")
  }

  private def createKafkaTopic(): Unit = {
    val zkClient = ZkUtils.createZkClient("localhost:2181", 10000, 10000)
    val zkUtils = ZkUtils(zkClient, isZkSecurityEnabled = false)
    val topicMetadata = AdminUtils.fetchTopicMetadataFromZk(SparkInstance.kafkaTopic, zkUtils)
    println(s"Kafka topic: ${topicMetadata.topic}")
    if (topicMetadata.topic != SparkInstance.kafkaTopic) {
      AdminUtils.createTopic(zkUtils, SparkInstance.kafkaTopic, 1, 1, SparkInstance.kafkaProducerProperties)
      println(s"Kafka Topic ( ${SparkInstance.kafkaTopic} ) created.")
    }
  }

  private def countWords(rdd: RDD[String]): RDD[(String, Int)] = {
    rdd.flatMap(l => l.split("\\P{L}+")).filter(_.nonEmpty).map(_.toLowerCase).map(w => (w, 1)).reduceByKey(_ + _)
  }

  private def countWords(ds: DStream[String]): DStream[(String, Int)] = {
    ds.flatMap(l => l.split("\\P{L}+")).filter(_.nonEmpty).map(_.toLowerCase).map(w => (w, 1)).reduceByKey(_ + _)
  }
}