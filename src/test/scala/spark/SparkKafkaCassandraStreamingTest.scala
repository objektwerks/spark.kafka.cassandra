package spark

import java.util.concurrent.TimeUnit

import com.datastax.driver.core.Cluster
import com.datastax.spark.connector.SomeColumns
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.mutable

class SparkKafkaCassandraStreamingTest extends FunSuite with BeforeAndAfterAll with Matchers {
  import SparkInstance._

  val logger = Logger.getLogger(classOf[SparkKafkaCassandraStreamingTest])
  val sparkContext = sparkSession.sparkContext

  override protected def beforeAll(): Unit = {
    createKafkaTopic()
    sendKafkaProducerMessages()
    createCassandraKeyspace()
  }

  test("stateless spark streaming") {
    val streamingContext = new StreamingContext(sparkContext, Milliseconds(1000))
    val queue = mutable.Queue[RDD[String]]()
    val ds = streamingContext.queueStream(queue)
    queue += sparkContext.makeRDD(license)
    val wordCountDs = countWords(ds)
    wordCountDs.saveAsTextFiles("./target/output/test/ds")
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
    rdd.count shouldBe 95
    rdd.map(_.getInt("count")).sum shouldBe 168
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }

  test("kafka spark streaming") {
    val streamingContext = new StreamingContext(sparkContext, Milliseconds(1000))
    val kafkaParams = kafkaConsumerProperties
    val kafkaTopics = Set(kafkaTopic)
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

  def createKafkaTopic(): Unit = {
    val zkClient = ZkUtils.createZkClient("localhost:2181", 10000, 10000)
    val zkUtils = ZkUtils(zkClient, isZkSecurityEnabled = false)
    val metadata = AdminUtils.fetchTopicMetadataFromZk(kafkaTopic, zkUtils)
    println(s"Kafka topic: ${metadata.topic}")
    zkClient.close()
  }

  def sendKafkaProducerMessages(): Unit = {
    val producer = new KafkaProducer[String, String](kafkaProducerProperties)
    val rdd = sparkContext.makeRDD(license)
    val wordCounts = countWords(rdd).collect()
    wordCounts.foreach { wordCount =>
      val (word, count) = wordCount
      val record = new ProducerRecord[String, String](kafkaTopic, 0, word, count.toString)
      val metadata = producer.send(record).get()
      logger.info(s"Producer -> topic: ${metadata.topic} partition: ${metadata.partition} offset: ${metadata.offset}")
      logger.info(s"Producer -> key: ${record.key} value: ${record.value}")
    }
    producer.flush()
    producer.close(3000L, TimeUnit.MILLISECONDS)
  }

  def createCassandraKeyspace(): Unit = {
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("DROP KEYSPACE IF EXISTS streaming;")
    session.execute("CREATE KEYSPACE streaming WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };")
    session.execute("CREATE TABLE streaming.words(word text PRIMARY KEY, count int);")
  }

  def countWords(rdd: RDD[String]): RDD[(String, Int)] = {
    rdd.flatMap(l => l.split("\\P{L}+")).filter(_.nonEmpty).map(_.toLowerCase).map(w => (w, 1)).reduceByKey(_ + _)
  }

  def countWords(ds: DStream[String]): DStream[(String, Int)] = {
    ds.flatMap(l => l.split("\\P{L}+")).filter(_.nonEmpty).map(_.toLowerCase).map(w => (w, 1)).reduceByKey(_ + _)
  }
}