package spark

import java.util.Properties

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import kafka.admin.AdminUtils
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringDecoder
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class StreamingTest extends FunSuite with BeforeAndAfterAll {
  val conf = SparkInstance.conf
  val context = SparkInstance.context
  val topic = "license"

  override protected def beforeAll(): Unit = {
    super.beforeAll
    createKafkaTopic
    sendKafkaProducerMessages
    val connector = CassandraConnector(conf)
    connector.withSessionDo { session =>
      session.execute("DROP KEYSPACE IF EXISTS test;")
      session.execute("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };")
      session.execute("CREATE TABLE test.words(word text PRIMARY KEY, count int);")
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
    wordCountDs.repartitionByCassandraReplica(keyspaceName = "test", tableName = "words", partitionsPerHost = 2)
    wordCountDs.saveToCassandra("test", "words", SomeColumns("word", "count"))
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(1000)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }

  test("streaming cassandra read") {
    import com.datastax.spark.connector.streaming._
    val streamingContext = new StreamingContext(context, Milliseconds(1000))
    val rdd = streamingContext.cassandraTable("test", "words").select("word", "count").cache
    assert(rdd.count == 95)
    assert(rdd.map(_.getInt("count")).sum == 168)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }

  test("kafka spark streaming") {
    val streamingContext = new StreamingContext(context, Milliseconds(1000))
    streamingContext.checkpoint("./target/output/test/checkpoint/kss")
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092", "auto.offset.reset" -> "smallest")
    val topics = Set(topic)
    val is: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topics)
    is.checkpoint(Milliseconds(1000))
    is.saveAsTextFiles("./target/output/test/is")
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(1000)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }

  test("kafka spark rdd") {
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val offsetRanges = Array(OffsetRange(topic = topic, partition = 0, fromOffset = 0, untilOffset = 94))
    val rdd = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](context, kafkaParams, offsetRanges)
    rdd.saveAsTextFile("./target/output/test/rdd")
  }

  private def sendKafkaProducerMessages(): Unit = {
    val props = new Properties
    props.put("metadata.broker.list", "localhost:9092")
    props.put("key.serializer.class", "kafka.serializer.StringEncoder")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")
    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    val rdd = context.makeRDD(SparkInstance.license)
    val wordCounts = countWords(rdd).collect()
    val messages = ArrayBuffer[KeyedMessage[String, String]]()
    wordCounts.foreach { wc =>
      messages += KeyedMessage[String, String](topic = topic, key = wc._1, partKey = wc._1, message = wc._2.toString)
    }
    producer.send(messages:_*)
    println(s"Sent ${messages.size} messages to Kafka topic: $topic.")
  }

  private def createKafkaTopic(): Unit = {
    val zkClient = new ZkClient("localhost:2181", 3000, 3000, ZKStringSerializer)
    val metadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient)
    metadata.partitionsMetadata.foreach(println)
    if (metadata.topic != topic) {
      AdminUtils.createTopic(zkClient, topic, 1, 1)
      println(s"Created topic: $topic")
    }
  }
}