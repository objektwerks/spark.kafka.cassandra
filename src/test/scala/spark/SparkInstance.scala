package spark

import java.util.Properties

import com.datastax.driver.core.Cluster
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConverters._
import scala.io.Source

object SparkInstance {
  val logger = Logger.getLogger(this.getClass)
  val sparkSession = SparkSession.builder
    .master("local[2]")
    .appName("sparky")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .config("spark.cassandra.auth.username", "cassandra")
    .config("spark.cassandra.auth.password", "cassandra")
    .getOrCreate()
  val sparkContext = sparkSession.sparkContext
  val license = Source.fromInputStream(getClass.getResourceAsStream("/license.mit")).getLines.toSeq
  val kafkaProducerProperties = loadProperties("/kafka-producer.properties")
  val kafkaConsumerProperties = toMap(loadProperties("/kafka-consumer.properties"))
  val licenseTopic = "license"

  createCassandraTestKeyspace()
  createCassandraStreamingKeyspace()
  createKafkaTopic(licenseTopic)
  sendKafkaProducerMessages(licenseTopic)

  def createCassandraTestKeyspace(): Unit = {
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("DROP KEYSPACE IF EXISTS test;")
    session.execute("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };")
    session.execute("CREATE TABLE test.kv(key text PRIMARY KEY, value int);")
    session.execute("INSERT INTO test.kv(key, value) VALUES ('k1', 1);")
    session.execute("INSERT INTO test.kv(key, value) VALUES ('k2', 2);")
    session.execute("INSERT INTO test.kv(key, value) VALUES ('k3', 3);")
    ()
  }

  def createCassandraStreamingKeyspace(): Unit = {
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("DROP KEYSPACE IF EXISTS streaming;")
    session.execute("CREATE KEYSPACE streaming WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };")
    session.execute("CREATE TABLE streaming.words(word text PRIMARY KEY, count int);")
    ()
  }

  def createKafkaTopic(topic: String): Boolean = {
    val adminClientProperties = new Properties()
    adminClientProperties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    val adminClient = AdminClient.create(adminClientProperties)
    val newTopic = new NewTopic(topic, 1, 1.toShort)
    val createTopicResult = adminClient.createTopics(List(newTopic).asJavaCollection)
    createTopicResult.values.containsKey(topic)
  }

  def sendKafkaProducerMessages(topic: String): Unit = {
    val producer = new KafkaProducer[String, String](kafkaProducerProperties)
    val rdd = sparkContext.makeRDD(license)
    val wordCounts = countWords(rdd).collect()
    wordCounts.foreach { wordCount =>
      val (word, count) = wordCount
      val record = new ProducerRecord[String, String](topic, 0, word, count.toString)
      val metadata = producer.send(record).get()
      logger.info(s"Producer -> topic: ${metadata.topic} partition: ${metadata.partition} offset: ${metadata.offset}")
      logger.info(s"Producer -> key: ${record.key} value: ${record.value}")
    }
    producer.flush()
    producer.close()
  }

  def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }

  def toMap(properties: Properties): Map[String, String] = properties.asScala.toMap

  def countWords(rdd: RDD[String]): RDD[(String, Int)] = {
    rdd
      .flatMap(line => line.split("\\W+"))
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
  }

  def countWords(ds: DStream[String]): DStream[(String, Int)] = {
    ds
      .flatMap(line => line.split("\\W+"))
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
  }
}