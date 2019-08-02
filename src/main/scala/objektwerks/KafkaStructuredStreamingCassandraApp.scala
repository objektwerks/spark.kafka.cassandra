package objektwerks

import com.datastax.driver.core.Cluster
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaStructuredStreamingCassandraApp extends App {
  val conf = ConfigFactory.load("app.conf").getConfig("app")
  val (kafkaBootstrapServers, urls) = ("kafka.bootstrap.servers", conf.getString("kafka-bootstrap-servers"))
  val sourceTopic = conf.getString("source-topic")

  val keyValueStructType = new StructType()
    .add(name = "key", dataType = StringType, nullable = false)
    .add(name = "value", dataType = StringType, nullable = false)

  val cluster = Cluster.builder.addContactPoint("127.0.0.1").build()
  val session = cluster.connect()
  session.execute("DROP KEYSPACE IF EXISTS kssc;")
  session.execute("CREATE KEYSPACE kssc WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };")
  session.execute("CREATE TABLE kssc.keyvalue(key text PRIMARY KEY, value text);")

  val sparkSession = SparkSession.builder
    .master(conf.getString("master"))
    .appName(conf.getString("name"))
    .config("spark.eventLog.enabled", conf.getBoolean("spark.eventLog.enabled"))
    .config("spark.eventLog.dir", conf.getString("spark.eventLog.dir"))
    .config("spark.cassandra.connection.host", conf.getString("cassandra-host"))
    .config("spark.cassandra.auth.username", conf.getString("cassandra-username"))
    .config("spark.cassandra.auth.password", conf.getString("cassandra-password"))
    .getOrCreate()
  println("Initialized Spark KafkaStructuredStreamingCassandraApp. Press Ctrl C to terminate.")

  sys.addShutdownHook {
    sparkSession.stop
    println("Terminated Spark KafkaStructuredStreamingCassandraApp.")
  }

  sparkSession
    .readStream
    .format("kafka")
    .option(kafkaBootstrapServers, urls)
    .option("subscribe", s"$sourceTopic")
    .load
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)")
    .writeStream
    .outputMode(OutputMode.Append)
    .format("console")
    .start
    .awaitTermination

  sparkSession
    .readStream
    .option("basePath", conf.getString("key-value-json-path"))
    .schema(keyValueStructType)
    .json(conf.getString("key-value-json-path"))
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .format("kafka")
    .option(kafkaBootstrapServers, urls)
    .option("topic", sourceTopic)
    .option("checkpointLocation", conf.getString("source-topic-checkpoint-location"))
    .start
    .awaitTermination

  import org.apache.spark.sql.functions._

  val sourceTopicToCassandraTable = sparkSession
    .readStream
    .format("kafka")
    .option(kafkaBootstrapServers, urls)
    .option("subscribe", sourceTopic)
    .load
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .withColumn("value", upper(col("value")))
    .writeStream
    .foreachBatch { (batchDF: DataFrame, _: Long) =>
      batchDF
        .write
        // .cassandraFormat("keyvalue", "kssc") // doesn't exist - yet.;)
        .option("cluster", "Test Cluster")
        .mode("append")
        .save()
    }
    .outputMode("update")
    .start
    .awaitTermination
}