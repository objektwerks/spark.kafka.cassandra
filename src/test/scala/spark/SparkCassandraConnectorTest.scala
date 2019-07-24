package spark

import com.datastax.driver.core.Cluster
import com.datastax.spark.connector._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}
import scala.language.postfixOps

case class KeyValue(key: String, value: Int)

class SparkCassandraConnectorTest extends FunSuite with BeforeAndAfterAll with Matchers {
  val sparkContext = SparkInstance.sparkSession.sparkContext

  override protected def beforeAll(): Unit = {
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

  test("read") {
    val rdd = sparkContext.cassandraTable(keyspace = "test", table = "kv").cache
    rdd.repartitionByCassandraReplica(keyspaceName = "test", tableName = "kv", partitionsPerHost = 2)
    rdd.keyspaceName shouldBe "test"
    rdd.tableName shouldBe "kv"
    rdd.selectedColumnNames shouldBe Seq("key", "value")
    rdd.count shouldBe 3
    rdd.map(_.getInt("value")).sum shouldBe 6
  }

  test("write") {
    val seq = sparkContext.parallelize(Seq(("k4", 4), ("k5", 5), ("k6", 6)))
    seq.saveToCassandra("test", "kv", SomeColumns("key", "value"))
    val rdd = sparkContext.cassandraTable(keyspace = "test", table = "kv").cache
    rdd.repartitionByCassandraReplica(keyspaceName = "test", tableName = "kv", partitionsPerHost = 2)
    rdd.count shouldBe 6
    rdd.map(_.getInt("value")).sum shouldBe 21
  }

  test("tuples") {
    val tuples = sparkContext.cassandraTable[(String, Int)](keyspace = "test", table = "kv").select("key", "value").collect
    tuples.map(_._2).sum shouldBe 21
  }

  test("case class") {
    val keyValues = sparkContext.cassandraTable[KeyValue](keyspace = "test", table = "kv").collect
    keyValues.map(_.value).sum shouldBe 21
  }

  test("case class future") {
    implicit val ec = ExecutionContext.global
    val future = sparkContext.cassandraTable[KeyValue](keyspace = "test", table = "kv").collectAsync
    future onComplete {
      case Success(keyValues) => keyValues.map(_.value).sum shouldBe 21
      case Failure(failure) => throw failure
    }
    Await.result(future, 3 seconds)
  }
}