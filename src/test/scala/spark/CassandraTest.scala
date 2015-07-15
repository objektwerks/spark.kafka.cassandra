package spark

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

case class KeyValue(key: String, value: Int)

class CassandraTest extends FunSuite with BeforeAndAfterAll {
  val conf = SparkInstance.conf
  val context = SparkInstance.context
  val sqlContext = new CassandraSQLContext(context)

  override protected def beforeAll(): Unit = {
    super.beforeAll
    val connector = CassandraConnector(conf)
    connector.withSessionDo { session =>
      session.execute("DROP KEYSPACE IF EXISTS test;")
      session.execute("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };")
      session.execute("CREATE TABLE test.kv(key text PRIMARY KEY, value int);")
      session.execute("INSERT INTO test.kv(key, value) VALUES ('k1', 1);")
      session.execute("INSERT INTO test.kv(key, value) VALUES ('k2', 2);")
      session.execute("INSERT INTO test.kv(key, value) VALUES ('k3', 3);")
    }
  }

  test("read") {
    val rdd = context.cassandraTable(keyspace = "test", table = "kv").cache
    rdd.repartitionByCassandraReplica(keyspaceName = "test", tableName = "kv", partitionsPerHost = 2)
    assert(rdd.keyspaceName == "test")
    assert(rdd.tableName == "kv")
    assert(rdd.selectedColumnNames == Seq("key", "value"))
    assert(rdd.count == 3)
    assert(rdd.first.getInt("value") == 1)
    assert(rdd.map(_.getInt("value")).sum == 6)
  }

  test("write") {
    val seq = context.parallelize(Seq(("k4", 4), ("k5", 5), ("k6", 6)))
    seq.saveToCassandra("test", "kv", SomeColumns("key", "value"))
    val rdd = context.cassandraTable(keyspace = "test", table = "kv").cache
    rdd.repartitionByCassandraReplica(keyspaceName = "test", tableName = "kv", partitionsPerHost = 2)
    assert(rdd.count == 6)
    assert(rdd.map(_.getInt("value")).sum == 21)
  }

  test("tuples") {
    val tuples = context.cassandraTable[(String, Int)](keyspace = "test", table = "kv").select("key", "value").collect
    assert(tuples.map(_._2).sum == 21)
  }

  test("case class") {
    val keyValues = context.cassandraTable[KeyValue](keyspace = "test", table = "kv").collect
    assert(keyValues.map(_.value).sum == 21)
  }

  test("case class future") {
    implicit val ec = ExecutionContext.global
    val future = context.cassandraTable[KeyValue](keyspace = "test", table = "kv").collectAsync
    future onComplete {
      case Success(keyValues) => assert(keyValues.map(_.value).sum == 21)
      case Failure(failure) => throw failure
    }
    Await.result(future, 3 seconds)
  }

  test("cql select") {
    val df = sqlContext.sql("select * from test.kv")
    assert(df.count == 6)
    val max = df.agg("value" -> "max").first
    assert(max.getInt(0) == 6)
    val sum = df.agg("value" -> "sum").first
    assert(sum.getLong(0) == 21)
  }

  test("cql select where") {
    val df = sqlContext.sql("select value from test.kv where value > 3")
    assert(df.count == 3)
    val max = df.agg("value" -> "max").first
    assert(max.getInt(0) == 6)
    val sum = df.agg("value" -> "sum").first
    assert(sum.getLong(0) == 15)
  }
}
