name := "spark.kafka.cassandra"
organization := "objektwerks"
version := "0.1"
scalaVersion := "2.11.12"
libraryDependencies ++= {
  val sparkVersion = "2.4.4"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
    "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.1",
    "com.typesafe" % "config" % "1.3.4",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % Test,
    "org.scalatest" %% "scalatest" % "3.0.8" % Test
  )
}