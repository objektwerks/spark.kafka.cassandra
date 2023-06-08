name := "spark.kafka.cassandra"
organization := "objektwerks"
version := "0.1-SNAPSHOT"
scalaVersion := "2.12.18"
libraryDependencies ++= {
  val sparkVersion = "2.4.8"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
    "org.apache.kafka" %% "kafka" % "2.8.2",
    "com.datastax.spark" %% "spark-cassandra-connector" % "3.3.0",
    "com.typesafe" % "config" % "1.4.2",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % Test,
    "org.scalatest" %% "scalatest" % "3.2.16" % Test
  )
}
