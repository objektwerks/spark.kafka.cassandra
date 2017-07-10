name := "spark.kafka.cassandra"
organization := "objektwerks"
version := "0.1-SNAPSHOT"
scalaVersion := "2.11.11"
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
libraryDependencies ++= {
  val sparkVersion = "2.1.1"
  Seq(
    "org.apache.spark" % "spark-core_2.11" % sparkVersion % "test",
    "org.apache.spark" % "spark-streaming_2.11" % sparkVersion % "test",
    "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "test",
    "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVersion % "test",
    "org.apache.kafka" % "kafka_2.11" % "0.11.0.0" % "test",
    "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.2" % "test",
    "org.slf4j" % "slf4j-api" % "1.7.25" % "test",
    "org.scalatest" % "scalatest_2.11" % "3.0.3" % "test"
  )
}
scalacOptions ++= Seq(
  "-language:postfixOps",
  "-language:reflectiveCalls",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-feature",
  "-Ywarn-unused-import",
  "-Ywarn-unused",
  "-Ywarn-dead-code",
  "-unchecked",
  "-deprecation",
  "-Xfatal-warnings",
  "-Xlint:missing-interpolator",
  "-Xlint"
)
javaOptions += "-server -Xss1m -Xmx2g"
fork in test := true
