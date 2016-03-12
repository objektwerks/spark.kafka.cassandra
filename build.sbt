name := "objektwerks.spark.kafka.cassandra"
version := "0.1"
scalaVersion := "2.11.8"
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
libraryDependencies ++= {
  val sparkVersion = "1.4.1"
  Seq(
    "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided",
    "org.apache.spark" % "spark-streaming_2.11" % sparkVersion % "provided",
    "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided",
    "org.apache.spark" % "spark-streaming-kafka_2.11" % sparkVersion % "provided",
    "org.apache.kafka" % "kafka_2.11" % "0.8.2.2" % "provided",
    "com.datastax.spark" % "spark-cassandra-connector_2.11" % "1.4.1" % "provided",
    "org.slf4j" % "slf4j-api" % "1.7.13" % "test",
    "org.scalatest" % "scalatest_2.11" % "2.2.5" % "test"
  )
}
scalacOptions ++= Seq(
  "-language:postfixOps",
  "-language:implicitConversions",
  "-language:reflectiveCalls",
  "-language:higherKinds",
  "-feature",
  "-unchecked",
  "-deprecation",
  "-Xlint",
  "-Xfatal-warnings"
)
javaOptions += "-server -Xss1m -Xmx2g"
fork in test := true
