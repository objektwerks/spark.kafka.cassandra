name := "objektwerks.spark"
version := "0.1"
scalaVersion := "2.11.7"
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

libraryDependencies ++= {
  val sparkVersion = "1.4.1"
  Seq(
    "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided",
    "org.apache.spark" % "spark-streaming_2.11" % sparkVersion % "provided",
    "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided",
    "org.apache.spark" % "spark-mllib_2.11" % sparkVersion % "provided",
    "org.apache.spark" % "spark-graphx_2.11" % sparkVersion % "provided",
    "org.apache.spark" % "spark-streaming-kafka_2.11" % sparkVersion % "provided",
    "org.apache.kafka" % "kafka_2.11" % "0.8.2.1" % "provided",
    "com.datastax.spark" % "spark-cassandra-connector_2.11" % "1.4.0-M1" % "provided",
    "org.slf4j" % "slf4j-api" % "1.7.12" % "test",
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

fork in test := false

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

import java.util.jar.Attributes.Name._
packageOptions in (Compile, packageBin) += {
  Package.ManifestAttributes(MAIN_CLASS -> "spark.SparkAppLauncher")
}
exportJars := true
artifactName := { (s: ScalaVersion, m: ModuleID, a: Artifact) => "spark-app-0.1.jar" }

assemblyMergeStrategy in assembly := {
  case "license.mit" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
test in assembly := {}
mainClass in assembly := Some("spark.SparkAppLauncher")
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "spark-app-0.1.jar"
