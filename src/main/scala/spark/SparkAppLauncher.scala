package spark

import org.apache.spark.launcher.SparkLauncher

object SparkAppLauncher extends App {
  val javaHome = System.getenv("JAVA_HOME")
  val sparkHome = System.getenv("SPARK_HOME")

  val process = new SparkLauncher()
    .setVerbose(true)
    .setJavaHome(javaHome)
    .setSparkHome(sparkHome)
    .setAppName("sparky")
    .setMaster("local[*]")
    .setAppResource("./target/scala-2.11/spark-app-0.1.jar")
    .setMainClass("spark.SparkApp")
    .launch()
  process.waitFor()
}