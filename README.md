Spark, Kafka and Cassandra
--------------------------
>The purpose of the project is to test Spark ( Streaming, Structured Streaming ), Kafka
>and Spark Cassandra Connector integration features.

Homebrew
--------
>Install Homebrew on OSX.

Installation
------------
>Install the following packages via Homebrew:

1. brew tap homebrew/services
2. brew install scala
3. brew install sbt
4. brew install cassandra
5. brew install kafka
6. brew install zookeeper

Services
--------
1. brew services start | stop cassandra
2. brew services start | stop zookeeper
3. brew services start | stop kafka

Test
----
1. sbt clean test

Run
---
1. sbt clean test run

Submit
------
>First create a log4j.properties file from log4j.properties.template.
>See: /usr/local/Cellar/apache-spark/2.4.3/libexec/conf/log4j.properties.template

1. sbt clean compile package
2. chmod +x submit.sh ( required only once )
3. ./submit.sh

>WARNING: Requires correct Scala version vis-a-vis Spark version to run correctly.

UI
--
1. SparkUI : localhost:4040
2. History Server UI : localhost:18080 : start-history-server.sh | stop-history-server.sh

Stop
----
1. Control-C

Logs
----
1. ./target/app.log
2. ./target/test.log

Events
------
1. ./target/spark-events

Kafka
-----
> source-kssc-topic, license

* kafka-topics --zookeeper localhost:2181 --list
* kafka-topics --zookeeper localhost:2181 --describe --topic license
* kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic license --time -1
* kafka-consumer-groups --bootstrap-server localhost:9092 --group objektwerks-group --describe
* kafka-topics --zookeeper localhost:2181 --delete --topic license
* kafka-consumer-groups --bootstrap-server localhost:9092 --list
* kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group objektwerks-group