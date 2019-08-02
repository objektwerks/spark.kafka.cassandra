Spark, Kafka and Cassandra Prototypes
-------------------------------------
>The purpose of the project is to test Spark, Spark-Cassandra Connector and Kafka-Spark Streaming features.

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
>Start:

1. brew services start cassandra
2. brew services start zookeeper
3. brew services start kafka


>Stop:

1. brew services stop cassandra
2. brew services stop kafka
3. brew services stop zookeeper

Test
----
1. sbt clean test

Run
---
>KafkaStructuredStreamingCassandraApp is still a work in progress due to foreachBatch issues with Cassandra.

1. sbt clean test run

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