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