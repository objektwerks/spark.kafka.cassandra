Spark, Kafka and Cassandra Prototypes
-------------------------------------
>The purpose of the project is to test Spark, Spark-Cassandra Connector and Kafka-Spark Streaming features.

Homebrew
--------
>Install Homebrew on OSX. [How-To] (http://coolestguidesontheplanet.com/installing-homebrew-os-x-yosemite-10-10-package-manager-unix-apps/)

Installation
------------
>Install the following packages via Homebrew:

1. brew tap homebrew/services [Homebrew Services] (https://robots.thoughtbot.com/starting-and-stopping-background-services-with-homebrew)
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
>Test results can be viewed at ./target/output/.

1. sbt test