Spark, Kafka and Cassandra Prototypes
-------------------------------------
>The purpose of the project is to test Spark, Spark-Cassandra Connector and Kafka-Spark Streaming features using
 Scala 2.11.7. And, yes, this will hurt.;) WARNING: Spark-Cassandra-Connector 1.5-M* is incompatible with Spark 1.5

***

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
5. brew install zookeeper
6. brew install gradle

Environment
-----------
>The following environment variables should be in your .bash_profile

- export JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.8.0_72.jdk/Contents/Home"
- export CASSANDRA_HOME="/usr/local/Cellar/cassandra/2.1.6/libexec"
- export KAFKA_HOME="/Users/javawerks/workspace/apache/kafka"
- export SCALA_VERSION="2.11.7"
- export SCALA_BINARY_VERSION="2.11"
- export SCALA_LIB="/usr/local/Cellar/scala/2.11.7/libexec/lib"
- export SPARK_SCALA_VERSION="2.11"
- export SPARK_HOME="/Users/javawerks/workspace/apache/spark"
- export SPARK_LAUNCHER="$SPARK_HOME/launcher/target"
- export PATH=${JAVA_HOME}/bin:${CASSANDRA_HOME}/bin:${KAFKA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:/usr/local/bin:/usr/local/sbin:$PATH

Spark
-----
>Install Spark release ( branch-1.4.1 ) from github. The brew and apache distros are Scala 2.10 oriented.

1. git clone --branch branch-1.4.1 https://github.com/apache/spark/
2. dev/change-scala-version.sh 2.11
3. mvn -Pyarn -Phadoop-2.6 -Dscala-2.11 -DskipTests clean package

>See [Scala 2.11 Support Instructions] (http://spark.apache.org/docs/latest/building-spark.html#building-for-scala-211)

Kafka
-----
>Install Kafka from github. The brew and apache distros are Scala 2.10 oriented. *NOTE:* Scala 2.11 distro is now provided. If so follow only steps 4 and 5.

1. git clone --branch branch-0.8.2 https://github.com/apache/kafka
2. gradle
3. gradle -PscalaVersion=2.11.7 jar
4. edit $KAFKA_HOME/config/server.properties log.dirs=$KAFKA_HOME/logs
5. mkdir $KAFKA_HOME/logs

>You can also edit scalaVersion=2.11.7 in gradle.properties. Provide a fully qualified path for Kafka server.properties log.dirs

Configuration
-------------
1. log4j.properties
2. spark.properties

>The test versions are fine as is. The main version of log4j.properties is only used within your IDE. To enable Spark
logging, copy main/resources/log4j.properties to $SPARK_HOME/conf ( where you will see a tempalte version ). Tune as required.

Services
--------
>Start:

1. brew services start cassandra
2. brew services start zookeeper
3. nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > $KAFKA_HOME/logs/kafka.nohup&

>Homebrew uses launchctl/launchd to ensure services are booted up on each system start, unless you stop them. Take a
 look at the apache.kafka.plist in this project. Homebrew service plist files are ideally stored in ~/Library/LaunchAgents
 See [launchd] (http://launchd.info) for details. Ideally you should use Xcode to edited a plist.

>Stop:

1. brew services stop cassandra
2. brew services stop zookeeper
3. $KAFKA_HOME/bin/kafka-server-stop.sh

Tests
-----
>Test results can be viewed at ./target/output/test. See the Output section below.

1. sbt test

Logging
-------
>Spark depends on log4j. Providing a log4j.properties file works great during testing and lauching of a spark app within an IDE.
Spark, however, ignores a jar-based log4j.properties file whether a job is run by spark-submit.sh or SparkLauncher. You have to
place a log4j.properties file in the $SPARK_HOME/conf directory. A log4j.properties.template file is provided in the same directory.

Output
------
>Output is directed to these directories:

1. ./target/output/test
2. ./target/output/main

Questions
---------
Q1. Does the Spark-Cassandra Connector co-locate code and data?

A1. Yes. Data locality is achieved through intelligent connection and cluster topology management and awareness by
    Spark, Cassandra and the Spark-Cassandra Connector. Spark and Cassandra partition tuning is critical. See these
    links for details:

1. [Spark-Cassandra Connector Connection Management] (https://github.com/datastax/spark-cassandra-connector/blob/master/doc/1_connecting.md)
2. [Spark-Cassandra Connector Best Practices] (http://presentations2015.s3.amazonaws.com/26_presentation.pdf)
3. [How Spark-Cassandra Connector Reads Data] (https://academy.datastax.com/demos/how-spark-cassandra-connector-reads-data)
4. [Cassandra Keys] (http://intellidzine.blogspot.com/2014/01/cassandra-data-modelling-primary-keys.html)
5. [Optimizing for Spark/Cassandra Data Locality] (https://www.youtube.com/watch?v=ikCzILOpYvA)
6. [Spark Streaming, Kafka, Cassandra and Scala] (https://www.parleys.com/tutorial/lambda-architecture-spark-streaming-kafka-cassandra-akka-scala)
7. [Reliable Spark Stream] (https://www.youtube.com/watch?v=GF2IzQajRO0)
8. [Getting Started with Spark & Cassandra] (https://www.youtube.com/watch?v=_gFgU3phogQ)

References
----------
1. [Spark] (https://spark.apache.org/docs/latest/)
2. [Spark-Cassandra Connector] (https://github.com/datastax/spark-cassandra-connector)
3. [Cassandra] (https://cassandra.apache.org)
4. [Datastax] (http://www.datastax.com)
5. [Kafka] (http://kafka.apache.org)

Books
-----
1. [Learning Spark] (http://shop.oreilly.com/product/0636920028512.do)
2. [Advanced Analystics with Spark] (http://shop.oreilly.com/product/0636920035091.do)

Articles
--------
1. [Exactly-once Spark Streaming from Apache Kafka] (http://blog.cloudera.com/blog/2015/03/exactly-once-spark-streaming-from-apache-kafka/)
2. [Working with Spark] (http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/)
3. [Spark Job Tuning, Part I] (http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-1/)
4. [Spark Job Tuning, Part II] (http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/)

Issues
------
1. [Spark Jira] (https://issues.apache.org/jira/browse/spark/?selectedTab=com.atlassian.jira.jira-projects-plugin:issues-panel)
2. [Cassandra Jira] (https://issues.apache.org/jira/browse/CASSANDRA/?selectedTab=com.atlassian.jira.jira-projects-plugin:issues-panel)
3. [Kafka Jira] (https://issues.apache.org/jira/browse/KAFKA/?selectedTab=com.atlassian.jira.jira-projects-plugin:issues-panel)
4. [ZooKeeper] (https://issues.apache.org/jira/browse/ZOOKEEPER/?selectedTab=com.atlassian.jira.jira-projects-plugin:issues-panel)

Kafka Notes
-----------
- kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic license
- kafka-topics.sh --zookeeper localhost:2181 --list

Spark Notes
-----------
>Logical Flow

- Driver 1 --- Task --- > * Executor * --- Result --- > * Store
- Driver 1 <--- Task > | < Result --- > * Executor

>Logical Architecture

- Driver 1 ---> 1 ClusterManager 1 ---> * Worker

>Scenarios

- A Driver executes in a JVM and composes a SparkContext ( optional StreamingContext, SqlContext, etc... ).
- On Driver failure, Checkpointing must have been configured and used for a successful auto-restart.
- A Cluster Manager (Standalone, YARN, Mesos ) executes in a JVM or native process and interacts with a Driver and managed Workers.
- A Worker composes an Executor, Cache and Tasks.
- An Executor invokes Tasks to work on data blocks, which are replicated across Executors for failover.
- Data guarantees include: (1) at least once with Receivers; and (2) Exactly once with DirectStreams.
- On Executor failure, the Driver resends Tasks to another Executor.
- A Driver creates Jobs, schedules Tasks, sends Tasks and retrieves Task results via a Cluster Manager and Worker Nodes.
- A Job composes a set of Stages, which composes a DAG of RDDs, defined by a set of chained Transformations, terminated by an Action.
- A Transformation yields an RDD. Transformations are chainable.
- An Action, a terminal operation on a chain of Transformations, yields a result.
- An RDD composes Partitions. Partitions are tunable.
- A Task executes Transformation logic on a Partition.
- A DStream composes a set of RDDs, which can be analyzed in micro-batches across a measured, finite windows of time.