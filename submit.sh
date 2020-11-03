#!/bin/sh
spark-submit \
  --class objektwerks.KafkaStructuredStreamingCassandraApp \
  --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.7,org.apache.kafka:kafka_2.12:2.6.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,com.typesafe:config:1.4.0 \
  ./target/scala-2.12/spark-kafka-cassandra_2.12-0.1-SNAPSHOT.jar