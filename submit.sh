#!/bin/sh
spark-submit \
  --class objektwerks.KafkaStructuredStreamingCassandraApp \
  --master local[2] \
  --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.3,org.apache.kafka:kafka_2.11:2.3.0,com.datastax.spark:spark-cassandra-connector_2.11:2.4.1,com.typesafe:config:1.3.4 \
  ./target/scala-2.11/spark-kafka-cassandra_2.11-0.1.jar