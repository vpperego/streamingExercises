#!/usr/bin/env bash

mkdir /tmp/spark-events
sbt package
rm -fr src/resources/checkpoints/
mkdir src/resources/checkpoints/

kafka-topics.sh --zookeeper localhost:2181 --delete --topic ratings
kafka-topics.sh --zookeeper localhost:2181 --delete --topic titles


spark-submit --properties-file /usr/local/spark/conf/spark-defaults.conf --driver-memory 4096M --master local[*] --class=producerStreaming --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 target/scala-2.11/streamingscala_2.11-0.1.jar
