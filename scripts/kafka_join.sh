#!/usr/bin/env bash

mkdir /tmp/spark-events
sbt package
rm -fr src/resources/checkpoints/
mkdir src/resources/checkpoints/

rm -fr file_join
mkdir file_join
kafka-topics.sh --zookeeper localhost:2181 --delete --topic imdb_output

spark-submit --properties-file /usr/local/spark/conf/spark-defaults.conf --driver-memory 4096M --master local[*] --class=newJoin --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 target/scala-2.11/streamingscala_2.11-0.1.jar
