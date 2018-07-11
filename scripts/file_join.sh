#!/usr/bin/env bash
sbt package

mkdir /tmp/spark-events

rm -fr src/resources/checkpoints/
mkdir src/resources/checkpoints/

rm -fr file_join
mkdir file_join

spark-submit --properties-file /usr/local/spark/conf/spark-defaults.conf --driver-memory 8192M --master local[*] --class=fileJoin --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 target/scala-2.11/streamingscala_2.11-0.1.jar
