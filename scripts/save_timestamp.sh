#!/usr/bin/env bash


sbt package
mkdir /tmp/spark-events

spark-submit --master local[*] --class=consumerStreaming --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 target/scala-2.11/streamingscala_2.11-0.1.jar
