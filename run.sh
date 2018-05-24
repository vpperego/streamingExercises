#!/bin/bash

sbt package
mkdir /tmp/spark-events
#TODO - need to test
#zookeeper-server-start.sh $KAFKA/config/zookeeper.properties &
#sleep 5
#kafka-server-start.sh $KAFKA/config/server.properties &

#Delete the topics
# kafka-topics.sh --zookeeper localhost:2181 --delete --topic actors
# kafka-topics.sh --zookeeper localhost:2181 --delete --topic titles
# kafka-topics.sh --zookeeper localhost:2181 --delete --topic actors_titles
# kafka-topics.sh --zookeeper localhost:2181 --delete --topic imdb_output
#kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic actors

rm -fr src/resources/checkpoints/*


spark-submit --master yarn-cluster --class=imdbJoin --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 target/scala-2.11/streamingscala_2.11-0.1.jar
# sleep 5
# spark-submit --master local[*] --class=producerStreaming --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 target/scala-2.11/streamingscala_2.11-0.1.jar
