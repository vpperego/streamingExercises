#!/bin/bash
rm -fr src/resources/output/
rm -fr src/resources/checkpoint/

sbt package

#TODO - need to test
#zookeeper-server-start.sh config/zookeeper.properties
#kafka-server-start.sh config/server.properties

#Create the topics

#kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic actors
#kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic titles
#kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic actors_titles
#kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic imdb_output


#spark-submit --class=imdbJoin target/scala-2.11/streamingscala_2.11-0.1.jar &
#sleep 20
spark-submit --class=producerStreaming --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 target/scala-2.11/streamingscala_2.11-0.1.jar