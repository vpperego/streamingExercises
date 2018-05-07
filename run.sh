#!/bin/bash
rm -fr src/resources/output/
rm -fr src/resources/checkpoint/

#sbt package
#spark-submit target/scala-2.11/streamingscala_2.11-0.1.jar
sbt run
