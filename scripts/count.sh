#!/usr/bin/env bash
#echo " Artists - file"
#wc -l src/resources/artists/artist.tsv
echo " Titles- file"
wc -l src/resources/titles/novo.tsv
#echo " Artists.Titles - file"
#wc -l src/resources/artist.title/artist.title.tsv
echo " Ratings - file"
wc -l src/resources/ratings/nov.tsv

echo  "Ratings - kafka"
kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic ratings --time -1 --offsets 1
#echo  "Artists - kafka"
#kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic actors --time -1 --offsets 1
echo " Titles- kafka"
kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic titles --time -1 --offsets 1
#echo " Artists.Titles- kafka"
#kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic actors_titles --time -1 --offsets 1
echo " IMDB Output - kafka"
kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic imdb_output --time -1 --offsets 1
