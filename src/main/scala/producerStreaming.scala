



/*
  Writes the movie dataset to the Kafka Topic named "movies"

 */
object producerStreaming extends App {
  inputStreams.startStreams()

  var q1 = inputStreams.actorStream
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("topic", "actors")
//    .option("failOnDataLoss","false")
//  .option("startingOffsets", "latest")
    .option("kafka.bootstrap.servers", inputStreams.kafkaAddress)
    .option("checkpointLocation", "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/actors")
    .start()

  q1.awaitTermination(45000)

 var q2 =  inputStreams.titleStream.selectExpr("to_json(struct(*)) AS value").
   writeStream
   .format("kafka")
   .option("topic", "titles")
   .option("failOnDataLoss","false")
//    .option("startingOffsets", "latest")
   .option("kafka.bootstrap.servers", inputStreams.kafkaAddress)
   .option("checkpointLocation", "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/titles")
   .start()


 q2.awaitTermination(45000)

 var q3 =  inputStreams.actorsTitleStream.selectExpr( "to_json(struct(*)) AS value")
    .writeStream
   .format("kafka")
   .option("topic", "actors_titles")
   .option("failOnDataLoss","false")
//    .option("startingOffsets", "latest")
   .option("kafka.bootstrap.servers", inputStreams.kafkaAddress)
   .option("checkpointLocation", "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/actors_titles")
   .start()

 q3.awaitTermination(45000)
}
