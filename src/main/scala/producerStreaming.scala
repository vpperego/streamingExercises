;

/*
  Writes the movie dataset to the Kafka Topic named "movies"

 */
object producerStreaming extends App {
  inputStreams.startStreams()

//  var q1 = inputStreams.actorStream
//    .selectExpr("to_json(struct(*)) AS value")
//    .writeStream
//    .format("kafka")
//    .option("topic", "actors")
////    .option("failOnDataLoss","false")
////  .option("startingOffsets", "latest")
//    .option("kafka.bootstrap.servers", inputStreams.kafkaAddress)
//    .option("checkpointLocation", "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/actors")
//    .start()
//
//  q1.awaitTermination(60000)

//  var ratingQuery =  inputStreams.ratingStream
//  .selectExpr("to_json(struct(*)) AS value")
//     .writeStream
//   .format("kafka")
//   .option("topic", "ratings")
//   .option("failOnDataLoss","false")
//   .option("kafka.bootstrap.servers", inputStreams.kafkaAddress)
//   .option("checkpointLocation", "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/ratings")
//   .start()
//
//
//   ratingQuery.awaitTermination(25000)

//  var titleQuery =  inputStreams.titleStream
//    .selectExpr("to_json(struct(*)) AS value")
//    writeStream
//    .format("kafka")
//    .option("topic", "titles")
//    .option("failOnDataLoss","false")
//    .option("kafka.bootstrap.servers", inputStreams.kafkaAddress)
//    .option("checkpointLocation", "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/titles")
//    .start()
//    .withColumn("timestamp",current_timestamp())
//    .select("*")
//    .writeStream
//    .format("csv")
//    .option("path", "file:////home/vinicius/IdeaProjects/sparkExercises/output2")
//    .option("checkpointLocation","file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/test")
//    .start()
//    .awaitTermination(10000)

//   titleQuery.awaitTermination(25000)

}
