



/*
  Writes the movie dataset to the Kafka Topic named "movies"

 */
object producerStreaming extends App {
  inputStreams.startStreams()

  inputStreams.actorStream.selectExpr("to_json(struct(*)) AS value").
    writeStream
    .format("kafka")
    .option("topic", "actors")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "/home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/actors")
    .start()
    .awaitTermination(5000)

  inputStreams.titleStream.selectExpr("to_json(struct(*)) AS value").
    writeStream
    .format("kafka")
    .option("topic", "titles")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "/home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/titles")
    .start()
    .awaitTermination(5000)

  inputStreams.actorsTitleStream.selectExpr( "to_json(struct(*)) AS value").
    writeStream
    .format("kafka")
    .option("topic", "actors_titles")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "/home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/actors_titles")
    .start()
    .awaitTermination(5000)
}