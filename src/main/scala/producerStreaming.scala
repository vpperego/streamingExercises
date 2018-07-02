/*
  Writes the movie dataset to the Kafka Topic named "movies"

 */
import org.apache.spark.sql.functions._
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

  q1.awaitTermination(60000)

  var ratingQuery =  inputStreams.ratingStream.selectExpr("to_json(struct(*)) AS value")
//    .withColumn("timestamp", lit(System.nanoTime()))
    .withColumn("whatIsTime", current_timestamp())
    .writeStream
   .format("kafka")
   .option("topic", "ratings")
   .option("failOnDataLoss","false")
   //    .option("startingOffsets", "latest")
   .option("kafka.bootstrap.servers", inputStreams.kafkaAddress)
   .option("checkpointLocation", "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/ratings")
   .start()


   ratingQuery.awaitTermination(25000)

  var titleQuery =  inputStreams.titleStream
    .selectExpr("CAST(tconst AS STRING) AS key", "to_json(struct(*)) AS value").
    writeStream
    .format("kafka")
    .option("topic", "titles")
    .option("failOnDataLoss","false")
 //    .option("startingOffsets", "latest")
    .option("kafka.bootstrap.servers", inputStreams.kafkaAddress)
    .option("checkpointLocation", "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/titles")
    .start()


   titleQuery.awaitTermination(40000)

 var q3 =  inputStreams.actorsTitleStream.selectExpr( "to_json(struct(*)) AS value")
    .writeStream
   .format("kafka")
   .option("topic", "actors_titles")
   .option("failOnDataLoss","false")
//    .option("startingOffsets", "latest")
   .option("kafka.bootstrap.servers", inputStreams.kafkaAddress)
   .option("checkpointLocation", "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/actors_titles")
   .start()

 q3.awaitTermination(60000)
}
