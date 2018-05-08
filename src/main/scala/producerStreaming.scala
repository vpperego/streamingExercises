import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._



/*
  Writes the movie dataset to the Kafka Topic named "movies"

 */
object producerStreaming extends App {

//  val sparkSession: SparkSession = _;
  import inputStreams.spark.implicits._


  //    val mySchema = StructType(Array(
  //      StructField("id", StringType),
  //      StructField("name", StringType),
  //      StructField("year", StringType),
  //      StructField("rating", StringType),
  //      StructField("duration", StringType)
  //    ))

  //    val streamingDataFrame =
  //      spark
  //        .readStream
  //        .schema(mySchema)
  //        .csv("sparkExercises/src/resources/")

  inputStreams.startStreams()

  inputStreams.actorStream.selectExpr("to_json(struct(*)) AS value").
    writeStream
    .format("kafka")
    .option("topic", "actors")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "/home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoint")
    .start()
    .awaitTermination()

  inputStreams.titleStream.selectExpr("to_json(struct(*)) AS value").
    writeStream
    .format("kafka")
    .option("topic", "titles")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "/home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoint")
    .start()
    .awaitTermination()

  inputStreams.artistTitleStream.selectExpr( "to_json(struct(*)) AS value").
    writeStream
    .format("kafka")
    .option("topic", "actors_titles")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "/home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoint")
    .start()
    .awaitTermination()
}