import org.apache.spark.sql.functions.from_json;
object newJoin extends App{

  inputStreams.startJoinStreams()
  import inputStreams.spark.implicits._

// a.printSchema
// b.printSchema
  val query = left
      .join(right ,left.col("tconst") === right.col("_tconst"))
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("topic", "imdb_output2")
      .option("kafka.bootstrap.servers", "localhost:9093")
      .option("checkpointLocation", "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/ImdbJoinCheckpoint")
      .start
  var left = inputStreams.ratingKafkaStream
            .selectExpr("CAST(value AS STRING)")
            .as[(String)]
            .select(from_json($"value", schemasDefinition.ratingSchema).as("data"))
            .select($"data.*")
  var right = inputStreams.titleKafkaStream
            .selectExpr("CAST(value AS STRING)")
            .as[(String)]
            .select(from_json($"value", schemasDefinition.titleSchema).as("data2"))
            .select($"data2.*")
            .withColumnRenamed("tconst", "_tconst")
     //  .writeStream
     // .format("csv")
     // .option("path", "file:////home/vinicius/IdeaProjects/sparkExercises/output2")
     // .option("checkpointLocation","file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/test")
     // .start()
       query.awaitTermination()
}
