import org.apache.spark.sql.functions.{from_json, _};
object newJoin extends App{

  inputStreams.startKafkaStreams()
  import inputStreams.spark.implicits._

// a.printSchema
// b.printSchema
  val query =
       a
       .join(b ,a.col("tconst") === b.col("_tconst"))
       .selectExpr("to_json(struct(*)) AS value")

      .writeStream
      .format("kafka")
      .option("topic", "imdb_output2")
      .option("kafka.bootstrap.servers", "localhost:9093")
      .option("checkpointLocation", "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/ImdbJoinCheckpoint")
      .start
  var a = inputStreams.ratingKafkaStream
            .selectExpr("CAST(value AS STRING)")
            .as[(String)]
            .select(from_json($"value", schemasDefinition.ratingSchema).as("data"))
            .withColumn("time_stamp", lit(current_timestamp()))
            .select($"data.*",$"time_stamp");
var b = inputStreams.titleKafkaStream
    .selectExpr("CAST(value AS STRING)")
    .as[(String)]
    .select(from_json($"value", schemasDefinition.titleSchema).as("data2"))
    .withColumn("time_stamp2", lit(current_timestamp()))
    .select($"data2.*",$"time_stamp2")
    // .select($"data2.tconst".as("_tconst"));
    .withColumnRenamed("tconst", "_tconst");
     //  .writeStream
     // .format("csv")
     // .option("path", "file:////home/vinicius/IdeaProjects/sparkExercises/output2")
     // .option("checkpointLocation","file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/test")
     // .start()
       query.awaitTermination()
}
