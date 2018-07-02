
import org.apache.spark.sql.functions.from_json

/**
  * Join between three relations in IMDB datasets
  * (https://www.imdb.com/interfaces/) :
  *   -title.basics.tsv.gz
  *   -title.principals.tsv.gz
  *   -name.basics.tsv.gz
  */
object imdbJoin extends App{

  inputStreams.startKafkaStreams()
  import inputStreams.spark.implicits._

  val query =
    inputStreams.titleKafkaStream
      .selectExpr("CAST(value AS STRING)")
      .as[(String)]
      .select(from_json($"value", schemasDefinition.titleSchema).as("data"))
      .select("data.*")
        .selectExpr("CAST(value AS STRING)").as[String].select(from_json($"value", schemasDefinition.titleSchema).as("data")).select("data.*")

            .join(
        inputStreams.ratingKafkaStream
          .selectExpr("CAST(value AS STRING)")
          .as[String]
          .select(from_json($"value", schemasDefinition.ratingSchema).as("bar2"))
          .select("bar2.*")
        ,"tconst")
      .join(
        inputStreams.actorKafkaStream
          .selectExpr("CAST(value AS STRING)")
          .as[String]
          .select(from_json($"value", schemasDefinition.actorSchema).as("bar3"))
          .select("bar3.*")
        ,"nconst")
      .selectExpr( "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("topic", "imdb_output")
      .option("kafka.bootstrap.servers", inputStreams.kafkaAddress)
//      .option("checkpointLocation", "file:///tmp/ImdbJoinCheckpoint")
      .option("checkpointLocation", "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/ImdbJoinCheckpoint")

      // .option("checkpointLocation", "hdfs://dbis-expsrv4.informatik.uni-kl.de:8020/")
      .start

  query.awaitTermination()
}
