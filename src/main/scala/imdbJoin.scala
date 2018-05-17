
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
      .as[String]
      .select(from_json($"value", schemasDefinition.titleSchema).as("bar"))
      .select("bar.*")
      .join(
        inputStreams.actorsTitleKafkaStream
          .selectExpr("CAST(value AS STRING)")
          .as[String]
          .select(from_json($"value", schemasDefinition.actorTitleSchema).as("bar2"))
          .select("bar2.*")
        //      right
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
      .option("failOnDataLoss","false")
      .option("topic", "imdb_output")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "/home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/imdbJoin")
      .start

  query.awaitTermination(10000)

}
