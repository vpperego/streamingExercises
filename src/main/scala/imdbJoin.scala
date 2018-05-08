
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Join between three relations in IMDB datasets
  * (https://www.imdb.com/interfaces/) :
  *   -title.basics.tsv.gz
  *   -title.principals.tsv.gz
  *   -name.basics.tsv.gz
  */
object imdbJoin extends App{

  inputStreams.startStreams()

  val query = inputStreams.titleStream
    .join(inputStreams.artistTitleStream,"tconst")
    .join(inputStreams.actorStream,"nconst")
    .select("*")
    .writeStream

//    .outputMode("append")
//    .format("csv")
//    .option("path","/home/vinicius/IdeaProjects/sparkExercises/src/resources/output")
//    .option("header","true")
//
//    .option("checkpointLocation",
//      "/home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoint")
    .format("kafka")
    .option("topic", "imdb_output")
    .option("kafka.bootstrap.servers", "localhost:9092")
//    .option("checkpointLocation", "/sparkExercises/src/checkpoint")
    .start

  query.awaitTermination()

}
