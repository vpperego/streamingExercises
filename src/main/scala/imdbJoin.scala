
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Join between three relations in IMDB datasets
  * (https://www.imdb.com/interfaces/) :
  *   -title.basics.tsv.gz
  *   -title.principals.tsv.gz
  *   -name.basics.tsv.gz
  */
object imdbJoin {

  def run() {
    val titleSchema = StructType(
        Array(
          StructField("tconst", StringType),
          StructField("primaryTitle", StringType),
          StructField("startYear", StringType),
          StructField("endYear", StringType)
        ))

      val artistTitleSchema = StructType(
        Array(
          StructField("nconst", StringType),
          StructField("tconst", StringType)
        ))

      val  artistSchema = StructType(
        Array(
          StructField("nconst", StringType),
          StructField("primaryName", StringType),
          StructField("birthYear", StringType),
          StructField("deathYear", StringType)
        ))
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

     val titleStream = spark
      .readStream
      .option("header","true")
      .option("sep", "\t").schema(titleSchema)
      .csv("/home/vinicius/IdeaProjects/sparkExercises/src/resources/titles_small" )

    val artistTitleStream = spark
      .readStream
      .option("header","true")
      .option("sep", "\t")
      .schema(artistTitleSchema)
      .csv("/home/vinicius/IdeaProjects/sparkExercises/src/resources/artist.title_small" )

    val actorStream = spark
      .readStream
      .option("header","true")
      .option("sep", "\t")
      .schema(artistSchema)
      .csv("/home/vinicius/IdeaProjects/sparkExercises/src/resources/artists_small" )

//    actorStream.select("fooBar").map(_.

    val query = titleStream
      .join(artistTitleStream,"tconst")
      .join(actorStream,"nconst")
      .select("*")
      .writeStream

      .outputMode("append")
      .format("csv")
      //.format("console")
      .option("path","/home/vinicius/IdeaProjects/sparkExercises/src/resources/output")
        .option("header","true")

      .option("checkpointLocation",
        "/home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoint")
      .start

    query.awaitTermination()
  }

}
