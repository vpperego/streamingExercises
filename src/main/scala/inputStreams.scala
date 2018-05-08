import org.apache.spark.sql.{DataFrame, SparkSession}

object inputStreams {

  var spark: SparkSession= _;

   var titleStream:DataFrame = _  ;
  var artistTitleStream:DataFrame = _  ;
  var actorStream:DataFrame = _  ;

  //  ,artistTitleStream,actorStream;

    def startStreams(): Unit ={
      spark = SparkSession
        .builder
        .master("local[*]")
        .appName("StructuredNetworkWordCount")
        .getOrCreate()


      titleStream = spark
        .readStream
        .option("header","true")
        .option("sep", "\t").schema(schemasDefinition.titleSchema)
        .csv("/home/vinicius/IdeaProjects/sparkExercises/src/resources/titles_small" )

      artistTitleStream = spark
        .readStream
        .option("header","true")
        .option("sep", "\t")
        .schema(schemasDefinition.artistTitleSchema)
        .csv("/home/vinicius/IdeaProjects/sparkExercises/src/resources/artist.title_small" )

      actorStream = spark
        .readStream
        .option("header","true")
        .option("sep", "\t")
        .schema(schemasDefinition.artistSchema)
        .csv("/home/vinicius/IdeaProjects/sparkExercises/src/resources/artists_small" )

    }
}
