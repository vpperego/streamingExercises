import org.apache.spark.sql.{DataFrame, SparkSession}

object inputStreams {

  val spark: SparkSession= SparkSession
    .builder
    .master("local[*]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  var titleStream:DataFrame = _  ;
  var actorsTitleStream:DataFrame = _  ;
  var actorStream:DataFrame = _  ;

  var titleKafkaStream:DataFrame = _  ;
  var actorsTitleKafkaStream:DataFrame = _  ;
  var actorKafkaStream:DataFrame = _  ;

  def startStreams(): Unit ={

    actorStream = spark
      .readStream
      .option("header","true")
      .option("sep", "\t")
      .schema(schemasDefinition.actorSchema)
      .csv("/home/vinicius/IdeaProjects/sparkExercises/src/resources/artists_small" )

    titleStream = spark
      .readStream
      .option("header","true")
      .option("sep", "\t")
      .schema(schemasDefinition.titleSchema)
      .csv("/home/vinicius/IdeaProjects/sparkExercises/src/resources/titles_small" )

    actorsTitleStream = spark
      .readStream
      .option("header","true")
      .option("sep", "\t")
      .schema(schemasDefinition.actorTitleSchema)
      .csv("/home/vinicius/IdeaProjects/sparkExercises/src/resources/artist.title_small" )

  }

  def startKafkaStreams(): Unit ={


    titleKafkaStream = spark
      .readStream
      .format("kafka")
      .option("failOnDataLoss","false")

      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "titles")
      .option("startingOffsets", "earliest")

      .load()

    actorsTitleKafkaStream = spark
      .readStream
      .option("failOnDataLoss","false")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "actors_titles")
      .option("startingOffsets", "earliest")

      .load()

    actorKafkaStream = spark
      .readStream
      .format("kafka")
      .option("failOnDataLoss","false")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "actors")
      .option("startingOffsets", "earliest")

      .load()
  }

}
