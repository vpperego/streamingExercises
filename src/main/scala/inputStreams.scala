import org.apache.spark.sql.{DataFrame, SparkSession}

object inputStreams {

  val spark: SparkSession= SparkSession.builder.appName("IMDB - join").getOrCreate()
      // .master("local[*]")

// 192.168.2.17
  val kafkaAddress = "localhost:9092"

  var titleStream:DataFrame = _  ;
  var actorsTitleStream:DataFrame = _  ;
  var actorStream:DataFrame = _  ;
  var ratingStream:DataFrame = _  ;


  var titleKafkaStream:DataFrame = _  ;
  var actorsTitleKafkaStream:DataFrame = _  ;
  var actorKafkaStream:DataFrame = _  ;
  var ratingKafkaStream:DataFrame = _  ;

  def startStreams(): Unit ={

    actorStream = spark
      .readStream
      .option("header","true")
      .option("sep", "\t")
      .schema(schemasDefinition.actorSchema)
      .csv("file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/artists" )

    ratingStream = spark
      .readStream
      .option("header","true")
      .option("sep", "\t")
      .schema(schemasDefinition.ratingSchema)
      .csv("file:////home/vinicius/IdeaProjects/sparkExercises/src/resources/ratings")

    titleStream = spark
      .readStream
      .option("header","true")
      .option("sep", "\t")
      .schema(schemasDefinition.titleSchema)
      .csv("file:////home/vinicius/IdeaProjects/sparkExercises/src/resources/titles" )

   actorsTitleStream = spark
     .readStream
     .option("header","true")
     .option("sep", "\t")
     .schema(schemasDefinition.actorTitleSchema)
     .csv("file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/artist.title" )

  }

  def startKafkaStreams(): Unit ={


    // titleKafkaStream = spark
    //   .readStream
    //   .format("kafka")
    //   .option("failOnDataLoss","false")
    //   .option("kafka.bootstrap.servers", kafkaAddress)
    //   .option("subscribe", "titles")
    //   .option("startingOffsets", "earliest")
    //   .load()

    ratingKafkaStream = spark
      .readStream
      .format("kafka")
      .option("failOnDataLoss","false")
      .option("kafka.bootstrap.servers", kafkaAddress)
      .option("subscribe", "ratings")
      .option("startingOffsets", "earliest")
      .load()

//    actorsTitleKafkaStream = spark
//      .readStream
//      .option("failOnDataLoss","false")
//      .format("kafka")
//      .option("kafka.bootstrap.servers", kafkaAddress)
//      .option("subscribe", "actors_titles")
//      .option("startingOffsets", "earliest")
//      .load()
//
//    actorKafkaStream = spark
//      .readStream
//      .format("kafka")
//      .option("failOnDataLoss","false")
//      .option("kafka.bootstrap.servers", kafkaAddress)
//      .option("subscribe", "actors")
//      .option("startingOffsets", "earliest")
//      .load()
  }

}
