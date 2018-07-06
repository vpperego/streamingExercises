import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._;


object consumerStreaming extends App {
  println("consumerStreaming")
   inputStreams.startStreams()

   val foo =  inputStreams
    .titleStream
 //    .foreach(row => row.getClass)
//    .withColumn("time",lit(System.currentTimeMillis())) DOENS'T WORK
//      .withColumn("time2",current_timestamp())DOENS'T WORK
//      .transform(addTimestamp) DOENS'T WORK
//      .rdd.map(foo => foo.toSeq ++ Array[Any](System.currentTimeMillis)))
//      .map( row => Row.fromSeq(row.toSeq ++ Seq(current_timestamp) ))
//      .withColumn("foo",)
//           .withColumn("processingTime",current_timestamp())//DOENS'T WORK
//      . groupBy(
//          window($"processingTime", "500 milliseconds")
//        ).count()
 //     .as[(String, Timestamp)]
//     .toDF("word", "timestamp")
//     .flatMap(line => line._1.split(" ").map(word => {
//     Thread.sleep(15000)
//     (word, line._2)
//   }))
       .withColumn("newTime",unix_timestamp())

//     .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
//     .as[(String, Timestamp)]
//     .select(from_json($"value", schemasDefinition.titleSchema).as("data2"), $"timestamp".as("time"))
//     .select($"data2.*")
//     .as[(String, Timestamp)]
//     .toDF("word", "timestamp")

     .select("*")
     .writeStream
      .format("csv")
      .option("path", "file:////home/vinicius/IdeaProjects/sparkExercises/output2")
      .option("checkpointLocation","file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/test")
      .start()
      .awaitTermination(10000)

  def transformRows(iter: Iterator[Row]): Iterator[Row] = iter.map(transformRow)

   //
  //  val sparkSession: SparkSession  = _ ;
//  import inputStreams.spark.implicits._
//  val addTimestamp = udf((df: DataFrame) =>
//    df.withColumn("time", lit((System.currentTimeMillis()) )

//  def addTimestamp(df: DataFrame): DataFrame = {
//    println("calling a func")
//    df.withColumn("time", lit((System.currentTimeMillis())))
//  }

  def transformRow(row: Row): Row =
    Row.fromSeq(row.toSeq ++ Array[Any](System.currentTimeMillis))

//      .rdd.map(transformRows)

//  foo
//    .writeStream
//    .format("csv")
//    .option("path", "file:////home/vinicius/IdeaProjects/sparkExercises/output2")
//    .option("checkpointLocation","file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/test")
//    .start()
//    .awaitTermination(10000)

  //
  // val df = spark
  //   .readStream
  //   .format("kafka")
  //   .option("kafka.bootstrap.servers", inputStreams.kafkaAddress)
  //   .option("subscribe", "ratings")
  //   .option("startingOffsets", "earliest")
  //   .load()
//
//   val realTimeMs = udf((a: Long) =>  a = System.currentTimeMillis() )
  //
  // df
    // .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
  //   .as[(String,Timestamp)]
  //   .select(from_json($"value", schemasDefinition.ratingSchema).as("bar"), $"timestamp".as("time"))
//       .withColumn("msTime",currrent_timestamp() realTimeMs($"time"))
  //   .select($"bar.*",$"msTime")
  //   .writeStream
  //  .format("csv")
  //  .option("path", "file:////home/vinicius/IdeaProjects/sparkExercises/output2")
  //  .option("checkpointLocation","file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/test")
  //  .start()
  //  .awaitTermination(10000)


//     inputStreams.ratingKafkaStream
//     .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
//       .as[(String,Timestamp)]
//    .withColumn("foo",)
//       .select(from_json($"value", schemasDefinition.ratingSchema).as("bar"), $"timestamp".as("time"))
//       .select($"bar.tconst",$"time")
//
//       .writeStream
//       .format("csv")
//       .option("path", "file:////home/vinicius/IdeaProjects/sparkExercises/rating_timestamp")
//       .option("checkpointLocation","file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/rating_timestamp")
//       .start()
//       .awaitTermination(10000)



//     inputStreams.titleKafkaStream
//       .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
//       .as[(String,Timestamp)]
//       .select(from_json($"value", schemasDefinition.ratingSchema).as("bar2"), $"timestamp".as("time2"))
//    .map(row => realTimeMs(row(1)))
//       .select($"bar2.tconst",$"time2")
//       .writeStream
//       .format("csv")
//       .option("path", "file:////home/vinicius/IdeaProjects/sparkExercises/title_timestamp")
//       .option("checkpointLocation","file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/title_timestamp")
//       .start()
//       .awaitTermination(10000)
//
//
//    var  imdbOutputStream = spark
//        .readStream
//        .format("kafka")
//        .option("failOnDataLoss","false")
//        .option("kafka.bootstrap.servers", "localhost:9093")
//        .option("subscribe", "imdb_output2")
//        .option("startingOffsets", "earliest")
//        .load()
//
//
//        imdbOutputStream
//          .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
//          .as[(String,Timestamp)]
//          .select(from_json($"value", schemasDefinition.ratingSchema).as("bar2"), $"timestamp".as("time2"))
//          .select($"bar2.tconst",$"time2")
//          .writeStream
//          .format("csv")
//          .option("path", "file:////home/vinicius/IdeaProjects/sparkExercises/imdb_output_timestamp")
//          .option("checkpointLocation","file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/imdb_output_timestamp")
//          .start()
//          .awaitTermination(10000)

}
