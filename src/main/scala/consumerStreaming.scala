import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

import inputStreams.spark
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions._;


object consumerStreaming extends App {
  println("consumerStreaming")

  //
  //  val sparkSession: SparkSession  = _ ;
  //  import sparkSession.implicits._
  import inputStreams.spark.implicits._


  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", inputStreams.kafkaAddress)
    .option("subscribe", "ratings")
    .option("startingOffsets", "earliest")
    .load()
//

  val date_microsec = udf((dt: String) => {
    val dtFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS*")
    LocalDateTime.parse(dt, dtFormatter).getLong(ChronoField.MICRO_OF_SECOND)
  })

  df
    .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
    .as[(String, Timestamp)]
    .select(from_json($"value", schemasDefinition.ratingSchema).as("bar"), $"timestamp")
    .select($"bar.*"
//      ,date_format($"timestamp","ss.SSS").as("newTime")
      ,date_microsec($"timestamp").as("NewNewTime"))
 //    ,$"timestamp")
    .writeStream
//    .format("csv")
//    .option("path", "file:////home/vinicius/IdeaProjects/sparkExercises/output")
//    .option("checkpointLocation","file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/test")
    // .option("truncate","false")
    .format("console")
    .outputMode("update")
    .start()
    .awaitTermination()

}
