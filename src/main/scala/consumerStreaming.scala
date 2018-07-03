import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

import inputStreams.spark
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

  val realTimeMs = udf((t: java.sql.Timestamp) => t.getTime)

  df
    .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
    .as[(String,Timestamp)]
    .select(from_json($"value", schemasDefinition.ratingSchema).as("bar"), $"timestamp".as("time"))
     .withColumn("msTime", realTimeMs($"time"))
    .select($"bar.*",$"msTime")
    .writeStream
   .format("csv")
   .option("path", "file:////home/vinicius/IdeaProjects/sparkExercises/output2")
   .option("checkpointLocation","file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/test")
   .start()
   .awaitTermination(15000)
    // .option("truncate","false")
    // .format("console")
    // .outputMode("update")

}
