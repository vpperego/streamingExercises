import inputStreams.spark
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object consumerStreaming extends App {
  println("consumerStreaming")

  //
  //  val sparkSession: SparkSession  = _ ;
  //  import sparkSession.implicits._
  import inputStreams.spark.implicits._



  val mySchema = StructType(Array(
    StructField("id", StringType),
    StructField("name", StringType),
    StructField("year", StringType),
    StructField("rating", StringType),
    StructField("duration", StringType)
  ))

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "movies")
    .option("startingOffsets", "earliest")

    .load()

  df
    .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)","CAST(topic AS STRING)")
    .as[(String, Timestamp, String)]
    .select(from_json($"value", mySchema).as("bar"), $"topic")
    .select("bar.", "topic")
    .writeStream
    .format("console")
    .option("truncate","false")
    .start()
    .awaitTermination()

}
