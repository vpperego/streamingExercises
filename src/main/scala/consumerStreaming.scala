import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object consumerStreaming{
  def run(){
    val spark: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate();

    import spark.implicits._;


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
      .format("console")// TODO - WRITE IN FILESYSTEM
      .option("truncate","false")
      .start()
      .awaitTermination()

  }
}
