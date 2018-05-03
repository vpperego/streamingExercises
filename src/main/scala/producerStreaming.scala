import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._



/*
  Writes the movie dataset to the Kafka Topic named "movies"

 */
object producerStreaming extends App {
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

    val streamingDataFrame =
      spark
        .readStream
        .schema(mySchema)
        .csv("sparkExercises/src/resources/")
//
    streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").
      writeStream
      .format("kafka")
      .option("topic", "movies")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "/sparkExercises/src/checkpoint")
      .start()
    .awaitTermination()
}