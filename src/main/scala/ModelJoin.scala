import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType


object ModelJoin extends App{
  val spark: SparkSession= SparkSession.builder
      .appName("Inequality Predicate Join")
    .getOrCreate()

  import spark.implicits._
 // $"id" === $"value"
  val userSchema = new StructType().add("value", "integer");
  val userSchema2 = new StructType().add("id", "integer");
  val stream1 = spark.readStream .option("sep", ";") .schema(userSchema) .csv("hdfs:/user/vinicius/datasets/stream1");
  val stream2 = spark.readStream .option("sep", ";") .schema(userSchema2) .csv("hdfs:/user/vinicius/datasets/stream2");
  stream1.join(stream2,$"value" < $"id").writeStream.format("csv")
    .option("path", "/user/vinicius/join-output")
    .option("checkpointLocation","/tmp/join-checkpoint")
  .start
  .awaitTermination(10000);

}
