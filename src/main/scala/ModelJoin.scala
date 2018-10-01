import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType


object ModelJoin extends App{
  val spark: SparkSession= SparkSession.builder
    .getOrCreate()

  import spark.implicits._

  val userSchema = new StructType().add("id", "integer");
  val userSchema2 = new StructType().add("value", "integer");
  val stream1 = spark.readStream .option("sep", ";") .schema(userSchema) .csv("file:///home/vinicius/IdeaProjects/spark/resources/stream1");
  val stream2 = spark.readStream .option("sep", ";") .schema(userSchema2) .csv("file:///home/vinicius/IdeaProjects/spark/resources/stream2");
  stream1.join(stream2,$"id" < $"value").writeStream.format("console")
  .start
  .awaitTermination();

}
