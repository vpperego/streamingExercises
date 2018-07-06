import java.sql.Timestamp

import org.apache.spark.sql.functions.from_json;

class extractTimeStamp {
  import inputStreams.spark.implicits._

  inputStreams.ratingKafkaStream
    .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
    .as[(String,Timestamp)]
    .select(from_json($"value", schemasDefinition.ratingSchema).as("bar"), $"timestamp".as("time"))
    .select($"bar.tconst",$"time")

    .writeStream
    .format("csv")
    .option("path", "file:////home/vinicius/IdeaProjects/sparkExercises/rating_timestamp")
    .option("checkpointLocation","file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/rating_timestamp")
    .start()

    .awaitTermination(10000)



  inputStreams.titleKafkaStream
    .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
    .as[(String,Timestamp)]
    .select(from_json($"value", schemasDefinition.ratingSchema).as("bar2"), $"timestamp".as("time2"))
    .select($"bar2.tconst",$"time2")
    .writeStream
    .format("csv")
    .option("path", "file:////home/vinicius/IdeaProjects/sparkExercises/title_timestamp")
    .option("checkpointLocation","file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/title_timestamp")
    .start()
    .awaitTermination(10000)



}
