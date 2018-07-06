import inputStreams.{ratingStream, titleStream}

object fileJoin extends App{

  inputStreams.startStreams()

//  titleStream.withColumnRenamed("tconst", "_tconst")

  val query = ratingStream
    .join(titleStream ,"tconst")
//     .select("*")
    .writeStream
    .format("csv")
    .option("path", "file:////home/vinicius/IdeaProjects/sparkExercises/file_join")
    .option("checkpointLocation","file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/file_join")
    .start()
    .awaitTermination(60000)
}
