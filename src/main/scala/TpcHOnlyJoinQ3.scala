import inputStreams.spark

object TpcHOnlyJoinQ3 extends App {

  /*
    Paths definition

   */
  val customerPath = "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/TpcHOnlyJoinQ3/customer"
  val customerStoreCheckpoint = "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/customer-store"
  val customerStorePath = "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/TpcHOnlyJoinQ3/output/customer-store"
  val orderStoreCheckpoint ="file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/checkpoints/order-store"
  val orderStorePath = "file:///home/vinicius/IdeaProjects/sparkExercises/src/resources/TpcHOnlyJoinQ3/output/order-store"

  //set schemaInference on
  spark.conf.set("spark.sql.streaming.schemaInference", "true")


  var customerInput =  spark.readStream
    .option("header","true").option("sep", ",")
     .csv(customerPath)

  //TODO - create a copy of customerInput stream
  var customerStoreStream = customerInput.cache()

  customerInput.writeStream
    .format("csv")
    .option("path",customerStorePath )
    .option("checkpointLocation",customerStoreCheckpoint)
    .start()
    .awaitTermination()


  customerStoreStream.writeStream
    .format("csv")
    .option("path", orderStorePath)
    .option("checkpointLocation",orderStoreCheckpoint)
    .start()
    .awaitTermination()


}
