import org.apache.spark.sql.types._

object schemasDefinition {

  val titleSchema = StructType(
    Array(
      StructField("tconst", IntegerType),
      StructField("primaryTitle", StringType),
      StructField("endYear", StringType)
    ))
//  val actorTitleSchema = StructType(
//    Array(
//      StructField("nconst", StringType),
//      StructField("tconst", StringType)
//    ))

  val ratingSchema = StructType(
    Array(
      StructField("tconst", IntegerType),
      StructField("averageRating", StringType),
      StructField("numVotes", StringType)
    ))

//  val  actorSchema = StructType(
//    Array(
//      StructField("nconst", IntegerType),
//      StructField("primaryName", StringType),
//      StructField("birthYear", StringType),
//      StructField("deathYear", StringType)
//      // StructField("primaryProfession", StringType),
//      //   StructField("knownForTitles", StringType)
//    ))
}
