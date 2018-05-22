import org.apache.spark.sql.types.{StringType, StructField, StructType}

object schemasDefinition {

  val titleSchema = StructType(
    Array(
      StructField("tconst", StringType),
      StructField("primaryTitle", StringType),
      StructField("originalTitle", StringType),
      StructField("endYear", StringType)
    ))
  val actorTitleSchema = StructType(
    Array(
      StructField("nconst", StringType),
      StructField("tconst", StringType)
    ))

  val  actorSchema = StructType(
    Array(
      StructField("nconst", StringType),
      StructField("primaryName", StringType),
      StructField("birthYear", StringType),
      StructField("deathYear", StringType)
      // StructField("primaryProfession", StringType),
      //   StructField("knownForTitles", StringType)
    ))
}
