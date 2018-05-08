import org.apache.spark.sql.types.{StringType, StructField, StructType}

object schemasDefinition {

  val titleSchema = StructType(
    Array(
      StructField("tconst", StringType),
      StructField("primaryTitle", StringType),
      StructField("startYear", StringType),
      StructField("endYear", StringType)
    ))

  val artistTitleSchema = StructType(
    Array(
      StructField("nconst", StringType),
      StructField("tconst", StringType)
    ))

  val  artistSchema = StructType(
    Array(
      StructField("nconst", StringType),
      StructField("primaryName", StringType),
      StructField("birthYear", StringType),
      StructField("deathYear", StringType)
    ))
}
