
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Join between three relations in IMDB datasets
  * (https://www.imdb.com/interfaces/) :
  *   -title.basics.tsv.gz
  *   -title.principals.tsv.gz
  *   -name.basics.tsv.gz
  */
object imdbJoin {

  def main(args: Array[String]) {
    if (args.length < 4){
      println("Usage: imdbJoin titlesBasic titlePrincipal actors")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()
    var tbSchema,tpSchema, actorSchema;
    (tbSchema,tpSchema, actorSchema) = startSchemas()

    val titlesBasicStream = spark
      .readStream
      .option("header","true")
      .option("sep", "\t")
      .schema(tbSchema)
      .csv(args(1))

    val titlesPrincipalStream = spark
      .readStream
      .option("header","true")
      .option("sep", "\t")
      .schema(tpSchema)
      .csv(args(2))

    val actorStream = spark
      .readStream
      .option("header","true")
      .option("sep", "\t")
      .schema(actorSchema)
      .csv(args(3))


    val query = titlesPrincipalStream
      .join(titlesBasicStream,"tconst")
      .join(actorStream,"nconst")
      .select("originalTitle","primaryName","birthYear")
      .writeStream
      .outputMode("append")
      .format("console")
      .start

    query.awaitTermination()
  }

  def startSchemas(){

    val titleBasicSchema = StructType(
      Array(
        StructField("tconst", StringType),
        StructField("titleType", StringType),
        StructField("primaryTitle", StringType),
        StructField("originalTitle", StringType),
        StructField("isAdult", IntegerType),
        StructField("startYear", IntegerType),
        StructField("endYear", StringType),
        StructField("runtimeMinutes", StringType),
        StructField("genres",StringType)
      ))

    val titlePrincipalSchema = StructType(
      Array(
        StructField("tconst", StringType),
        StructField("ordering", IntegerType),
        StructField("nconst", StringType),
        StructField("category", StringType),
        StructField("job", StringType),
        StructField("characters", StringType)
      ))

    val  actorSchema = StructType(
      Array(
        StructField("nconst", StringType),
        StructField("primaryName", StringType),
        StructField("birthYear", IntegerType),
        StructField("deathYear", IntegerType),
        StructField("primaryProfession", StringType),
        StructField("knownForTitles", StringType)
      ))

    List(titleBasicSchema, titlePrincipalSchema, actorSchema)
  }
}