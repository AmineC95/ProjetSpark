import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Netflix {
    var pathRoute = "inputs/netflix_titles.csv"
    val spark = SparkSession.builder.getOrCreate()

    val df = spark.read.option("header", true).option("inferSchema", "true").option("multiLine", true).option("escape", "\"").csv(pathRoute)

    val cleanedDf = df.na.drop(Seq("director", "cast", "country", "date_added", "release_year", "duration"))

    // Lister les réalisateurs les plus prolifiques, et leur nombre de films respectifs
    val prolificDirectors = cleanedDf.filter(col("director").isNotNull)
        .groupBy("director")
        .count()
        .orderBy(desc("count"))
    prolificDirectors.show()

    // Afficher en pourcentages les pays dans lesquels les films/séries ont été produits
    val countryCounts = cleanedDf.filter(col("country").isNotNull)
        .groupBy("country")
        .count()
    val totalProductions = countryCounts.agg(sum("count")).first().getLong(0)
    val countryPercentages = countryCounts.withColumn("percentage", (col("count") / totalProductions) * 100)
        .orderBy(desc("count"))
    countryPercentages.show()

    // Quelle est la durée moyenne des films sur Netflix? Le film le plus long? Le film le plus court?
    val movies = cleanedDf.filter(col("type") === "Movie")
    val durations = movies.withColumn("duration", regexp_extract(col("duration"), "\\d+", 0).cast(IntegerType))
    val avgDuration = durations.agg(avg("duration")).first().getDouble(0)
    val longestMovie = durations.orderBy(desc("duration")).filter(col("duration").isNotNull).select("title", "duration").first()
    val shortestMovie = durations.orderBy(asc("duration")).filter(col("duration").isNotNull).select("title", "duration").first()

    println(s"La duree moyenne des films sur Netflix est de ${avgDuration} minutes.")
    println(s"Le film le plus long est '${longestMovie.getString(0)}' avec ${longestMovie.getInt(1)} minutes.")
    println(s"Le film le plus court est '${shortestMovie.getString(0)}' avec ${shortestMovie.getInt(1)} minutes.")

    // Afficher la durée moyenne des films par intervalles de 2 ans, ordonné par yearInterval du plus récent au plus vieux
    val withYear = durations.withColumn("release_year", col("release_year").cast(IntegerType))
    val withInterval = withYear.withColumn("yearInterval", concat((col("release_year") - (col("release_year") % 2)).cast(StringType), lit("-"), (col("release_year") - (col("release_year") % 2) + 1).cast(StringType)))
    val avgDurationByInterval = withInterval.groupBy("yearInterval").agg(avg("duration").as("avg_duration"))
        .withColumn("startYear", regexp_extract(col("yearInterval"), "\\d{4}", 0).cast(IntegerType))
        .orderBy(desc("startYear"))

    avgDurationByInterval.select("yearInterval", "avg_duration").show()

    // Quel est le duo réalisateur-acteur qui a collaboré dans le plus de films?
    val explodedCast = cleanedDf.withColumn("cast", explode(split(col("cast"), ", ")))
    val directorActorPairs = explodedCast.withColumn("director_actor", concat_ws(" - ", col("director"), col("cast")))
    val mostFrequentPair = directorActorPairs.groupBy("director_actor").count().orderBy(desc("count")).first()

    println(s"Le duo realisateur-acteur qui a collabore dans le plus de films est '${mostFrequentPair.getString(0)}' avec ${mostFrequentPair.getLong(1)} collaborations.")
}