import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Twitter {
    var pathRoute ="inputs/trump_insult_tweets_2014_to_2021.csv"
    val spark = SparkSession.builder.getOrCreate()

    // val df = spark.read.option("header", true).option("multiLine", true).option("escape", "\"").csv(pathRoute.split(";").mkString(","))
    val df = spark.read.option("header", true).option("inferSchema", "true").option("multiLine", true).option("escape", "\"").csv(pathRoute.split(";").mkString(","))

// Afficher les 7 comptes que Donald Trump insulte le plus (en nombres et en pourcentages)
    val insultCounts = df.groupBy("target").count()
    val totalInsults = insultCounts.agg(sum("count")).first().getLong(0)
    val insultPercentages = insultCounts.withColumn("percentage", (insultCounts("count") / totalInsults) * 100)
    val sortedInsultPercentages = insultPercentages.orderBy(-insultPercentages("count"))
    sortedInsultPercentages.show(7)


// Afficher les insultes que Donald Trump utilise le plus (en nombres et en pourcentages)
    val insultCounts2 = df.groupBy("insult").count()
    val totalInsults2 = insultCounts2.agg(sum("count")).first().getLong(0)
    val insultPercentages2 = insultCounts2.withColumn("percentage", (insultCounts2("count") / totalInsults2) * 100)
    val sortedInsultPercentages2 = insultPercentages2.orderBy(-insultPercentages2("count"))
    sortedInsultPercentages2.show()

// Quelle est l'insulte que Donald Trump utilise le plus pour Joe Biden?
    val bidenInsults = df.filter(col("target") === "joe-biden")
    val bidenInsultCounts = bidenInsults.groupBy("insult").count()
    val mostRepeatedInsult = bidenInsultCounts.orderBy(desc("count")).first()

    println(s"L'insulte la plus repetee est '${mostRepeatedInsult.getString(0)}'  ${mostRepeatedInsult.getLong(1)} fois.")


// Combien de fois a-t-il tweeté le mot "Mexico"? Le mot "China"? Le mot "coronavirus"? 
    val mexicoTweets = df.filter(lower(col("tweet")).contains("mexico"))
    val chinaTweets = df.filter(lower(col("tweet")).contains("china"))
    val coronavirusTweets = df.filter(lower(col("tweet")).contains("coronavirus"))
    
    println(s"Nombre de tweets contenant 'Mexico': ${mexicoTweets.count()}")
    println(s"Nombre de tweets contenant 'China': ${chinaTweets.count()}")
    println(s"Nombre de tweets contenant 'coronavirus': ${coronavirusTweets.count()}")


// Classer le nombre de tweets par période de 6 mois
    val withDate = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    val withPeriodStartEnd = withDate.withColumn("periodStartEnd", 
        concat(date_format(trunc(col("date"), "yyyy"), "MM-yyyy"), lit(" - "), 
        date_format(add_months(trunc(col("date"), "yyyy"), 6), "MM-yyyy")))
    val tweetAmountByPeriod = withPeriodStartEnd.groupBy("periodStartEnd").count().orderBy("periodStartEnd")

    tweetAmountByPeriod.show()
}