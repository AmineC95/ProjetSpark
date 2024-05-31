import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AirBnb {
    var pathRoute ="inputs/listings.csv"
    val spark = SparkSession.builder.getOrCreate()

    val df = spark.read.option("header", true).option("multiLine", true).option("escape", "\"").csv(pathRoute.split(";").mkString(","))
//Room Types
    val total = df.count()
    val roomTypeCounts = df.groupBy("room_type").count()
    val roomTypePercentages = roomTypeCounts.withColumn("percentage", (roomTypeCounts("count") / total) * 100)
    val sortedRoomTypePercentages = roomTypePercentages.orderBy(-roomTypePercentages("percentage"))

    sortedRoomTypePercentages.show()


//Activity (à revoir) : number_of_review * min_night avg ntm /2
    val dfClean = df.withColumn("price", regexp_replace(col("price"), "\\$", "").cast("double"))
        .na.fill(0, Seq("price", "minimum_nights", "number_of_reviews"))
    val dfWithEstimates = dfClean.withColumn("estimated_nights_booked", col("minimum_nights") * col("number_of_reviews"))
        .withColumn("estimated_income", col("price") * col("estimated_nights_booked"))
    val averages = dfWithEstimates.agg(
        avg("estimated_nights_booked").alias("average_nights_booked"),
        avg("price").alias("average_price_per_night"),
        avg("estimated_income").alias("average_income")
    )

    averages.show()


//Listings per Host
    val hostListingCount = df.groupBy("host_name").count()
    val singleListings = hostListingCount.filter(col("count") === 1).count()
    val multiListings = hostListingCount.filter(col("count") > 1).count()
    val totalHosts = hostListingCount.count()
    val singleListingPercentage = (singleListings.toDouble / totalHosts) * 100
    val multiListingPercentage = (multiListings.toDouble / totalHosts) * 100

    println(s"Single listings: $singleListings ($singleListingPercentage%)")
    println(s"Multi-listings: $multiListings ($multiListingPercentage%)")

//Short-Term Rentals
    val totalListings = df.count()
    val shortTermRentals = df.filter(col("minimum_nights") <= 30).count()
    val longTermRentals = df.filter(col("minimum_nights") > 30).count()
    val shortTermPercentage = (shortTermRentals.toDouble / totalListings) * 100
    val longTermPercentage = (longTermRentals.toDouble / totalListings) * 100

    println(s"Short-term rentals: $shortTermRentals ($shortTermPercentage%)")
    println(s"Long-term rentals: $longTermRentals ($longTermPercentage%)")


//Top Hosts
    val roomTypeCount = df.groupBy("host_name", "room_type").count()
    val pivotDF = roomTypeCount.groupBy("host_name").pivot("room_type").sum("count")
    val totalListingHosts = pivotDF.withColumn("Total Listings", col("Entire Home/Apt") + col("Private room") + col("Shared room") + col("Hotel room"))

    totalListingHosts.orderBy(desc("Hotel room")).show()

}