import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object NBA {
    var pathRoute ="../../../inputs/  .csv"
    val spark = SparkSession.builder.getOrCreate()

    val df = spark.read.option("header", true).option("multiLine", true).option("escape", "\"").csv(pathRoute.split(";").mkString(","))
}