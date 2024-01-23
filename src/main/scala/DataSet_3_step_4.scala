import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

//object DfColumn extends DfColumn {
//  implicit def columnToString(col: DfColumn.Value): String = col.toString
//}
//
//trait DfColumn extends Enumeration {
//  val JobTitle, Company, Location, CompanyReviews, Link = Value
//}
object DataSet_3_step_4 extends App with Context {
  override val appName: String = "DataSet_3_step_4"

  def convert(df: DataFrame): DataFrame = {
    df
      .withColumn("CompanyReviews", regexp_replace(col("CompanyReviews"), ",", "")
      )
      .select()
  }

  import spark.implicits._
  val df: DataFrame = spark
    .read
    .option("header", "true")
    .option("multiLine", "true")
    .option("escape", "\'")
    .option("inferSchema", "true")
    .csv("src/main/resources/AIJobsIndustry.csv")

  val cnt = df
    .transform(convert)

  cnt
    .show()
//  df.printSchema()
//
//
//  df.write
//    .format("json")
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/data/test")

}
