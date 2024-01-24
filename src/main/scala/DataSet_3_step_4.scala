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
  }

  def replaceNull(df: DataFrame): DataFrame = {
    df
      .na
      .fill(
        Map(
          "JobTitle" -> "Unknown",
          "Company" -> "Unknown",
          "CompanyReviews" -> 0,
        )
      )
  }

  import spark.implicits._
  val df: DataFrame = spark
    .read
    .option("header", "true")
    .option("multiLine", "true")
    .option("escape", "\"")
    .option("quote", "\"")
//    .option("inferSchema", "true")
    .csv("src/main/resources/AIJobsIndustry.csv")


  //"Internship: ""Data science"""
  //""">【智通所】LTE網路研發工程師(定期契約)"
  //"PhD Researcher ""Machine Learning for correct component design and manufacturing"""

  val df1 = spark
    .read
    .option("lineSep", "\r")
    .text("src/main/resources/AIJobsIndustry.csv")

  df1.withColumn("value",
      regexp_replace($"value", "\"\"\"\\w", "XXXXX")
    )

  df1.filter(col("value").rlike("\"\"\"\\w")).show()

  df1.show()


  val cnt = df
    .transform(convert)
//    .transform(replaceNull)

  cnt
    .show()

  val cnt2 = cnt
    .select("CompanyReviews").distinct()
    .orderBy(col("CompanyReviews"))

  cnt2
    .write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/test2")

}
