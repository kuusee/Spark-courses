import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
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

  def withCompanyReview(df: DataFrame): DataFrame = {
    df
      .withColumn(
        "CompanyReviews", regexp_replace(
          col("CompanyReviews"), "[^[0-9]]", ""
        )
      )
  }

  def replaceNull(df: DataFrame): DataFrame = {
    df
      .na
      .fill(
        Map(
          "JobTitle" -> "Unknown",
          "Company" -> "Unknown",
          "Location" -> "Unknown",
          "CompanyReviews" -> 0,
        )
      )
  }

  def withLowerCase(df: DataFrame): DataFrame = {
    // Преобразовываем слова в нижний регистр
    df.withColumn("Word", lower(col("Word")))
  }

  def withJobTitle(columnName: String)(df: DataFrame): DataFrame = {
    val regexp1 = regexp_replace(_: Column, "\"", "")
    val regexp2 = regexp_replace(_: Column, "[ ]", "_")
    val regexp3 = regexp_replace(_: Column, "[\\p{C}\\p{Zs}]", "")

    df
      .withColumn(columnName,
        regexp3(
          regexp2(
            regexp1(col(columnName))))
      )
  }

  import spark.implicits._
  val df: DataFrame = spark
    .read
    .option("header", "true")
    .option("multiLine", "true")
    .option("escape", "\"")
    .option("quote", "\"")
    .option("inferSchema", "true")
    .csv("src/main/resources/AIJobsIndustry.csv")


  //"Internship: ""Data science"""
  //""">【智通所】LTE網路研發工程師(定期契約)"
  //"PhD Researcher ""Machine Learning for correct component design and manufacturing"""


  val cnt = df
    .transform(withCompanyReview)
    .transform(replaceNull)
    .transform(withJobTitle("JobTitle"))

  val cnt2 = cnt
    .select("JobTitle").distinct()
//    .orderBy(col("JobTitle"))

  cnt2.show()

  cnt2
    .write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/JobTitle3")

}
