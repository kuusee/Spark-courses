import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object DataSet_2 extends App {
  val spark = SparkSession.builder()
    .appName("DataSet_2")
    .config("spark.master", "local")
    .getOrCreate()

  val employeeDF: DataFrame = spark.read
    .option("header", "true")
    .csv("src/main/resources/employee.csv")

  employeeDF.select("*")
    .where(col("birthday").isNull)
    .show()

  employeeDF
    .select("*")
    .orderBy(
      col("date_of_birth").desc_nulls_first
    )
    .show()

  val employeeDF_1 = employeeDF.select("*").na.drop().show()

  val employeeDF_2 = employeeDF.na.fill("n/a", List("birthday", "date_of_birth")).show()

  val employeeDF_3 = employeeDF.na.fill(Map("birthday" -> "-", "date_of_birth" -> "+")).show()

  val employeeDF_4 = employeeDF.select(
    col("name"),
    coalesce(col("birthday"), col("date_of_birth"), lit("bb"))
  ).show()

  val employeeDF_5 = employeeDF.selectExpr(
    "name",
    "birthday",
    "date_of_birth",
    "ifnull(birthday, date_of_birth) as ifnull",
    "nvl(birthday, date_of_birth) as nvl",
    "nvl2(birthday, date_of_birth, 'Unknown') as nvl2"
  ).show()

}
