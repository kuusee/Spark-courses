import org.apache.spark.sql.{DataFrame, SparkSession, Encoders, Dataset}
import org.apache.spark.sql.types._


object DataSet_1 extends App {

  case class Customer(
                       name: String,
                       surname: String,
                       age: Int,
                       occupation: String,
                       customer_rating: Double
                     )

  val spark = SparkSession.builder()
    .appName("Dataset_1")
    .master("local")
    .getOrCreate()

  val namesDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/names.csv")

  import spark.implicits._
  val namesDS: Dataset[String] = namesDF.as[String]

  namesDS.filter(n => n != "Bob").show()

  val customerDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv("src/main/resources/customers.csv")

  val customerDS = customerDF.as[Customer]

  customerDS.show(5)
  customerDS.printSchema()

}
