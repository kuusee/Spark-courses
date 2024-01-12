import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object DataFrame_5_step_2 extends App {

  val spark = SparkSession.builder()
    .appName("DataFrame_5")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

//  загрузить данные из файла в датафрейм
  val customersDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/mall_customers.csv")


  val incomeDF = customersDF.withColumn("Age", $"Age" + 2) // Увеличиваем возраст на 2 года
    .groupBy($"Gender", $"Age")
    .agg(
      round(mean($"Annual Income (k$$)"), 1).as("Annual_Income_k$")
    )
    .where(($"Age" >= 30) && ($"Age" <= 35))
    .withColumn("gender_code",
      when($"Gender" === "Female", 0)
      .when($"Gender" === "Male", 1)
        .otherwise(null)
    )
    .orderBy("Gender", "Age")


  incomeDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/customers")
}
