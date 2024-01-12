import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._

import java.text.SimpleDateFormat
import java.time.LocalDate.now
import java.util.Locale

object DataSet_3 extends App {

  case class Channel(
                      channel_name: String,
                      city: String,
                      country: String,
                      created: String
                    )

  case class Age(
                  channel_name: String,
                  age: String
                )

  def toDate(date: String, dateFormat: String): Long = {
    val format = new SimpleDateFormat(dateFormat, Locale.ENGLISH)
    format.parse(date).getTime
  }

  def countChannelAge(channel: Channel): Age = {
    val age = (toDate(now.toString, "yyyy-MM-dd") - toDate(channel.created, "yyyy MMM dd")) / (1000 * 60 * 60 * 24)
    Age(channel.channel_name, age.toString)
  }

  val spark = SparkSession.builder()
    .appName("DataSet_3")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val channelsDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/channel.csv")

  val channelsDS = channelsDF.as[Channel]

  val cityNamesDS: Dataset[String] = channelsDS.map(channel => channel.city.toUpperCase)

  val channelsDS_1 = channelsDF.as[Channel].map(channel => countChannelAge(channel))

  cityNamesDS.show()
  channelsDS_1.show()

}
