import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

object DataFrame_5_step_3_old extends App with Context {
  override val appName: String = "DataFrame_5_step_3"

  def filterHeader(df: DataFrame): DataFrame = {
    // Фильтруем данные от заголовков
    val words = "Game Of Thrones.*\\.srt"

    df.filter(!col("_c0").rlike(words))
  }

  def filterNumber(df: DataFrame): DataFrame = {
    // Фильтруем данные от чисел
    df.filter(col("Word").cast("int").isNull)
  }

  def filterSpace(df: DataFrame): DataFrame = {
    // Фильтруем данные от пустых строк
    val isSpace = col("Word") =!= ""

    df.filter(isSpace)
  }

  def withLowerCase(df: DataFrame): DataFrame = {
    // Преобразовываем слова в нижний регистр
    df.withColumn("Word", lower(col("Word")))
  }

  def withSplitPhrase(df: DataFrame): DataFrame = {
  // Разделяем строки на слова и создаем столбец с этими словами
     df
       .select(
            explode(
              split(col("_c0"), "\\W+")
            ).as("Word")
          )
  }

  def withNumberId(df: DataFrame): DataFrame = {
    // Создаем столбец с номером строки
    df.withColumn("id", monotonically_increasing_id())
  }

  def extractWordCount(col_word: String, col_count: String)(df: DataFrame): DataFrame = {
    // Агрегируем слова, сортируем и забираем первые 20 строк
    df
      .groupBy(col("Word").as(col_word))
      .agg(count("Word").as(col_count))
      .orderBy(desc(col_count))
      .limit(20)
  }

  def pipeline(col_word: String = "Word",
               col_count: String = "Total")(df: DataFrame): DataFrame = {
    // Общий поток преобразований
    df
      .transform(filterHeader)
      .transform(withSplitPhrase)
      .transform(filterNumber)
      .transform(filterSpace)
      .transform(withLowerCase)
      .transform(extractWordCount(col_word, col_count))
      .transform(withNumberId)
  }

  val sub_s1_DF = spark.read
    .option("inferSchema", "true")
    .csv("src/main/resources/subtitles_s1.json")

  val sub_s2_DF = spark.read
    .option("inferSchema", "true")
    .csv("src/main/resources/subtitles_s2.json")

  val agg_s1_DF = pipeline("w_s1", "cnt_s1")(sub_s1_DF).as("df1")
  val agg_s2_DF = pipeline("w_s2", "cnt_s2")(sub_s2_DF).as("df2")

  val joinCondition = agg_s1_DF.col("id") === agg_s2_DF.col("id")

  val resultDF = agg_s1_DF
    .join(agg_s2_DF, joinCondition, "inner")
    .select("df1.id", "w_s1", "cnt_s1", "w_s2", "cnt_s2")

  resultDF.show()
  resultDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/wordcount")

}
