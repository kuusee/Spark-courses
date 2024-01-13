import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object DfColumn extends DfColumn {
  implicit def columnToString(col: DfColumn.Value): String = col.toString
}

trait DfColumn extends Enumeration {
  val _c0,
  id, Word,
  w_s1, w_s2,
  cnt_s1, cnt_s2 = Value
}


object DataFrame_5_step_3 extends App {

  def filterHeader(df: DataFrame): DataFrame = {
    // Фильтруем данные от заголовков
    val words = "Game Of Thrones.*\\.srt"
    df.filter(!col(DfColumn._c0).rlike(words))
  }

  def filterNumberAndSpace(df: DataFrame): DataFrame = {
    // Фильтруем данные от чисел и пустых строк
    val isSpace = (col(DfColumn.Word) =!= "") && col(DfColumn.Word).cast("int").isNull
    df.filter(isSpace)
  }

  def withSplitPhrase(df: DataFrame): DataFrame = {
  // Разделяем строки на слова и создаем столбец с этими словами
     df
       .select(
            explode(
              split(lower(col(DfColumn._c0)), "\\W+")
            ).as(DfColumn.Word)
          )
  }

  def withId(df: DataFrame): DataFrame = {
    // Создаем столбец с номером строки
    df.withColumn(DfColumn.id, monotonically_increasing_id())
  }

  def extractWordCount(colWord: String, colCount: String)(df: DataFrame): DataFrame = {
    // Агрегируем слова, сортируем и забираем первые 20 строк
    df
      .groupBy(col(DfColumn.Word).as(colWord))
      .agg(count(DfColumn.Word).as(colCount))
      .orderBy(desc(colCount))
      .limit(20)
  }

  def pipeline(colWord: String = "Word",
               colCount: String = "Total")(df: DataFrame): DataFrame = {
    // Общий поток преобразований
    df
      .transform(filterHeader)
      .transform(withSplitPhrase)
      .transform(filterNumberAndSpace)
      .transform(extractWordCount(colWord, colCount))
      .transform(withId)
  }

  def joinDF(df2: DataFrame)(df1: DataFrame): DataFrame = {
    // Условие для джойна
    val joinCondition = df1.col(DfColumn.id) === df2.col(DfColumn.id)

    df1
      .join(df2, joinCondition, "inner")
      .select("df1.id", DfColumn.w_s1, DfColumn.cnt_s1, DfColumn.w_s2, DfColumn.cnt_s2)
  }

  def readFile(file: String): DataFrame = {
    spark
      .read
      .option("inferSchema", "true")
      .csv(file)
  }

  val spark = SparkSession.builder()
    .appName("DataFrame_5_step_3")
    .config("spark.master", "local")
    .getOrCreate()

  val subS1DF = readFile("src/main/resources/subtitles_s1.json")
  val subS2DF = readFile("src/main/resources/subtitles_s2.json")


  val resultDF = pipeline("w_s1", "cnt_s1")(subS1DF).as("df1")
    .transform(
      joinDF(
        pipeline("w_s2", "cnt_s2")(subS2DF).as("df2")
      )
    )

  resultDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/wordcount")
}
