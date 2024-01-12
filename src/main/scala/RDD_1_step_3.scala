import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}
object RDD_1_step_3 extends App {
  val spark = SparkSession.builder()
    .appName("RDD_1_step_3")
    .config("spark.master", "local")
    .getOrCreate()

  // Создадим список id значений List(id-0, id-1, id-2, id-3, id-4)
  val ids = List.fill(5)("id-").zipWithIndex.map(x => x._1 + x._2)
  println(ids)

  import spark.implicits._

  // Создадим датасет
  val idsDS: Dataset[String] = ids.toDF.as[String]

  // При запуске локально данные помещаются в одну партицию,
  // поэтому для более наглядной демонстрации потребуется переразбить данные на партиции.
  val idsPartitioned = idsDS.repartition(6)

  // Проверим, что количество партиций соответствует заданному числу:
  val numPartitions = idsPartitioned.rdd.partitions.length
  println(s"partitions = ${numPartitions}")

  // Посмотрим, как данные распределены по партициям
  idsPartitioned.rdd
    .mapPartitionsWithIndex(
      (partition: Int, it: Iterator[String]) =>
        it.toList.map(id => {
          println(s" partition = $partition; id = $id")
        }).iterator
    ).collect
}
