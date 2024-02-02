import org.apache.spark.sql.SparkSession

trait Context {
  val appName: String

  lazy val spark = createSession(appName)

  private def createSession(appName: String) = {
    SparkSession.builder()
      .appName(appName)
      .master("local")
      .config("spark.executor.memory", "3g")
      .config("spark.executor.processTreeMetrics.enabled", "false")
      .getOrCreate()
  }
}
