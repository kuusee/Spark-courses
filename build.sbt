lazy val sparkVersion = "3.2.1"
lazy val root = (project in file("."))
  .settings(
    name := "Spark-courses",
    scalaVersion := "2.13.8",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion
    )
  )
