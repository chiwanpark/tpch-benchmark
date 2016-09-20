val versions = Map(
  "scala" -> "2.11.8",
  "scopt" -> "3.5.0",
  "spark" -> "2.0.0"
)

lazy val commonSettings = Seq(
  organization := "com.chiwanpark.benchmark",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := versions("scala"),
  test in assembly := {}
)

lazy val `tpch-benchmark` = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "tpch-benchmark"
  ).aggregate(common, spark)

lazy val common = project
  .settings(commonSettings: _*)
  .settings(
    name := "tpch-benchmark-common",
    libraryDependencies ++= Seq(
      "com.github.scopt" %% "scopt" % versions("scopt")
    )
  )

lazy val spark = project
  .settings(commonSettings: _*)
  .settings(
    name := "tpch-benchmark-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % versions("spark") % "provided",
      "org.apache.spark" %% "spark-sql" % versions("spark") % "provided"
    )
  ).dependsOn(common)
