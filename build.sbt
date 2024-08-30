import sbt.project

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-test",

      // 添加 Spark 依赖项
      libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.2",
      "org.apache.spark" %% "spark-sql" % "3.5.2"
    )
  )
