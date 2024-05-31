ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.1"
)

lazy val root = (project in file("."))
  .settings(
    name := "DSAirbnb"
  )
