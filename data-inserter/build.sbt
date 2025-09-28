val scala3Version = "3.7.3"

lazy val root = project
  .in(file("."))
  .settings(
    name := "data-inserter",
    version := "1.0",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.8.1",
    libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "2.0.0"
  )
