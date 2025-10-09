val scala3Version = "3.7.3"

lazy val root = project
  .in(file("."))
  .settings(
    name := "metrics-collector",
    version := "1.0",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.0.0" % Test,
      "org.apache.kafka" % "kafka-clients" % "3.8.1",
    )
  )
