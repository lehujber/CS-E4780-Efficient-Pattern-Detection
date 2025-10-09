val scala3Version = "3.7.3"

import sbtassembly.MergeStrategy
import sbtassembly.PathList

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.discard
  case PathList("META-INF", _ @ _*)                                => MergeStrategy.discard
  case "reference.conf"                                            => MergeStrategy.concat
  case x if x.endsWith(".SF") || x.endsWith(".DSA") || x.endsWith(".RSA") => MergeStrategy.discard
  case _                                                           => MergeStrategy.first
}

lazy val root = project
  .in(file("."))
  .settings(
    name := "metrics-collector",
    version := "1.0",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.0.0" % Test,
      "org.apache.kafka" % "kafka-clients" % "3.8.1",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "ch.qos.logback" % "logback-classic" % "1.3.5"
    )
  )
