ThisBuild / scalaVersion := "2.13.10"
ThisBuild / version := "0.1.0-SNAPSHOT"

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / fork := true
Global / connectInput := true
Global / cancelable := true

val zioSchemaVersion = "0.2.1"

lazy val root = (project in file("."))
  .settings(
    name := "foorocks",
    libraryDependencies ++= List(
      "dev.zio" %% "zio" % "2.0.5",
      "dev.zio" %% "zio-streams" % "2.0.5",
      "dev.zio" %% "zio-kafka" % "2.0.2",
      "dev.zio" %% "zio-http" % "0.0.3",
      "dev.zio" %% "zio-schema" % zioSchemaVersion,
      "dev.zio" %% "zio-schema-protobuf" % zioSchemaVersion,
      "dev.zio" %% "zio-schema-avro" % zioSchemaVersion,
      "dev.zio" %% "zio-schema-json" % zioSchemaVersion,
      "dev.zio" %% "zio-schema-derivation" % zioSchemaVersion,
      "dev.zio" %% "zio-rocksdb" % "0.4.2",
      "ch.qos.logback" % "logback-classic" % "1.4.5"
    )
  )
