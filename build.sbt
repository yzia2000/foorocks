ThisBuild / scalaVersion := "2.13.10"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.zims"
ThisBuild / organizationName := "zims"

Global / onChangedBuildSource := ReloadOnSourceChanges

val zioSchemaVersion = "0.2.1"

lazy val root = (project in file("."))
  .settings(
    name := "foorocks",
    libraryDependencies ++= List(
      "dev.zio" %% "zio-streams" % "2.0.1",
      "dev.zio" %% "zio-logging" % "2.1.1",
      "dev.zio" %% "zio-schema" % "0.2.1",
      "dev.zio" %% "zio-kafka" % "2.0.1",
      "io.d11" %% "zhttp" % "2.0.0-RC11",
      "dev.zio" %% "zio-schema-protobuf" % zioSchemaVersion,
      "dev.zio" %% "zio-schema-avro" % zioSchemaVersion,
      "dev.zio" %% "zio-schema-json" % zioSchemaVersion,
      "dev.zio" %% "zio-schema-derivation" % zioSchemaVersion,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value % "provided",
      "dev.zio" %% "zio-rocksdb" % "0.4.2",
      "org.slf4j" % "slf4j-api" % "2.0.3",
      "org.slf4j" % "slf4j-simple" % "2.0.3"
    )
  )
