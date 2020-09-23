val dottyVersion = "0.27.0-RC1"

lazy val root = project
  .in(file("."))
  .settings(
    name := "avro4s",
    version := "0.1.0",
    scalaVersion := dottyVersion
  )

lazy val core = project
  .in(file("avro4s-core"))
  .settings(
    name := "avro4s-core",
    version := "0.1.0",
    scalaVersion := dottyVersion,
    libraryDependencies ++= List(
      "org.apache.avro" % "avro" % "1.10.0"
    )
  )