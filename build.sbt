lazy val root = project
  .in(file("."))
  .aggregate(core)
  .settings(
    name := "avro4s",
    version := "5.0.0",
    scalaVersion := "3.0.0-M3",
    useScala3doc := true
  )

lazy val core = project
  .in(file("avro4s-core"))
  .settings(
    name := "avro4s-core",
    version := "5.0.0",
    scalaVersion := "3.0.0-M3",
    useScala3doc := true,
    libraryDependencies ++= List(
      "org.apache.avro" % "avro" % "1.10.1",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "org.scalatest" % "scalatest_3.0.0-M3" % "3.2.3" % "test"
    )
  )