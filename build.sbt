// Note: settings common to all subprojects are defined in project/GlobalPlugin.scala

// The root project is implicit, so we don't have to define it.
// We do need to prevent publishing for it, though:
publishArtifact := false
publish := {}

val `avro4s-macros` = project.in(file("avro4s-macros"))
  .settings(libraryDependencies += "com.chuusai" %% "shapeless" % "2.3.3")

val `avro4s-core` = project.in(file("avro4s-core"))
  .dependsOn(`avro4s-macros`)

val `avro4s-json` = project.in(file("avro4s-json"))
  .dependsOn(`avro4s-core`)
  .settings(libraryDependencies += "org.json4s" %% "json4s-native" % "3.5.3")