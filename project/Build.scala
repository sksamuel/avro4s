import com.typesafe.sbt.pgp.PgpKeys
import sbt._
import sbt.Keys._

object Build extends Build {

  val org = "com.sksamuel.avro4s"
  val appVersion = "1.0.0"

  val AvroVersion = "1.7.7"
  val ScalaVersion = "2.11.7"
  val ScalatestVersion = "3.0.0-M12"
  val Slf4jVersion = "1.7.12"
  val Log4jVersion = "1.2.17"
  val ShapelessVersion = "2.2.5"

  val rootSettings = Seq(
    organization := org,
    scalaVersion := ScalaVersion,
    crossScalaVersions := Seq(ScalaVersion, "2.12.0-M3"),
    publishMavenStyle := true,
    resolvers += Resolver.mavenLocal,
    publishArtifact in Test := false,
    parallelExecution in Test := false,
    scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8"),
    javacOptions := Seq("-source", "1.7", "-target", "1.7"),
    sbtrelease.ReleasePlugin.autoImport.releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    sbtrelease.ReleasePlugin.autoImport.releaseCrossBuild := false,
    libraryDependencies ++= Seq(
      "org.scala-lang"    % "scala-reflect"     % ScalaVersion,
      "com.chuusai"       %% "shapeless"        % ShapelessVersion,
      "org.apache.avro"   % "avro"              % AvroVersion,
      "org.slf4j"         % "slf4j-api"         % Slf4jVersion,
      "log4j"             % "log4j"             % Log4jVersion % "test",
      "org.slf4j"         % "log4j-over-slf4j"  % Slf4jVersion % "test",
      "org.scalatest"     %% "scalatest"        % ScalatestVersion % "test"
    ),
    publishTo <<= version {
      (v: String) =>
        val nexus = "https://oss.sonatype.org/"
        if (v.trim.endsWith("SNAPSHOT"))
          Some("snapshots" at nexus + "content/repositories/snapshots")
        else
          Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    pomExtra := {
      <url>https://github.com/sksamuel/avro4s</url>
        <licenses>
          <license>
            <name>MIT</name>
            <url>https://opensource.org/licenses/MIT</url>
            <distribution>repo</distribution>
          </license>
        </licenses>
        <scm>
          <url>git@github.com:sksamuel/avro4s.git</url>
          <connection>scm:git@github.com:sksamuel/avro4s.git</connection>
        </scm>
        <developers>
          <developer>
            <id>sksamuel</id>
            <name>sksamuel</name>
            <url>http://github.com/sksamuel</url>
          </developer>
        </developers>
    }
  )

  lazy val root = Project("avro4s", file("."))
    .settings(rootSettings: _*)
    .settings(publish := {})
    .settings(publishArtifact := false)
    .settings(name := "avro4s")
    .aggregate(core, generator)

  lazy val core = Project("avro4s-core", file("avro4s-core"))
    .settings(rootSettings: _*)
    .settings(publish := {})
    .settings(name := "avro4s-core")

  lazy val generator = Project("avro4s-generator", file("avro4s-generator"))
    .settings(rootSettings: _*)
    .settings(publish := {})
    .settings(name := "avro4s-generator")

  lazy val json = Project("avro4s-json", file("avro4s-json"))
    .settings(rootSettings: _*)
    .settings(publish := {})
    .settings(libraryDependencies += "org.json4s" %% "json4s-native" % "3.3.0")
    .settings(name := "avro4s-json")
}
