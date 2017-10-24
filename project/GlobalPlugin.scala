import com.typesafe.sbt.pgp.PgpKeys
import sbt.Keys._
import sbt._
import sbtrelease.ReleasePlugin

/** Adds common settings automatically to all subprojects */
object GlobalPlugin extends AutoPlugin {

    val org = "com.sksamuel.avro4s"

    val AvroVersion = "1.8.2"
    val ScalatestVersion = "3.0.4"
    val ScalaVersion = "2.12.3"
    val Slf4jVersion = "1.7.25"
    val Log4jVersion = "1.2.17"

    override def requires = ReleasePlugin
    override def trigger = allRequirements
    override def projectSettings = publishingSettings ++ Seq(
        organization := org,
        scalaVersion := ScalaVersion,
        //crossScalaVersions := Seq("2.11.8", "2.12.1"),
        resolvers += Resolver.mavenLocal,
        parallelExecution in Test := false,
        scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-Ywarn-unused-import",
            "-Xfatal-warnings", "-feature", "-language:existentials"
        ),
        javacOptions := Seq("-source", "1.8", "-target", "1.8"),
        libraryDependencies ++= Seq(
            "org.scala-lang" % "scala-reflect" % scalaVersion.value,
            "org.apache.avro" % "avro" % AvroVersion,
            "org.slf4j" % "slf4j-api" % Slf4jVersion,
            "log4j" % "log4j" % Log4jVersion % Test,
            "org.slf4j" % "log4j-over-slf4j" % Slf4jVersion % Test,
            "org.scalatest" %% "scalatest" % ScalatestVersion % Test
        )
    )

    val publishingSettings = Seq(
        publishMavenStyle := true,
        publishArtifact in Test := false,
        ReleasePlugin.autoImport.releasePublishArtifactsAction := PgpKeys.publishSigned.value,
        ReleasePlugin.autoImport.releaseCrossBuild := true,
        publishTo := {
            val corporateRepo = "http://toucan.simplesys.lan/"
            if (isSnapshot.value)
                Some("snapshots" at corporateRepo + "artifactory/libs-snapshot-local")
            else
                Some("releases" at corporateRepo + "artifactory/libs-release-local")
        },
        credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
    )
}
