import sbt.Keys._
import sbt._

/** Adds common settings automatically to all subprojects */
object Build extends AutoPlugin {

  object autoImport {
    val org = "com.sksamuel.avro4s"
    val AvroVersion = "1.9.2"
    val Log4jVersion = "1.2.17"
    val ScalatestVersion = "3.2.9"
    val Slf4jVersion = "1.7.30"
    val Json4sVersion = "4.0.5"
    val CatsVersion = "2.0.0"
    val RefinedVersion = "0.9.29"
    val ShapelessVersion = "2.3.9"
    val MagnoliaVersion = "0.17.0"
    val SbtJmhVersion = "0.3.7"
    val JmhVersion = "1.35"
  }

  import autoImport._

  def isGithubActions = sys.env.getOrElse("CI", "false") == "true"
  def releaseVersion = sys.env.getOrElse("RELEASE_VERSION", "")
  def isRelease = releaseVersion != ""
  def githubRunNumber = sys.env.getOrElse("GITHUB_RUN_NUMBER", "local")
  def ossrhUsername = sys.env.getOrElse("OSSRH_USERNAME", "")
  def ossrhPassword = sys.env.getOrElse("OSSRH_PASSWORD", "")
  def publishVersion = if (isRelease) releaseVersion else "4.1.0." + githubRunNumber + "-SNAPSHOT"

  override def trigger = allRequirements
  override def projectSettings = publishingSettings ++ Seq(
    organization := org,
    scalaVersion := "2.13.5",
    crossScalaVersions := Seq("2.12.14", "2.13.5"),
    resolvers += Resolver.mavenLocal,
    Test / parallelExecution := false,
    scalacOptions := Seq(
      "-unchecked", "-deprecation",
      "-encoding",
      "utf8",
      "-feature",
      "-language:higherKinds",
      "-language:existentials",
      "-Ybackend-parallelism",
      "8"
    ),
    javacOptions := Seq("-source", "1.8", "-target", "1.8"),
    libraryDependencies ++= Seq(
      "org.scala-lang"    % "scala-reflect"     % scalaVersion.value,
      "org.apache.avro"   % "avro"              % AvroVersion,
      "org.slf4j"         % "slf4j-api"         % Slf4jVersion          % "test",
      "log4j"             % "log4j"             % Log4jVersion          % "test",
      "org.slf4j"         % "log4j-over-slf4j"  % Slf4jVersion          % "test",
      "org.scalatest"     %% "scalatest"        % ScalatestVersion      % "test"
    )
  )

  val publishingSettings = Seq(
    publishMavenStyle := true,
    Test / publishArtifact := false,
    credentials += Credentials(
      "Sonatype Nexus Repository Manager",
      "oss.sonatype.org",
      ossrhUsername,
      ossrhPassword
    ),
    credentials += Credentials(
      "Sonatype Nexus Repository Manager",
      "s01.oss.sonatype.org",
      ossrhUsername,
      ossrhPassword
    ),
    version := publishVersion,
    publishTo := {
      if (isRelease) {
        Some("releases" at "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2")
      } else {
        Some("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
      }
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
}
