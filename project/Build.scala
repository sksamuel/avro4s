import sbt.Keys.*
import sbt.*

/** Adds common settings automatically to all subprojects */
object Build extends AutoPlugin {

  object autoImport {
    val AvroVersion = "1.12.1"
    val Log4jVersion = "1.2.17"
    val ScalatestVersion = "3.2.17"
    val Slf4jVersion = "2.0.17"
    val Json4sVersion = "3.6.11"
    val CatsVersion = "2.7.0"
    val RefinedVersion = "0.9.26"
    val ShapelessVersion = "2.3.7"
    val MagnoliaVersion = "1.3.18"
    val SbtJmhVersion = "0.3.7"
    val JmhVersion = "1.32"
  }

  import autoImport._

  def isGithubActions: Boolean = sys.env.getOrElse("CI", "false") == "true"
  def releaseVersion: String = sys.env.getOrElse("RELEASE_VERSION", "")
  def isRelease: Boolean = releaseVersion != ""
  def githubRunNumber: String = sys.env.getOrElse("GITHUB_RUN_NUMBER", "local")
  def ossrhUsername: String = sys.env.getOrElse("OSSRH_USERNAME", "")
  def ossrhPassword: String = sys.env.getOrElse("OSSRH_PASSWORD", "")
  def publishVersion: String = if (isRelease) releaseVersion else "5.0.0." + githubRunNumber + "-SNAPSHOT"

  override def trigger = allRequirements
  override def projectSettings = publishingSettings ++ Seq(
    scalaVersion := "3.3.7",
    resolvers += Resolver.mavenLocal,
    Test / parallelExecution := false,
    Test / scalacOptions ++= Seq("-Xmax-inlines:100", "-Yretain-trees"),
    javacOptions := Seq("-source", "1.8", "-target", "1.8"),    
    libraryDependencies ++= Seq(
      "org.apache.avro"   % "avro"              % AvroVersion,
      "org.slf4j"         % "slf4j-api"         % Slf4jVersion          % "test",
      "log4j"             % "log4j"             % Log4jVersion          % "test",
      "org.slf4j"         % "log4j-over-slf4j"  % Slf4jVersion          % "test",
      "org.scalatest"     % "scalatest_3"       % ScalatestVersion      % "test"
    )
  )

  val publishingSettings = Seq(
    pomIncludeRepository := { _ => false },
    publishMavenStyle := true,
    Test / publishArtifact := false,
    publishTo := {
      val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
      if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
      else localStaging.value
    },
    credentials += Credentials(
      null,
      "central.sonatype.com",
      ossrhUsername,
      ossrhPassword
    ),
    version := publishVersion,
    organization := "com.sksamuel.avro4s",
    homepage := Some(url("https://github.com/sksamuel/avro4s")),
    scmInfo := Some(ScmInfo(url("https://github.com/sksamuel/avro4s"), "scm:git@github.com:sksamuel/avro4s.git")),
    licenses := List("The Apache 2.0 License" -> url("https://opensource.org/licenses/Apache-2.0")),
    developers := List(Developer("sksamuel", "sksamuel", "", url("http://github.com/sksamuel")))
  )
}

