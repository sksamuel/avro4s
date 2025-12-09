import sbt.Keys._
import sbt._
import xerial.sbt.Sonatype._
import xerial.sbt.Sonatype.autoImport._

/** Adds common settings automatically to all subprojects */
object Build extends AutoPlugin {

  object autoImport {
    val org = "com.natural-transformation"
    val AvroVersion = "1.11.5"
    val Log4jVersion = "1.2.17"
    val ScalatestVersion = "3.2.17"
    val Slf4jVersion = "2.0.17"
    val Json4sVersion = "4.0.6"
    val CatsVersion = "2.10.0"
    val RefinedVersion = "0.9.26"
    val ShapelessVersion = "2.3.7"
    val MagnoliaVersion = "1.3.18"
    val SbtJmhVersion = "0.4.8"
    val JmhVersion = "1.32"
  }

  import autoImport._

  def isGithubActions: Boolean = sys.env.getOrElse("CI", "false") == "true"
  def releaseVersion: String = sys.env.getOrElse("RELEASE_VERSION", "")
  def isRelease: Boolean = releaseVersion != ""
  def publishVersion: String = if (isRelease) releaseVersion else "5.1.0" + "-SNAPSHOT"

  override def trigger = allRequirements
  override def projectSettings = commonSettings ++ publishingSettings 
  
  val commonSettings = Seq(
    organization       := org,
    scalaVersion := "3.3.7",
    resolvers += Resolver.mavenLocal,
    Test / parallelExecution := false,
    Test / scalacOptions ++= Seq("-Xmax-inlines:100", "-Yretain-trees"),
    javacOptions := Seq("-source", "21", "-target", "21"),    
    libraryDependencies ++= Seq(
      "org.apache.avro"   % "avro"              % AvroVersion,
      "org.slf4j"         % "slf4j-api"         % Slf4jVersion          % "test",
      "log4j"             % "log4j"             % Log4jVersion          % "test",
      "org.slf4j"         % "log4j-over-slf4j"  % Slf4jVersion          % "test",
      "org.scalatest"     % "scalatest_3"       % ScalatestVersion      % "test"
    )
  )

  val publishingSettings = Seq(
    publishMavenStyle := true,
    Test / publishArtifact := false,

    credentials ++= {
      val credsFile = Path.userHome / ".sbt" / "sonatype_credentials"
      if (credsFile.exists()) {
        // If the file exists, use it (for local publishing)
        Seq(Credentials(credsFile))
      } else {
        // Otherwise, read credentials from env variables if available.
        val user = sys.env.getOrElse("OSSRH_USERNAME", "")
        val pass = sys.env.getOrElse("OSSRH_PASSWORD", "")
        if (user.nonEmpty && pass.nonEmpty) {
          Seq(Credentials(
            realm = "Sonatype Nexus Repository Manager",
            host = "s01.oss.sonatype.org",
            userName = user,
            passwd = pass
          ))
        } else {
          // No file and no env variables means no credentials are provided
          Nil
        }
      }
    },

    version := publishVersion,
    pomIncludeRepository := { _ => false },
    publishTo := sonatypePublishToBundle.value,
    sonatypeCredentialHost := "s01.oss.sonatype.org",
    sonatypeRepository := "https://s01.oss.sonatype.org/service/local",
    sonatypeProfileName := "com.natural-transformation",
    sonatypeProjectHosting := Some(GitHubHosting("natural-transformation", "avro4s", "zli@natural-transformation.com")),
    licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
    homepage := Some(url("https://github.com/natural-transformation/avro4s")),
    scmInfo := Some(ScmInfo(
      url("https://github.com/natural-transformation/avro4s"),
      "scm:git:git@github.com:natural-transformation/avro4s.git"
    )),
    developers := List(Developer(
      id = "natural-transformation",
      name = "Natural Transformation BV",
      email = "zli@natural-transformation.com",
      url = url("https://natural-transformation.com")
    ))
  )
}
