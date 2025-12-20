import sbt.Keys._
import sbt._
import xerial.sbt.Sonatype._
import xerial.sbt.Sonatype.autoImport._

/** Adds common settings automatically to all subprojects */
object Build extends AutoPlugin {

  object autoImport {
    val org = "com.natural-transformation"
    val AvroVersion = "1.11.5"
    val ScalatestVersion = "3.2.17"
    val Slf4jVersion = "2.0.17"
    val Json4sVersion = "4.0.6"
    val CatsVersion = "2.10.0"
    val MagnoliaVersion = "1.3.18"
    val SbtJmhVersion = "0.4.8"
    val JmhVersion = "1.32"
  }

  import autoImport._

  def releaseVersion: String = sys.env.getOrElse("RELEASE_VERSION", "")
  def isRelease: Boolean = releaseVersion != ""
  private val BaseVersion: String = "5.2.0"
  def publishVersion: String = if (isRelease) releaseVersion else s"$BaseVersion-SNAPSHOT"

  private val UseMavenLocalEnvVar: String = "AVRO4S_USE_MAVEN_LOCAL"
  private def useMavenLocalResolver: Boolean = sys.env.get(UseMavenLocalEnvVar).contains("1")

  // Since OSSRH (s01.oss.sonatype.org / oss.sonatype.org) reached end-of-life for the *UI* flow,
  // we publish SNAPSHOTs via the Central Portal snapshots repository.
  //
  // For releases, sbt-sonatype still relies on Nexus 2 staging endpoints.
  // As of 2025-12, the "OSSRH staging API compatibility" host doesn't support all the endpoints sbt-sonatype uses
  // (e.g. `/service/local/staging/profile_repositories`), so we keep using the classic OSSRH host for release staging.
  //
  // Docs:
  // - https://central.sonatype.org/pages/ossrh-eol/
  // - https://central.sonatype.org/publish/publish-portal-snapshots/
  private val CentralPortalHost           = "central.sonatype.com"
  private val CentralPortalSnapshotsRepo  = "https://central.sonatype.com/repository/maven-snapshots/"
  private val OssrhHost                  = "s01.oss.sonatype.org"
  private val OssrhServiceLocal          = "https://s01.oss.sonatype.org/service/local"
  private val CentralNexusRealm           = "Sonatype Nexus Repository Manager"

  override def trigger = allRequirements
  override def projectSettings = commonSettings ++ publishingSettings 
  
  val commonSettings = Seq(
    organization       := org,
    scalaVersion := "3.3.7",
    // For reproducible builds, we avoid `mavenLocal` unless explicitly enabled.
    // Set AVRO4S_USE_MAVEN_LOCAL=1 to opt in.
    resolvers ++= (if (useMavenLocalResolver) Seq(Resolver.mavenLocal) else Nil),
    Test / parallelExecution := false,
    Test / scalacOptions ++= Seq("-Xmax-inlines:100", "-Yretain-trees"),
    javacOptions := Seq("-source", "21", "-target", "21"),    
    libraryDependencies ++= Seq(
      "org.apache.avro"   % "avro"              % AvroVersion,
      // Provide a no-op SLF4J provider in tests to avoid "No SLF4J providers were found" warnings.
      "org.slf4j"         % "slf4j-api"         % Slf4jVersion          % Test,
      "org.slf4j"         % "slf4j-nop"         % Slf4jVersion          % Test,
      "org.slf4j"         % "log4j-over-slf4j"  % Slf4jVersion          % Test,
      "org.scalatest"     % "scalatest_3"       % ScalatestVersion      % Test
    )
  )

  val publishingSettings = Seq(
    publishMavenStyle := true,
    Test / publishArtifact := false,

    credentials ++= {
      val credsFile = Path.userHome / ".sbt" / "sonatype_credentials"
      // We support either:
      // - ~/.sbt/sonatype_credentials (local)
      // - OSSRH_USERNAME / OSSRH_PASSWORD (CI or local env)
      //
      // The same Central user token can authenticate both:
      // - Central Portal snapshots repository (Nexus 3)
      // - OSSRH staging API compatibility service (Nexus 2-like API)
      val userPassFromFile: Option[(String, String)] =
        if (credsFile.exists()) {
          // We parse the file ourselves instead of using Credentials(file) because
          // `Credentials(file)` returns an internal credentials type that doesn't expose user/pass.
          val kv =
            IO.readLines(credsFile)
              .iterator
              .map(_.trim)
              .filter(l => l.nonEmpty && !l.startsWith("#"))
              .flatMap { l =>
                l.split("=", 2) match {
                  case Array(k, v) => Some((k.trim.toLowerCase, v.trim))
                  case _           => None
                }
              }
              .toMap

          val user = kv.get("user").orElse(kv.get("username")).getOrElse("")
          val pass = kv.getOrElse("password", "")

          if (user.nonEmpty && pass.nonEmpty) Some((user, pass)) else None
        } else None

      val userPassFromEnv: Option[(String, String)] = {
        val user = sys.env.getOrElse("OSSRH_USERNAME", "")
        val pass = sys.env.getOrElse("OSSRH_PASSWORD", "")
        if (user.nonEmpty && pass.nonEmpty) Some((user, pass)) else None
      }

      val userPass = userPassFromFile.orElse(userPassFromEnv)

      userPass match {
        case Some((user, pass)) =>
          Seq(
            // Central Portal snapshots (for -SNAPSHOT versions)
            Credentials(CentralNexusRealm, CentralPortalHost, user, pass),
            // OSSRH staging (for releases)
            Credentials(CentralNexusRealm, OssrhHost, user, pass)
          )
        case None =>
          // No file and no env variables means no credentials are provided
          Nil
      }
    },

    version := publishVersion,
    pomIncludeRepository := { _  => false },
    // For snapshots, publish directly to the Central Portal snapshots repository.
    // For releases, keep using sbt-sonatype's bundle flow (which uses the staging API host below).
    publishTo := {
      if (isSnapshot.value) Some("central-portal-snapshots" at CentralPortalSnapshotsRepo)
      else sonatypePublishToBundle.value
    },
    sonatypeCredentialHost := OssrhHost,
    sonatypeRepository := OssrhServiceLocal,
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
