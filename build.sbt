ThisBuild / scalaVersion := "3.8.4"

name := "HNAnalyzer"

enablePlugins(JavaAppPackaging)
Compile / mainClass := Some("App")
executableScriptName := "app"

// zio-jdbc (archived, last released against zio-schema 0.4.16) pulls in an
// older zio-schema than zio-http needs; 1.8.6 wins resolution and works fine
// for our usage, but sbt's strict eviction check would otherwise fail the build.
ThisBuild / evictionErrorLevel := Level.Warn

val zioVersion = "2.1.26"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio"          % zioVersion,
  "dev.zio" %% "zio-streams"  % zioVersion % Optional,
  "dev.zio" %% "zio-http"     % "3.11.4",
  "dev.zio" %% "zio-json"     % "0.10.0",
  "dev.zio" %% "zio-jdbc"     % "0.1.2",
  "dev.zio" %% "zio-schema"   % "1.8.6",
  "org.postgresql" % "postgresql" % "42.7.4",
  "dev.zio" %% "zio-test"     % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test
)