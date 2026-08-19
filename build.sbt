ThisBuild / scalaVersion := "3.8.4"

val zioVersion = "2.1.26"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio"          % zioVersion,
  "dev.zio" %% "zio-streams"  % zioVersion % Optional,
  "dev.zio" %% "zio-http"     % "3.11.4",
  "dev.zio" %% "zio-test"     % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test
)