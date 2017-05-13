name := "AprendiendoAkkaStreams"

version := "1.0"

resolvers ++= Seq(
  "SpinGo OSS" at "http://spingo-oss.s3.amazonaws.com/repositories/releases"
)

scalaVersion := "2.12.0"
val opRabbitVersion = "2.0.0-rc1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.4.17",
  "com.typesafe.akka" %% "akka-actor" % "2.4.17",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.4.17" % "test",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",

  "com.spingo" %% "op-rabbit-core" % opRabbitVersion,
  "com.spingo" %% "op-rabbit-play-json" % opRabbitVersion,
  "com.spingo" %% "op-rabbit-akka-stream" % opRabbitVersion
)