name := "akka-redis-streams-example"

version := "1.0"

scalaVersion := "2.13.1"

lazy val akkaVersion = "2.6.10"

libraryDependencies ++= Seq(
  "org.redisson" % "redisson" % "3.13.6",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.1.0" % Test
)
