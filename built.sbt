name := "Spark ProbSkyline Project"

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies ++= Seq(
      "junit" % "junit" % "4.7" % "test",
      "org.apache.spark" %% "spark-core" % "1.2.0",
      "com.typesafe" % "config" % "0.4.0",
      "org.json4s" %% "json4s-native" % "3.2.10"
)
