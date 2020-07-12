name := "hoodi-demo"

version := "0.1"

scalaVersion := "2.11.12"
//libraryDependencies += "com.uber.hoodie" % "hoodie" % "0.4.7" pomOnly()
libraryDependencies += "org.apache.hudi" %% "hudi-spark" % "0.5.2-incubating"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-avro" % "2.4.4"
libraryDependencies += "org.apache.parquet" % "parquet-avro" % "1.8.1"
libraryDependencies += "log4j" % "log4j" % "1.2.17"

//configuration file dependency
libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "com.typesafe" % "config" % "1.2.1")