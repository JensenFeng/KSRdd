name := "ksrdd"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.0" % "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-log4j12" % "1.7.10",
  "org.slf4j" % "slf4j-api" % "1.7.10",
  "org.apache.zookeeper" % "zookeeper" % "3.4.6",
  "log4j" % "log4j" % "1.2.17",
  "org.scala-lang" % "scala-library" % "2.10.6",
  "org.scala-lang" % "scala-compiler" % "2.10.0"
)

assemblyOption  in assembly := (assemblyOption in assembly).value.copy(includeScala = false)