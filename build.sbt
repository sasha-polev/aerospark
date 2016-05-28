name := "aerospike-spark"

version := "1.1.0"

organization := "com.aerospike"

scalaVersion := "2.10.6"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" 

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.1"

libraryDependencies += "com.aerospike" % "aerospike-helper-java" % "1.0.5"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

