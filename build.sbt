name := "datasets-windowing"

version := "1.0"

scalaVersion := "2.10.6"

// spark
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.1.0"

// spark databricks csv reader
// libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.5.0"

// https://mvnrepository.com/artifact/org.apache.avro/avro
libraryDependencies += "org.apache.avro" % "avro" % "1.8.1"

// simple csv writer
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.4"

// scala test
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.2")
