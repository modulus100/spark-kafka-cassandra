ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "spark-kafka-cassandra"
  )

// spark
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.1",
  "org.apache.spark" %% "spark-sql" % "3.2.1",
  "org.apache.spark" %% "spark-streaming" % "3.2.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.1"
)

libraryDependencies ++= Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.1.0",
  "joda-time" % "joda-time" % "2.10.14"
)
