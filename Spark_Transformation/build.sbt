ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "testing(SBT)"
  )

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= {
  val sparkVer = "3.2.1"
  val hadoopVer = "2.7.4"
  val postgresVersion = "42.2.2"

  Seq(
    // hadoop
    // "org.apache.hadoop" % "hadoop-common" % hadoopVer,

    // spark
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer,
    // logging
    "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
    "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
    // postgres for DB connectivity
    "org.postgresql" % "postgresql" % postgresVersion
  )
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

