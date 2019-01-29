name := "geospark-sample"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"

libraryDependencies += "org.datasyslab" % "geospark" % "1.2.0-SNAPSHOT"
libraryDependencies += "org.datasyslab" % "geospark-sql_2.1" % "1.2.0-SNAPSHOT"
