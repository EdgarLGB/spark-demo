name := "spark-demo"

version := "1.0-SNAPSHOT"

//Older Scala Version
scalaVersion := "2.11.8"

isSnapshot := true

val overrideScalaVersion = "2.11.8"
val sparkVersion = "2.2.1"

//Override Scala Version to the above 2.11.8 version
ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}

resolvers ++= Seq(
  "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion exclude("jline", "2.12"),
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  "org.scalaj" %% "scalaj-http" % "2.3.0"
)

publishTo := {
  val nexus = "https://fastconnect.org/maven/content/repositories/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "opensource-snapshot")
  else
    Some("releases"  at nexus + "opensource")
}

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")