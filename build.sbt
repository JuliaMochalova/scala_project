scalaVersion := "2.11.12"
organization := "ru.ml"

name := "LookAlikeDmp"
val mainVersion = "0.4.7"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-sql",
  "org.apache.spark" %% "spark-mllib"
).map(_ % sparkVersion % Provided)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  "ai.catboost" % "catboost-spark_2.4_2.11" % "1.0.6",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
  "com.github.scopt" %% "scopt" % "3.7.1",
  "joda-time" % "joda-time" % "2.10.5",
  "com.typesafe" % "config" % "1.4.1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "org.json4s" %% "json4s-native" % "3.5.3",
  "com.github.pureconfig" %% "pureconfig" % "0.14.0",
  "org.mlflow" % "mlflow-client" % "2.2.1",
  "ru" % "clickhouse-utilities_2.11" % "0.3.0"
)

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)).evaluated

scalacOptions ++= Seq("-deprecation", "-unchecked")
version := {
  val branch = sys.env.getOrElse("CI_COMMIT_REF_NAME", mainVersion)
  (branch match {
    case "master" | "main" => mainVersion
    case _ => branch
  }) + sys.env.getOrElse("SNAPSHOT", "")
}
publishConfiguration := publishConfiguration.value.withOverwrite(true)
publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

externalResolvers := Seq(
  Resolver.mavenLocal,
  "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  Resolver.mavenCentral
)

Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
Test / parallelExecution := false

assembly / assemblyJarName := s"${name.value}-${version.value}.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
assembly / artifact := {
  val art = (assembly / artifact).value
  art.withClassifier(Some("fat"))
}
addArtifact(assembly / artifact, assembly)
