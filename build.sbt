import AssemblyKeys._

seq(assemblySettings: _*)

name := "googlegenomics-spark-examples"

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions += "-target:jvm-1.7"

val sparkVersion = "1.3.1"

val genomicsUtilsVersion = "v1beta2-0.33-SNAPSHOT"
  
  
libraryDependencies ++= Seq(
  "com.google.cloud.genomics" % "google-genomics-utils" % genomicsUtilsVersion excludeAll(
      ExclusionRule(organization = "org.mortbay.jetty", name = "servlet-api"),
      ExclusionRule(organization = "com.google.guava", name = "guava-jdk5"),
      ExclusionRule(organization = "com.fasterxml.jackson.core", name = "jackson-core"),
      ExclusionRule(organization = "javax", name="javaee-api")),
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.rogach" %% "scallop" % "0.9.5",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

resolvers += Resolver.mavenLocal

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
    case x => old(x)
  }
}