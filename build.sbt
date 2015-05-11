name := "googlegenomics-spark-examples"

version := "1.0"

scalaVersion := "2.10.4"

val googleAPIVersion = "1.20.0"
val googleAPIGenomicsVersion = "v1beta2-rev34-1.20.0"

val sparkVersion = "1.3.0"

val genomicsUtilsVersion = "v1beta2-0.23"

val excludeMortbayJetty = ExclusionRule(organization = "org.mortbay.jetty", name = "servlet-api")

val excludeGuavaJdk5 = ExclusionRule(organization = "com.google.guava", name = "guava-jdk5")

val excludeJackson = ExclusionRule(organization = "com.fasterxml.jackson.core", name = "jackson-core")

libraryDependencies ++= Seq(
  "com.google.cloud.genomics" % "google-genomics-utils" % genomicsUtilsVersion excludeAll(excludeGuavaJdk5, excludeMortbayJetty, excludeJackson),
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.rogach" %% "scallop" % "0.9.5",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/"
)
