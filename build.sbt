name := "googlegenomics-spark-examples"

version := "1.0"

scalaVersion := "2.10.4"

val googleAPIVersion = "1.19.0"
val googleAPIGenomicsVersion = "v1beta2-rev3-1.19.0"

val sparkVersion = "1.1.0"

val genomicsUtilsVersion = "v1beta2-0.13"

val excludeMortbayJetty = ExclusionRule(organization = "org.mortbay.jetty", name = "servlet-api")

val excludeGuavaJdk5 = ExclusionRule(organization = "com.google.guava", name = "guava-jdk5")

libraryDependencies ++= Seq(
  "com.google.api-client" % "google-api-client" % googleAPIVersion excludeAll(excludeGuavaJdk5),
  "com.google.api-client" % "google-api-client-java6" % googleAPIVersion,
  "com.google.apis" % "google-api-services-genomics" % googleAPIGenomicsVersion,
  "com.google.oauth-client" % "google-oauth-client-java6" % googleAPIVersion,
  "com.google.oauth-client" % "google-oauth-client-jetty" % googleAPIVersion excludeAll(excludeMortbayJetty), 
  "com.google.http-client" % "google-http-client" % googleAPIVersion,
  "com.google.http-client" % "google-http-client-jackson2" % googleAPIVersion,
  "com.google.cloud.genomics" % "google-genomics-utils" % genomicsUtilsVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.rogach" %% "scallop" % "0.9.5",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/"
)
