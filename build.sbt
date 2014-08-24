name := "googlegenomics-spark-examples"

version := "1.0"

scalaVersion := "2.10.3"

val googleAPIVersion = "1.18.0-rc"
val googleAPIGenomicsVersion = "v1beta-rev17-1.18.0-rc"

val sparkVersion = "1.0.2"

val excludeMortbayJetty = ExclusionRule(organization = "org.mortbay.jetty", name = "servlet-api")

libraryDependencies ++= Seq(
  "com.google.api-client" % "google-api-client" % googleAPIVersion,
  "com.google.api-client" % "google-api-client-java6" % googleAPIVersion,
  "com.google.apis" % "google-api-services-genomics" % googleAPIGenomicsVersion,
  "com.google.oauth-client" % "google-oauth-client-java6" % googleAPIVersion,
  "com.google.oauth-client" % "google-oauth-client-jetty" % googleAPIVersion excludeAll(excludeMortbayJetty), 
  "com.google.http-client" % "google-http-client" % googleAPIVersion,
  "com.google.http-client" % "google-http-client-jackson2" % googleAPIVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.rogach" %% "scallop" % "0.9.5"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/"
)
