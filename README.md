spark-examples
==============

The projects in this repository demonstrate working with genomic data accessible via the [Google Genomics API](https://developers.google.com/genomics/) using [Apache Spark](http://spark.apache.org/).

Getting Started
---------------

 1. Follow the [sign up instructions](https://developers.google.com/genomics) and download the `client_secrets.json` file. This file can be copied to the _spark-examples_ directory.

 2. Download and install [Apache Spark](https://spark.apache.org/downloads.html).

 3. If needed, install [SBT](http://www.scala-sbt.org/release/docs/Getting-Started/Setup.html)

 4. Open the file `src/main/scala/com/google/cloud/genomics/spark/examples/SearchReadsExample.scala` in an editor and change the following values to match your configuration:

```
object Examples {
  final val secretsFile = "PATH_TO/client_secrets.json"
  final val sparkPath = "SPARK_HOME_DIR"
  final val sparkMaster = "local"
  final val outputPath = "."
  final val jarPath = "PATH_TO/googlegenomics-spark-examples-assembly-1.0.jar"
```

Local Run
---------
```
  cd spark-examples
  sbt run
```

A menu should appear asking you to pick the sample to run:
```
Multiple main classes detected, select one to run:

 [1] com.google.cloud.genomics.spark.examples.SearchReadsExample1
 [2] com.google.cloud.genomics.spark.examples.SearchReadsExample2
 [3] com.google.cloud.genomics.spark.examples.SearchReadsExample3

Enter number:
```

Cluster Run
-----------
_SearchReadsExample3_ produces output files and therefore requires HDFS. Be sure to update the `Examples.outputPath` value to point to your HDFS master (e.g. _hdfs://namenode:9000/path_). Refer to the Spark [documentation](http://spark.apache.org/docs/0.9.1/spark-standalone.html#running-alongside-hadoop) for more information.


Now generate the self-contained `googlegenomics-spark-examples-assembly-1.0.jar`
```
  cd spark-examples
  sbt assembly
```
which can be found in the _spark-examples/target/scala-2.10_ directory. Ensure this JAR is copied to all workers at the same location. Run the examples as above.


Licensing
---------

See [LICENSE](LICENSE).
