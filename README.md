spark-examples
==============

The projects in this repository demonstrate working with genomic data accessible via the [Google Genomics API](https://developers.google.com/genomics/) using [Apache Spark](http://spark.apache.org/).

Getting Started
---------------

 1. Follow the [sign up instructions](https://developers.google.com/genomics) and download the `client_secrets.json` file. This file can be copied to the _spark-examples_ directory.

 2. Download and install [Apache Spark](https://spark.apache.org/downloads.html).

 3. If needed, install [SBT](http://www.scala-sbt.org/release/docs/Getting-Started/Setup.html)


Local Run
=========
From the spark-examples directory run 
```
sbt run
```

Use the following flags to match your runtime configuration:

```
$ sbt "run --help"
  -c, --client-secrets  <arg>    (default = client_secrets.json)
  -j, --jar-path  <arg>
                                (default = target/scala-2.10/googlegenomics-spark-examples-assembly-1.0.jar)
  -o, --output-path  <arg>       (default = .)
  -s, --spark-master  <arg>      (default = local[2])
      --spark-path  <arg>        (default = )
      --help                    Show help message
```

For example to run spark in local mode with four cores and read the client_secrets.json file 
from the parent directory:

```
$ sbt "run --client-secrets ../client_secrets.json --spark-master local[4]"
```


A menu should appear asking you to pick the sample to run:
```
Multiple main classes detected, select one to run:

 [1] com.google.cloud.genomics.spark.examples.SearchReadsExample1
 [2] com.google.cloud.genomics.spark.examples.SearchReadsExample2
 [3] com.google.cloud.genomics.spark.examples.SearchReadsExample3

Enter number:
```

If you are getting a `java.lang.OutOfMemoryError: PermGen space` error, set the following SBT_OPTS flag:
```
export SBT_OPTS='-XX:MaxPermSize=256m'
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
