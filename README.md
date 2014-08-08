spark-examples
==============

The projects in this repository demonstrate working with genomic data accessible via the [Google Genomics API](https://developers.google.com/genomics/) using [Apache Spark](http://spark.apache.org/).

Getting Started
---------------

 1. Follow the [sign up instructions](https://developers.google.com/genomics) and download the `client_secrets.json` file. This file can be copied to the _spark-examples_ directory.

 2. Download and install [Apache Spark](https://spark.apache.org/downloads.html).

 3. If needed, install [SBT](http://www.scala-sbt.org/release/docs/Getting-Started/Setup.html)


Local Run
---------
From the `spark-examples` directory run `sbt run`

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

For example: 

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

### Troubleshooting:

If you are seeing `java.lang.OutOfMemoryError: PermGen space` errors, set the following SBT_OPTS flag:
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


Run on Google Compute Engine
-----------------------------
Follow the [instructions](https://groups.google.com/forum/#!topic/gcp-hadoop-announce/EfQms8tK5cE) to setup Google Cloud and install the Cloud SDK. At the end of the process you should be able to launch a test instance and login into it using gcutil.


Create a Google Cloud Storage bucket to store the configuration of the cluster.

```
gsutil mb gs://<bucket-name>
```

Run [bdutil](https://groups.google.com/forum/#!topic/gcp-hadoop-announce/EfQms8tK5cE) to create a Spark cluster.

```
./bdutil -e extensions/spark/spark_shark_env.sh -b <configbucket> deploy

```

Upload the following files to provide the workers with appropriate credentials.

```
gcutil push --ssh_user=hadoop hs-ghfs-nn ~/.store client_secrets.json .
gcutil push --ssh_user=hadoop hs-ghfs-dn-1 ~/.store client_secrets.json .
gcutil push --ssh_user=hadoop hs-ghfs-dn-0 ~/.store client_secrets.json .
```
(This step assumes thad you already ran the example locally and generated the credentials.)

Upload the assembly jar to the master node.
```
gcutil push --ssh_user=hadoop hs-ghfs-nn \
target/scala-2.10/googlegenomics-spark-examples-assembly-1.0.jar .
```

To run the examples on GCE, login to the master node and launch the examples using the `scala-class` script.
```bash
# Login into the master node
gcutil ssh --ssh_user=hadoop hs-ghfs-nn

# Add the jar to the classpath
export SPARK_CLASSPATH=googlegenomics-spark-examples-assembly-1.0.jar

# Run the examples
spark-class com.google.cloud.genomics.spark.examples.SearchReadsExample1 \
--client-secrets /home/hadoop/client_secrets.json \ 
--spark-master spark://hs-ghfs-nn:7077 \
--jar-path googlegenomics-spark-examples-assembly-1.0.jar
```

The --jar-path will take care of copying the jar to all the workers before launching the tasks.


### Debuging 
To debug the jobs from the Spark web UI, either setup a SOCKS5 proxy 
or open the web UI ports on your instances.

To use your SOCKS5 proxy with port 12345 on Firefox:

```
bdutil socksproxy 12345
Go to Edit -> Preferences -> Advanced -> Network -> Settings
Enable "Manual proxy configuration" with a SOCKS host "localhost" on port 12345
Force the DNS resolution to occur on the remote proxy host rather than locally.
Go to "about:config" in the URL bar
Search for "socks" to toggle "network.proxy.socks_remote_dns" to "true".
Visit the web UIs exported by your cluster!
http://hs-ghfs-nn:8080 for Spark
```

To open the web UI ports.

```
gcutil addfirewall default-allow-8080 \
--description="Incoming http 8080 allowed." \
--allowed="tcp:4040,8080,8081" \
--target_tags="http-8080-server"
```
From the [developers console](https://console.developers.google.com/project),
add the `http-8080-server` tag to the master and worker instances or follow the instructions
[here](https://developers.google.com/compute/docs/instances#tags) to do it from the command line.

Then point the browser to `http://<master-node-public-ip>:8080`

Licensing
---------

See [LICENSE](LICENSE).
