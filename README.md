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

 [1] com.google.cloud.genomics.spark.examples.SearchVariantsExampleKlotho
 [2] com.google.cloud.genomics.spark.examples.SearchVariantsExampleBRCA1
 [3] com.google.cloud.genomics.spark.examples.SearchReadsExample1
 [4] com.google.cloud.genomics.spark.examples.SearchReadsExample2
 [5] com.google.cloud.genomics.spark.examples.SearchReadsExample3
 [6] com.google.cloud.genomics.spark.examples.SearchReadsExample4
 [7] com.google.cloud.genomics.spark.examples.VariantsPcaDriver
 
Enter number:
```

### Troubleshooting:

If you are seeing `java.lang.OutOfMemoryError: PermGen space` errors, set the following SBT_OPTS flag:
```
export SBT_OPTS='-XX:MaxPermSize=256m'
``` 

Cluster Run
-----------
`SearchReadsExample3` produces output files and therefore requires HDFS. Be sure to specify the `--output-path` flag to point to your HDFS master (e.g. _hdfs://namenode:9000/path_). Refer to the Spark [documentation](http://spark.apache.org/docs/0.9.1/spark-standalone.html#running-alongside-hadoop) for more information.

To run the code on a cluster generate the self-contained `googlegenomics-spark-examples-assembly-1.0.jar`
```
  cd spark-examples
  sbt assembly
```
which can be found in the _spark-examples/target/scala-2.10_ directory. Ensure this JAR is copied to all workers at the same location. Run the examples as above.

Run on Google Compute Engine
-----------------------------
Follow the [instructions](https://groups.google.com/forum/#!topic/gcp-hadoop-announce/EfQms8tK5cE) to setup Google Cloud and install the Cloud SDK. At the end of the process you should be able to launch a test instance and login into it using `gcutil`.


Create a Google Cloud Storage bucket to store the configuration of the cluster.

```
gsutil mb gs://<bucket-name>
```

Run [Click-to-deploy Hadoop](https://cloud.google.com/solutions/hadoop/click-to-deploy) or use [bdutil](https://groups.google.com/forum/#!topic/gcp-hadoop-announce/EfQms8tK5cE) directly to create a Spark cluster.

```
./bdutil -e extensions/spark/spark_env.sh -b <configbucket> deploy

```

Upload the `client_secrets.json` file to the master node.

```
gcutil push hadoop-m client_secrets.json .
```

Upload the assembly jar to the master node.
```
gcutil push hadoop-m target/scala-2.10/googlegenomics-spark-examples-assembly-1.0.jar .
```

To run the examples on GCE, login to the master node and launch the examples using the `scala-class` script.
```bash
# Login into the master node
gcutil ssh hadoop-m

# Add the jar to the classpath
export SPARK_CLASSPATH=googlegenomics-spark-examples-assembly-1.0.jar

# Run the examples
spark-class com.google.cloud.genomics.spark.examples.SearchReadsExample1 \
--client-secrets client_secrets.json \ 
--spark-master spark://hadoop-m:7077 \
--jar-path googlegenomics-spark-examples-assembly-1.0.jar
```

When prompted copy the authentication URL to a browser, authorize the application and copy 
the authorization code. This step will copy the access token to all the workers.

The --jar-path will take care of copying the jar to all the workers before launching the tasks.

### Running PCA variant analysis on GCE
To run the [variant PCA analysis](https://github.com/googlegenomics/spark-examples/blob/master/src/main/scala/com/google/cloud/genomics/spark/examples/VariantsPca.scala) on GCE  make sure you have followed all the steps on the previous section and that you are able to run at least one of the examples.

Run the example PCA analysis for BCRA1 on the [1000 Genomes Project dataset](https://cloud.google.com/genomics/data/1000-genomes).
```
export SPARK_CLASSPATH=googlegenomics-spark-examples-assembly-1.0.jar
spark-class com.google.cloud.genomics.spark.examples.VariantsPcaDriver \
--client-secrets client_secrets.json \ 
--spark-master spark://hadoop-m:7077 \
--jar-path googlegenomics-spark-examples-assembly-1.0.jar
```

The analysis will output the two principal components for each sample to the console. Here is an example of the last few lines.
```
...
NA20811		0.0286308791579312	-0.008456233951873527
NA20812		0.030970386921818943	-0.006755469223823698
NA20813		0.03080348019961635	-0.007475822860939408
NA20814		0.02865238920148145	-0.008084003476919057
NA20815		0.028798695736608034	-0.003755789964021788
NA20816		0.026104805529612096	-0.010430718823329282
NA20818		-0.033609576645005836	-0.026655905606186293
NA20819		0.032019557126552155	-0.00775750983842731
NA20826		0.03026607917284046	-0.009102704080927001
NA20828		-0.03412964005321165	-0.025991697661590686
NA21313		-0.03401702847363714	-0.024555217139987182
```

To save the PCA analysis output to a file, specify the `--output-path` flag.

To specify a different variantset or run the analysis on multiple references use the `--variant-set-id` and  `--references` flags, the `--references` flag understand the following format, `<reference>:<start>:<end>,...`.

To run a genome wide analysis on 1K genomes on a reasonable time, make sure you are running on at least 40 cores (`10 n1-standard-4 machines + 1 master`), the following command will run in approximatey 2 hours:

```
export SPARK_CLASSPATH=googlegenomics-spark-examples-assembly-1.0.jar 
spark-class -Dspark.shuffle.spill=true \
com.google.cloud.genomics.spark.examples.VariantsPcaDriver \
--spark-master spark://hadoop-m:7077 \
--jar-path googlegenomics-spark-examples-assembly-1.0.jar \
--client-secrets ./client_secrets.json \
--partitions-per-reference 500 \
--num-reduce-partitions 500 \
--references 1:1:249584621,2:1:243573643,3:1:198646620,4:1:191166555,5:1:181789530,6:1:171437644,7:1:159384882,8:1:147035750,9:1:141622696,10:1:136179071,11:1:135844125,12:1:134672335,13:1:115181169,14:1:107515075,15:1:103358507,16:1:90985975,17:1:81775057,18:1:78247741,19:1:59585744,20:1:63962825,21:1:48974388,22:1:51869428 \
--output-path 1000genomes
```

You can track the progress of the job in the Spark UI console (see the following section to make sure you can connect to the UI using your browser) .

To retrieve the results form the output directory use `gsutil`:

```
gsutil cat gs://<bucket-name>/<output-path>-pca.tsv/part* > pca-results.tsv
```

### Debugging 
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
http://hadoop-m:8080 for Spark
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
