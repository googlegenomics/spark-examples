spark-examples [![Build Status](https://img.shields.io/travis/googlegenomics/spark-examples.svg?style=flat)](https://travis-ci.org/googlegenomics/spark-examples)
==============

The projects in this repository demonstrate working with genomic data accessible via the [Google Genomics API](https://cloud.google.com/genomics/) using [Apache Spark](http://spark.apache.org/).

> If you are ready to start coding, take a look at the information below.  But if you are
> looking for a task-oriented list (e.g., [How do I compute principal coordinate analysis
> with Google Genomics?](http://googlegenomics.readthedocs.org/en/latest/use_cases/compute_principal_coordinate_analysis/index.html)),
> a better place to start is the [Google Genomics Cookbook](http://googlegenomics.readthedocs.org/en/latest/index.html).

Getting Started
---------------

 1. Follow the [sign up instructions](https://cloud.google.com/genomics/install-genomics-tools#authenticate) and download the `client_secrets.json` file. This file can be copied to the _spark-examples_ directory.

 2. Download and install [Apache Spark](https://spark.apache.org/downloads.html).

 3. If needed, install [SBT](http://www.scala-sbt.org/release/docs/Getting-Started/Setup.html)

Local Run
---------
From the `spark-examples` directory run `sbt run`

Use the following flags to match your runtime configuration:

```
$ sbt "run --help"
  -c, --client-secrets  <arg>    (default = client_secrets.json)
  -o, --output-path  <arg>
  -s, --spark-master  <arg>      A spark master URL. Leave empty if using spark-submit.
  ...
      --help                     Show help message
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

Run on Google Compute Engine
-----------------------------

If you have not done so already, follow the [instructions](https://cloud.google.com/hadoop/) to setup the Google Cloud SDK and bdutil. At the end of the process you should have launched a small cluster and logged into it using `gcloud compute`.

(1) Use [bdutil](https://cloud.google.com/hadoop/bdutil) to deploy the cluster with Spark enabled.  **bdutil does not yet deploy Spark 1.3 by default. Make [these edits](https://github.com/GoogleCloudPlatform/bdutil/commit/141480076280cdf1fe26fed5da950b54b87803d2) to the relevant files to deploy a cluster running Spark 1.3.**
```
./bdutil -e extensions/spark/spark_env.sh deploy
```
(2) Copy your ``client_secrets.json`` to the master.
```
gcloud compute copy-files client_secrets.json hadoop-m:~/
```
(3) Copy the assembly jar to the master node.
```
gcloud compute copy-files \
  target/scala-2.10/googlegenomics-spark-examples-assembly-1.0.jar  hadoop-m:~/
```
(4) ssh to the master.
```
gcloud compute ssh hadoop-m
```
(5) Run one of the examples.
```
spark-submit --class com.google.cloud.genomics.spark.examples.SearchReadsExample1 \
  --master spark://hadoop-m:7077 googlegenomics-spark-examples-assembly-1.0.jar \
  --client-secrets client_secrets.json
```

When prompted copy the authentication URL to a browser, authorize the application and copy 
the authorization code. This step will copy the access token to all the workers.

### Running PCA variant analysis on GCE
To run the [variant PCA analysis](https://github.com/googlegenomics/spark-examples/blob/master/src/main/scala/com/google/cloud/genomics/spark/examples/VariantsPca.scala) on GCE  make sure you have followed all the steps on the previous section and that you are able to run at least one of the examples.

Run the example PCA analysis for BRCA1 on the [1000 Genomes Project dataset](https://cloud.google.com/genomics/data/1000-genomes).
```
spark-submit --class com.google.cloud.genomics.spark.examples.VariantsPcaDriver \
  --master spark://hadoop-m:7077 googlegenomics-spark-examples-assembly-1.0.jar \
  --client-secrets client_secrets.json
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
spark-submit --class com.google.cloud.genomics.spark.examples.VariantsPcaDriver \
  --master spark://hadoop-m:7077 \
  --conf spark.shuffle.spill=true \
  googlegenomics-spark-examples-assembly-1.0.jar \
  --client-secrets client_secrets.json \
  --bases-per-partition 1000000 \
  --num-reduce-partitions 500 \
  --all-references \
  --output-path 1000genomes
```

You can track the progress of the job in the Spark UI console (see the following section to make sure you can connect to the UI using your browser) .

To retrieve the results form the output directory use `gsutil`:

```
gsutil cat gs://<bucket-name>/<output-path>-pca.tsv/part* > pca-results.tsv
```

### Debugging 

To debug the jobs from the Spark web UI, either setup a SOCKS5 proxy (recommended)
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
gcloud compute firewall-rules create allow-spark-console-8080 \
  --target-tag spark-console \
  --description "Incoming 8080-8081 allowed." --allow tcp:8080-8081
gcloud compute firewall-rules create allow-spark-console-4040 \
  --target-tag spark-console \
  --description "Incoming 4040 allowed." --allow tcp:4040
```
From the [developers console](https://console.developers.google.com/project),
add the `spark-console` tag to the master and worker instances or follow the instructions
[here](https://cloud.google.com/compute/docs/instances#tags) to do it from the command line.

Then point the browser to `http://<master-node-public-ip>:8080`
