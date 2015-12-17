spark-examples [![Build Status](https://img.shields.io/travis/googlegenomics/spark-examples.svg?style=flat)](https://travis-ci.org/googlegenomics/spark-examples)
==============

The projects in this repository demonstrate working with genomic data accessible via the [Google Genomics API](https://cloud.google.com/genomics/) using [Apache Spark](http://spark.apache.org/).

> If you are ready to start coding, take a look at the information below.  But if you are
> looking for a task-oriented list (e.g., [How do I compute principal coordinate analysis
> with Google Genomics?](http://googlegenomics.readthedocs.org/en/latest/use_cases/compute_principal_coordinate_analysis/index.html)),
> a better place to start is the [Google Genomics Cookbook](http://googlegenomics.readthedocs.org/en/latest/index.html).

Getting Started
---------------

 1. git clone this repository.
 
 1. If you have not already done so, follow the Google Genomics [getting started instructions](https://cloud.google.com/genomics/install-genomics-tools) to set up your environment
  including [installing gcloud](https://cloud.google.com/sdk/) and running `gcloud init`.

 1. Download and install [Apache Spark](https://spark.apache.org/downloads.html).

 1. Install [SBT](http://www.scala-sbt.org/release/docs/Getting-Started/Setup.html).

 1. This project now includes code for calling the Genomics API using [gRPC](http://www.grpc.io).  To use gRPC, you'll need a version of ALPN that matches your JRE version. 

  2. See the [ALPN documentation](http://www.eclipse.org/jetty/documentation/9.2.10.v20150310/alpn-chapter.html) for a table of which ALPN jar to use for your JRE version.
  2. Then download the correct version from [here](http://mvnrepository.com/artifact/org.mortbay.jetty.alpn/alpn-boot).

Local Run
---------
From the `spark-examples` directory run `sbt run`

Use the following flags to match your runtime configuration:

```
$ export SBT_OPTS='-Xbootclasspath/p:/YOUR/PATH/TO/alpn-boot-YOUR-VERSION.jar'
$ sbt "run --help"
  -o, --output-path  <arg>
  -s, --spark-master  <arg>      A spark master URL. Leave empty if using spark-submit.
  ...
      --help                     Show help message
```

For example: 

```
$ sbt "run --spark-master local[4]"
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

(1) Build the assembly.
```
sbt assembly
```
(2) Deploy your Spark cluster using [Google Cloud Dataproc](https://cloud.google.com/dataproc/).
```
gcloud beta dataproc clusters create example-cluster --scopes cloud-platform
```
(3) Copy the assembly jar to the master node.
```
gcloud compute copy-files \
  target/scala-2.10/googlegenomics-spark-examples-assembly-1.0.jar  example-cluster-m:~/
```
(4) ssh to the master.
```
gcloud compute ssh example-cluster-m
```
(5) Run one of the examples.
```
spark-submit --class com.google.cloud.genomics.spark.examples.SearchReadsExample1 \
  googlegenomics-spark-examples-assembly-1.0.jar
```

### Running PCA variant analysis on GCE
To run the [variant PCA analysis](https://github.com/googlegenomics/spark-examples/blob/master/src/main/scala/com/google/cloud/genomics/spark/examples/VariantsPca.scala) on GCE  make sure you have followed all the steps on the previous section and that you are able to run at least one of the examples.

Run the example PCA analysis for BRCA1 on the [1000 Genomes Project dataset](https://cloud.google.com/genomics/data/1000-genomes).
```
spark-submit --class com.google.cloud.genomics.spark.examples.VariantsPcaDriver \
  googlegenomics-spark-examples-assembly-1.0.jar
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

This pipeline is described in greater detail on [How do I compute principal coordinate analysis with Google Genomics?](http://googlegenomics.readthedocs.org/en/latest/use_cases/compute_principal_coordinate_analysis/index.html)

### Debugging 

For more information, see https://cloud.google.com/dataproc/faq
