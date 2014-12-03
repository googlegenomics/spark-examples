/*
Copyright 2014 Google Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.google.cloud.genomics.spark.examples

import org.apache.spark.SparkContext
import org.rogach.scallop.ScallopConf
import org.apache.spark.SparkConf
import com.google.cloud.genomics.Client
class GenomicsConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val basesPerPartition = opt[Int](default = Some(100000),
      descr = "Partition each reference using a fixed number of bases")
  val clientSecrets = opt[String](default = Some("client_secrets.json"))
  val inputPath = opt[String]()
  val jarPath = opt[String]()
  val numReducePartitions = opt[Int](default = Some(10),
      descr = "Set it to a " +
      "number greater than the number of cores, to achieve maximum " +
      "throughput.")
  val outputPath = opt[String]()
  val partitionsPerReference = opt[Int](default = Some(10),
      descr = "How many partitions per reference. Set it to a " +
      "number greater than the number of cores, to achieve maximum " +
      "throughput.")
  val references = opt[String](default=Some("17:41196311:41277499"),
      descr = "Comma separated tuples of reference:start:end,...")
  val sparkMaster = opt[String](default = Some("local[2]"))
  val sparkPath = opt[String](default = Some(""))
  val variantSetId = opt[String](
    default = Some(GoogleGenomicsPublicData.Thousand_Genomes_Phase_1),
      descr = "VariantSetId to use in the analysis.")

  def newSparkContext(className: String) = {
    val jarPath = if (this.jarPath.isDefined)
      Seq(this.jarPath())
    else
      Seq[String]()
    val conf = new SparkConf()
      .setMaster(this.sparkMaster())
      .setAppName(className)
      .setSparkHome(this.sparkPath())
      .setJars(jarPath)
      .set("spark.shuffle.consolidateFiles", "true")
    new SparkContext(conf)
  }

  def getReferences = {
    this.references().split(",").map(contig => {
      val data = contig.split(":")
      (data(0), (data(1).toLong, data(2).toLong))
    }).toMap
  }
}

class PcaConf(arguments: Seq[String]) extends GenomicsConf(arguments) {
  val numPc = opt[Int](default = Some(2))
}
