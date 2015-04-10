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

import scala.collection.JavaConversions._

import org.apache.spark.SparkContext
import org.rogach.scallop.ScallopConf
import org.apache.spark.SparkConf
import com.google.cloud.genomics.Client
import com.google.cloud.genomics.utils.Contig
import com.google.cloud.genomics.utils.Contig.SexChromosomeFilter
import com.google.api.services.genomics.Genomics

class GenomicsConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val basesPerPartition = opt[Long](default =
    Some(Contig.DEFAULT_NUMBER_OF_BASES_PER_SHARD),
      descr = "Partition each reference using a fixed number of bases")
  val clientSecrets = opt[String](default = Some("client_secrets.json"))
  val inputPath = opt[String]()
  val numReducePartitions = opt[Int](default = Some(10),
      descr = "Set it to a " +
      "number greater than the number of cores, to achieve maximum " +
      "throughput.")
  val outputPath = opt[String]()
  val references = opt[List[String]](default=Some(List(Contig.BRCA1)),
      descr = "Comma separated tuples of reference:start:end,... " +
      "one per variantset, in the corresponding order.")
  val sparkMaster = opt[String](
      descr = "A spark master URL. Leave empty if using spark-submit.")
  val variantSetId = opt[List[String]](
    default = Some(List(GoogleGenomicsPublicData.Thousand_Genomes_Phase_1)),
      descr = "List of VariantSetId to use in the analysis.")

  def newSparkContext(className: String) = {
    val conf = new SparkConf()
      .setAppName(className)
      .set("spark.shuffle.consolidateFiles", "true")
    if (this.sparkMaster.isDefined)
      conf.setMaster(this.sparkMaster())
    new SparkContext(conf)
  }

  def getReferences = {
    this.references() map {
      Contig.parseContigsFromCommandLine(_)
    }
  }
}

object PcaConf {
  val ExcludeXY = SexChromosomeFilter.EXCLUDE_XY
}

class PcaConf(arguments: Seq[String]) extends GenomicsConf(arguments) {
  val allReferences = opt[Boolean](
      descr =  "Use all references (except X and Y) to compute PCA " +
      "(overrides --references).")
  val minAlleleFrequency = opt[Float]()
  val numPc = opt[Int](default = Some(2))

  /**
   * Returns either the parsed references from --references or all references
   * except X and Y if --all-references is specified.
   */
  def getReferences(client: Genomics, variantSetIds: List[String]) = {
    println(s"Running PCA on ${variantSetIds.length} references.")
    variantSetIds.zip(this.references()).map {
      case (variantSetId, references)  =>
      if (this.allReferences()) {
        println(s"Variantset: ${variantSetId}; All refs, exclude XY")
        Contig.getContigsInVariantSet(client, variantSetId, PcaConf.ExcludeXY)
      } else {
        println(s"Variantset: ${variantSetId}; Refs: ${references}")
        Contig.parseContigsFromCommandLine(references)
      }
    }.flatten
  }
}
