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

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.rogach.scallop.ScallopConf

import com.google.cloud.genomics.spark.examples.rdd.AllReferencesVariantsPartitioner
import com.google.cloud.genomics.spark.examples.rdd.ReferencesVariantsPartitioner
import com.google.cloud.genomics.spark.examples.rdd.VariantsPartitioner
import com.google.cloud.genomics.utils.OfflineAuth
import com.google.cloud.genomics.utils.ShardUtils.SexChromosomeFilter

class GenomicsConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val DEFAULT_NUMBER_OF_BASES_PER_SHARD = 1000000
  val PLATINUM_GENOMES_BRCA1_REFERENCES = "chr17:41196311:41277499"

  val basesPerPartition = opt[Long](default =
    Some(DEFAULT_NUMBER_OF_BASES_PER_SHARD),
      descr = "Partition each reference using a fixed number of bases")
  val clientSecrets = opt[String](
    descr = "Provide the file path to client_secrets.json to use a user "
    + "credential instead of the Application Default Credential.")
  val inputPath = opt[String]()
  val numReducePartitions = opt[Int](default = Some(10),
      descr = "Set it to a " +
      "number greater than the number of cores, to achieve maximum " +
      "throughput.")
  val outputPath = opt[String]()
  val references = opt[List[String]](default=
    Some(List(PLATINUM_GENOMES_BRCA1_REFERENCES)),
      descr = "Comma separated tuples of reference:start:end,... " +
      "one list of tuples should be specified per variantset " +
      "in the corresponding order.")
  val sparkMaster = opt[String](
      descr = "A spark master URL. Leave empty if using spark-submit.")
  val variantSetId = opt[List[String]](
    default = Some(List(GoogleGenomicsPublicData.Platinum_Genomes)),
      descr = "List of VariantSetId to use in the analysis.")

  def newSparkContext(className: String) = {
    val conf = new SparkConf()
      .setAppName(className)
      .set("spark.shuffle.consolidateFiles", "true")
    if (this.sparkMaster.isDefined)
      conf.setMaster(this.sparkMaster())
    new SparkContext(conf)
  }

  def getPartitioner(references: String) = {
    new ReferencesVariantsPartitioner(references, this.basesPerPartition())
  }
}

object PcaConf {
  val ExcludeXY = SexChromosomeFilter.EXCLUDE_XY
}

class PcaConf(arguments: Seq[String]) extends GenomicsConf(arguments) {
  val allReferences = opt[Boolean](
      descr =  "Use all references (except X and Y) to compute PCA " +
      "(overrides --references).")
  val debugDatasets = opt[Boolean]()
  val minAlleleFrequency = opt[Float](
      descr = "For 2-way PCA, omit variants from the left variant set (typically 1,000 Genomes)" +
      " by including only variants with allelic frequency (field AF) greater than" +
      " or equal to this value.")
  val numPc = opt[Int](default = Some(2))

  /**
   * Returns either the parsed references for all datasets and their
   * corresponding  --references or all references
   * except X and Y if --all-references is specified.
   */
  def getPartitioner(auth: OfflineAuth, variantSetId: String,
      variantSetIndex: Int = 0) = {
    if (this.allReferences()) {
      new AllReferencesVariantsPartitioner(this.basesPerPartition(), auth)
    } else {
      new ReferencesVariantsPartitioner(this.references().get(variantSetIndex),
          this.basesPerPartition())
    }
  }
}
