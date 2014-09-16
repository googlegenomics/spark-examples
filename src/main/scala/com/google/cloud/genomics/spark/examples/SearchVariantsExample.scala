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

import collection.JavaConversions._
import collection.mutable.{ Map => MutableMap }
import com.google.api.services.genomics.model.SearchVariantsRequest
import com.google.cloud.genomics.Client
import com.google.cloud.genomics.spark.examples.rdd.{ VariantsRDDBuilder,
                                                      VariantsPartitioner,
                                                      VariantsRDD,
                                                      FixedContigSplits }
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.google.cloud.genomics.spark.examples.rdd.VariantKey
import com.google.cloud.genomics.spark.examples.rdd.Variant

object VariantDatasets {
  final val Google_PGP_gVCF_Variants =   "11785686915021445549"
  final val Google_1000_genomes_phase_1 = "1154144306496329440"
}

/**
 * The variant in this example corresponds to dbSNP ID rs9536314,
 * causing an amino acid substitution in the Klotho gene (KL
 * F327V). About 30% of people carry the variant. In build 37, this is
 * an A to G substition at chromosome 13, position 33628138.
 */
object SearchVariantsExampleKlotho {
  def main(args: Array[String]) = {
    val conf = new GenomicsConf(args)
    val sc = conf.newSparkContext(this.getClass.getName)
    Logger.getLogger("org").setLevel(Level.WARN)
    val klotho = Map(("13" -> (33628138L, 33628139L)))
    val data = new VariantsRDD(sc,
      this.getClass.getName,
      conf.clientSecrets(),
      VariantDatasets.Google_PGP_gVCF_Variants,
      new VariantsPartitioner(klotho, FixedContigSplits(1)))
    data.cache()  // The amount of data is small since its just for one SNP.
    println("We have " + data.count() + " records that overlap Klotho.")
    println("But only " + data.filter { kv =>
                                        val (key, variant) = kv
                                        variant.referenceBases != "N"
      }.count() + " records are of a variant.")
    println("The other " + data.filter { kv =>
                                         val (key, variant) = kv
                                         variant.referenceBases == "N"
      }.count() + " records are reference-matching blocks.")
    val variants = data.filter { kv =>
                                 val(key, variant) = kv
                                 variant.referenceBases != "N"
    }
    variants.collect.foreach { kv =>
                               val (key, variant) = kv
                               println(variant.contig + " " + variant.position)
    }

    // Exercise conversion from scala objects back to java objects.  This
    // is needed for a forthcoming example which writes modified
    // variants back to the variant store.
    //
    // TODO: this really belongs in an integration test or a unit test
    // with a mocked-out Genomics client; not in this sample.
    data.collect.foreach { kv =>
                           val (key, variant) = kv
                           variant.toJavaVariant() }
    sc.stop
  }
}

/**
 * This example pulls all variants that overlap BRCA1.
 */
object SearchVariantsExampleBRCA1 {
  def main(args: Array[String]) = {
    val conf = new GenomicsConf(args)
    val sc = conf.newSparkContext(this.getClass.getName)
    Logger.getLogger("org").setLevel(Level.WARN)
    val brca1 = Map(("17" -> (41196312L, 41277500L)))
    val data = if (conf.inputPath.isDefined)  
      sc.objectFile[(VariantKey, Variant)](conf.inputPath())
    else
      new VariantsRDD(sc,
        this.getClass.getName,
        conf.clientSecrets(),
        VariantDatasets.Google_PGP_gVCF_Variants,
        new VariantsPartitioner(brca1, FixedContigSplits(1)))
    data.cache() // The amount of data is small since its just for one gene
    println("We have " + data.count() + " records that overlap BRCA1.")
    println("But only " + data.filter { kv =>
                                        val(key, variant) = kv
                                        variant.referenceBases != "N"
      }.count() + " records are of a variant.")
    println("The other " + data.filter { kv =>
                                         val(key, variant) = kv
                                         variant.referenceBases == "N"
      }.count() + " records are reference-matching blocks.")
    sc.stop
  }
}
