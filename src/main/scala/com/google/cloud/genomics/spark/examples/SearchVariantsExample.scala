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

import org.apache.log4j.Level
import org.apache.log4j.Logger
import com.google.cloud.genomics.spark.examples.rdd.Variant
import com.google.cloud.genomics.spark.examples.rdd.VariantKey
import com.google.cloud.genomics.spark.examples.rdd.VariantsPartitioner
import com.google.cloud.genomics.spark.examples.rdd.VariantsRDD
import com.google.cloud.genomics.Authentication
import com.google.cloud.genomics.utils.Contig

object GoogleGenomicsPublicData {
  final val Platinum_Genomes =   "3049512673186936334"
  final val Thousand_Genomes_Phase_1 = "10473108253681171589"
  final val Thousand_Genomes_Phase_3 = "4252737135923902652"
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
    val applicationName = this.getClass.getName
    val sc = conf.newSparkContext(applicationName)
    Logger.getLogger("org").setLevel(Level.WARN)
    val klotho = Seq(new Contig("chr13", 33628137L, 33628138L))
    val accessToken = Authentication.getAccessToken(conf.clientSecrets())
    val data = new VariantsRDD(sc,
      applicationName,
      accessToken,
      GoogleGenomicsPublicData.Platinum_Genomes,
      new VariantsPartitioner(klotho, conf.basesPerPartition()))
    data.cache()  // The amount of data is small since its just for one SNP.
    println("We have " + data.count() + " records that overlap Klotho.")
    println("But only " + data.filter { kv =>
                                        val (key, variant) = kv
                                        variant.alternateBases != None
      }.count() + " records are of a variant.")
    println("The other " + data.filter { kv =>
                                         val (key, variant) = kv
                                         variant.alternateBases == None
      }.count() + " records are reference-matching blocks.")
    val variants = data.filter { kv =>
                                 val(key, variant) = kv
                                 variant.referenceBases != "N"
    }
    variants.collect.foreach { kv =>
      val (key, variant) = kv
      println(s"Reference: ${variant.contig} @ ${variant.start}")
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
    val applicationName = this.getClass.getName
    val sc = conf.newSparkContext(applicationName)
    Logger.getLogger("org").setLevel(Level.WARN)
    val brca1 = Seq(new Contig("chr17", 41196311L, 41277499L))
    val accessToken = Authentication.getAccessToken(conf.clientSecrets())
    val data = new VariantsRDD(sc,
        this.getClass.getName,
        accessToken,
        GoogleGenomicsPublicData.Platinum_Genomes,
        new VariantsPartitioner(brca1, conf.basesPerPartition()))
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
