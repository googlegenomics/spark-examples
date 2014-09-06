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
import com.google.cloud.genomics.spark.examples.rdd.VariantsRDD
import com.google.cloud.genomics.spark.examples.rdd.VariantsPartitioner
import com.google.cloud.genomics.spark.examples.rdd.FixedContigSplits
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import com.google.cloud.genomics.spark.examples.rdd.VariantKey
import com.google.cloud.genomics.spark.examples.rdd.Variant
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import com.google.cloud.genomics.spark.examples.rdd.VariantCallsRDD
import com.google.cloud.genomics.spark.examples.rdd.VariantCalls
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import com.google.cloud.genomics.spark.examples.rdd.Call
import org.apache.spark.SparkContext

class VariantsPca {

}

class VariantsRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[VariantCalls])
    kryo.register(classOf[Call])
  }
}

/** 
 * Saves the result of a variant search as an RDD of VariantCalls
 */
object VariantsSource {
  def main(args: Array[String]) = {
    val conf = new GenomicsConf(args)
    val sc = conf.newSparkContext(this.getClass.getName,
      Some("com.google.cloud.genomics.spark.examples.VariantsRegistrator"))
    Logger.getLogger("org").setLevel(Level.WARN)
    val contigs = conf.getContigs
    val data = new VariantCallsRDD(sc,
      this.getClass.getName,
      conf.clientSecrets(),
      VariantDatasets.Google_1000_genomes_phase_1,
      new VariantsPartitioner(contigs, 
          FixedContigSplits(conf.partitionsPerContig())),
          conf.maxResults())
      .saveAsObjectFile(conf.outputPath())
  }
}

object VariantsPcaDriver {
  def main(args: Array[String]) = {
    val conf = new PcaConf(args)
    val sc = conf.newSparkContext(this.getClass.getName)
    Logger.getLogger("org").setLevel(Level.WARN)
    val out = conf.outputPath()
    if (!conf.nocomputeSimilarity()) {
      val data = if (conf.inputPath.isDefined) {
        println(conf.inputPath())
        sc.objectFile[(VariantKey, VariantCalls)](conf.inputPath())
      } else {
        val contigs = conf.getContigs
        new VariantCallsRDD(sc,
          this.getClass.getName,
          conf.clientSecrets(),
          VariantDatasets.Google_PGP_gVCF_Variants,
          new VariantsPartitioner(contigs, FixedContigSplits(1)))
      }
      
      val logger = Logger.getLogger(this.getClass.getName)
      val samplesWithVariant = data.map(kv => kv._2)
        .map(_.calls.getOrElse(Seq()))
        .map(calls => calls.filter(_.genotype.foldLeft(false)(_ || _ > 0)))
        // Keep only the variants that have more than 1 call
        .filter(_.size > 1).cache
        
      val callsets = samplesWithVariant.map(_.map(_.callsetName))
      val counts = callsets.flatMap(callset => callset.map((_, 1)))
        .reduceByKey(_ + _, conf.reducePartitions()).cache()
      counts.saveAsTextFile(s"${out}-counts.txt")
      val indexes = counts.map(_._1)
        .zipWithIndex()
      indexes.saveAsObjectFile(s"${out}-indexes.dat")
      val names = indexes.collectAsMap
      val broadcastNames = sc.broadcast(names)
      println(s"Distinct calls ${names.size}") // Matrix size
      val toIds = callsets.map(callset => {
        val mapping = broadcastNames.value
        callset.map(mapping(_).toInt)
      }).cache()
      computeSimilarity(toIds, out, conf)
    }
    doPca(sc, out)
  }

  def computeSimilarity(toIds: RDD[Seq[Int]], out: String, conf: GenomicsConf) {
    // Keep track of how many calls shared the same variant
    val similar = toIds.flatMap(callset => 
      for (c1 <- callset.iterator; c2 <- callset.iterator)
        yield ((c1, c2), 1)
    ).reduceByKey(_ + _, conf.reducePartitions()).cache()
    similar.saveAsObjectFile(s"${out}-similar.dat")
  }

  def doPca(sc: SparkContext, out: String) {
    val similarFromFile =
      sc.objectFile[((Int, Int), Int)](s"${out}-similar.dat")
    val indexes =
      sc.objectFile[(String, Long)](s"${out}-indexes.dat")
      .map(item => (item._2, item._1)).collectAsMap
    val rowCount = indexes.size()
    val indexedRows =
      similarFromFile.map(item => (item._1._1, item._1._2, item._2.toDouble))
        .map(item => (item._1, (item._2, item._3)))
        .groupByKey()
        .sortByKey(true)
        .map(row => row._2.toSeq)
    println(s"RC: ${indexedRows.count()}")
    val rows = indexedRows.map(row => Vectors.sparse(rowCount, row))
    val matrix = new RowMatrix(rows)
    val pca = matrix.computePrincipalComponents(2)
    val array = pca.toArray
    val table = for (i <- 0 until pca.numRows) 
      yield (indexes(i), array(i), array(i + pca.numRows))
    
    table.sortBy(_._1).foreach(tuple => 
        println(s"${tuple._1}\t\t${tuple._2}\t${tuple._3}"))
  }
}
