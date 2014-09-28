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

import scala.collection.JavaConversions.asScalaIterator
import scala.collection.JavaConversions.seqAsJavaList
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext.rddToOrderedRDDFunctions
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import com.google.api.services.genomics.Genomics
import com.google.api.services.genomics.model.CallSet
import com.google.api.services.genomics.model.SearchCallSetsRequest
import com.google.cloud.genomics.spark.examples.rdd.FixedContigSplits
import com.google.cloud.genomics.spark.examples.rdd.Variant
import com.google.cloud.genomics.spark.examples.rdd.VariantKey
import com.google.cloud.genomics.spark.examples.rdd.VariantsPartitioner
import com.google.cloud.genomics.spark.examples.rdd.VariantsRDD
import com.google.cloud.genomics.utils.Paginator

object VariantsPcaDriver {
  
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new PcaConf(args)
    val driver = VariantsPcaDriver(conf)
    val callsRdd = driver.getCallsRdd.cache()
    val simMatrix = driver.getSimilarityMatrix(callsRdd).cache()
    driver.doPca(simMatrix)
    driver.stop
  }

  def apply(conf: PcaConf) = new VariantsPcaDriver(conf)
}

class VariantsPcaDriver(conf: PcaConf) {

  val sc = conf.newSparkContext(this.getClass.getName)
  
  val indexes = {
    val client = conf.newGenomicsClient(this.getClass.getName)
    val searchCallsets = Paginator.Callsets.create(client)   
    val req = new SearchCallSetsRequest()
        .setVariantSetIds(List(VariantDatasets.Google_1000_genomes_phase_1))
    searchCallsets.search(req).iterator()
      .map(callSet => callSet.getSampleId).toSeq.zipWithIndex.toMap
  }

  private def getData = {
    if (conf.inputPath.isDefined) {
      sc.objectFile[(VariantKey, Variant)](conf.inputPath())
    } else {
      val contigs = conf.getContigs
      new VariantsRDD(sc, this.getClass.getName, conf.clientSecrets(),
        VariantDatasets.Google_1000_genomes_phase_1,
        new VariantsPartitioner(contigs, 
            FixedContigSplits(conf.partitionsPerContig())))
    }
  }
  
  def getCallsRdd = {
    val samplesWithVariant = getData.map(kv => kv._2)
      .map(_.calls.getOrElse(Seq()))
      .map(calls => calls.filter(_.genotype.foldLeft(false)(_ || _ > 0)))
      // Keep only the variants that have more than 1 call
      .filter(_.size > 1).cache
    val callsets = samplesWithVariant.map(_.map(_.callsetName))
    val broadcastNames = sc.broadcast(indexes)
    callsets.map(callset => {
      val mapping = broadcastNames.value
      callset.map(mapping(_).toInt)
    })
  }

  def getSimilarityMatrix(ids: RDD[Seq[Int]]) = {
    // Keep track of how many calls shared the same variant
    val similar = ids.flatMap(callset =>
      // Emit only half of the counts
      for (c1 <- callset.iterator; c2 <- callset.iterator if c1 <= c2)
        yield ((c1, c2), 1)).reduceByKey(_ + _, conf.reducePartitions())
    if (conf.outputPath.isDefined) {
      val out = conf.outputPath()
      similar.saveAsObjectFile(s"${out}-similar.dat")
      //indexes.saveAsObjectFile(s"${out}-indexes.dat")
    }
    similar
  }

  def doPca(simVectors: RDD[((Int, Int), Int)]) {
    val rowCount = indexes.size
    val entries =
      simVectors.map(item => (item._1._1, item._1._2, item._2.toDouble))
        .flatMap(item => {
          // Rebuild the symmetric matrix
          if (item._1 < item._2) {
            Seq(item, (item._2, item._1, item._3))
          } else {
            Seq(item)
          }
        })
        .map(item => (item._1, (item._2, item._3)))
        .groupByKey()
        .sortByKey(true)
        .cache
    val rowSums = entries.map(_._2.foldLeft(0D)(_ + _._2)).collect
    val broadcastRowSums = sc.broadcast(rowSums)
    val matrixSum = rowSums.reduce(_ + _)
    val matrixMean = matrixSum / rowCount / rowCount;
    val centeredRows = entries.map(indexedRow => {
      val localRowSums = broadcastRowSums.value
      val i = indexedRow._1
      val row = indexedRow._2
      val rowMean = localRowSums(i) / rowCount;
      row.map(entry => {
        val j = entry._1
        val data = entry._2
        val colMean = localRowSums(j) / rowCount;
        (j, data - rowMean - colMean + matrixMean)
      }).toSeq
    })
    val rows = centeredRows.map(row => Vectors.sparse(rowCount, row))
    val matrix = new RowMatrix(rows)
    val pca = matrix.computePrincipalComponents(conf.numPc())
    val array = pca.toArray
    val reverse = indexes.map(_.swap)
    val table = for (i <- 0 until pca.numRows)
      yield (reverse(i), array(i), array(i + pca.numRows))

    table.sortBy(_._1).foreach(tuple =>
      println(s"${tuple._1}\t\t${tuple._2}\t${tuple._3}"))
  }

  def stop {
    sc.stop
  }
}