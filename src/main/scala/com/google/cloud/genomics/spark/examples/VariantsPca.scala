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
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import com.google.api.services.genomics.Genomics
import com.google.api.services.genomics.model.CallSet
import com.google.api.services.genomics.model.SearchCallSetsRequest
import com.google.cloud.genomics.Authentication
import com.google.cloud.genomics.Client
import com.google.cloud.genomics.spark.examples.rdd.Variant
import com.google.cloud.genomics.spark.examples.rdd.VariantKey
import com.google.cloud.genomics.spark.examples.rdd.VariantsPartitioner
import com.google.cloud.genomics.spark.examples.rdd.VariantsRDD
import com.google.cloud.genomics.utils.Paginator
import breeze.linalg._
import com.google.cloud.genomics.spark.examples.rdd.VariantsRddStats

object VariantsPcaDriver {

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new PcaConf(args)
    val driver = VariantsPcaDriver(conf)
    val callsRdd = driver.getCallsRdd
    val simMatrix = driver.getSimilarityMatrix(callsRdd)
    val result = driver.computePca(simMatrix)
    driver.emitResult(result)
    driver.reportIoStats
    driver.stop
  }

  def apply(conf: PcaConf) = new VariantsPcaDriver(conf)
}

class VariantsPcaDriver(conf: PcaConf) {

  private val applicationName = this.getClass.getName
  private val sc = conf.newSparkContext(this.getClass.getName)
  private val auth = Authentication.getAccessToken(conf.clientSecrets())
  private val ioStats = createIoStats

  val indexes: Map[String, Int] = {
    val client = Client(auth).genomics
    val searchCallsets = Paginator.Callsets.create(client)
    val req = new SearchCallSetsRequest()
        .setVariantSetIds(List(conf.variantSetId()))
    searchCallsets.search(req).iterator()
      .map(callSet => callSet.getSampleId).toSeq.zipWithIndex.toMap
  }

  private def getData: RDD[(VariantKey, Variant)] = {
    if (conf.inputPath.isDefined) {
      sc.objectFile[(VariantKey, Variant)](conf.inputPath())
    } else {
      val client = Client(auth).genomics
      val contigs = conf.getReferences(client, conf.variantSetId()).toArray
      println(s"Running PCA on ${contigs.length} references.")
      new VariantsRDD(sc, this.getClass.getName, auth,
        conf.variantSetId(),
        new VariantsPartitioner(contigs, conf.basesPerPartition()),
        stats=ioStats)
    }
  }

  /**
   * Returns an RDD of variant callsets with each call mapped to an position.
   */
  def getCallsRdd: RDD[Seq[Int]] = {
    val samplesWithVariant = getData.map(kv => kv._2)
      .map(_.calls.getOrElse(Seq()))
      .map(calls => calls.filter(_.genotype.foldLeft(false)(_ || _ > 0)))
      // Keep only those variants that have at least one call.
      .filter(_.size > 0)
    val broadcastNames = sc.broadcast(indexes)
    val callsets = samplesWithVariant.map(_.map(_.callsetName))
    callsets.map(callset => {
      val mapping = broadcastNames.value
      callset.map(mapping(_).toInt)
    })
  }

  /**
   * Computes a similarity matrix from the variant information.
   *
   * This method computes the similarity in place, this means it updates the
   * the counts in place on a pre-allocated dense matrix.
   *
   * Use this method if the partial matrix will fit in memory, roughly
   * a data set with 50K samples would fit on ~20GB of memory.
   *
   * @param calls an RDD of call ids, one variant per record.
   * @return an RDD of tuples with the matrix entry indexes and its similarity.
   */
  def getSimilarityMatrix(callsets: RDD[Seq[Int]]) = {
    val size = indexes.size
    callsets.mapPartitions(callsInPartition => {
      val matrix = DenseMatrix.zeros[Int](size, size)
      callsInPartition.foreach(callset =>
        for (c1 <- callset; c2 <- callset)
          matrix.update(c1, c2, matrix(c1, c2) + 1))
      matrix.iterator
    }).reduceByKey(_ + _, conf.numReducePartitions())
  }

  /**
   * Computes the PCA from the similarity matrix entries.
   *
   * @param matrixEntries an RDD of tuples representing the matrix entries.
   */
  def computePca(matrixEntries: RDD[((Int, Int), Int)]) = {
    val rowCount = indexes.size
    val entries =
      matrixEntries.map(item => (item._1._1, item._1._2, item._2.toDouble))
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
    for (i <- 0 until pca.numRows)
      yield (reverse(i), array(i), array(i + pca.numRows))
  }

  def emitResult(result: Seq[(String, Double, Double)]) {
    result.sortBy(_._1).foreach(tuple =>
      println(s"${tuple._1}\t\t${tuple._2}\t${tuple._3}"))

    if(conf.outputPath.isDefined) {
      val resultRdd = sc.parallelize(result)
      resultRdd.map(tuple => s"${tuple._1}\t${tuple._2}\t${tuple._3}")
        .saveAsTextFile(conf.outputPath() + "-pca.tsv")
    }
  }

  /**
   * Computes a similarity matrix from the variant information.
   *
   * This method computes the similarity in a streaming fashion, this means
   * it never stores the partial similarity matrix in memory. The drawback
   * from this approach is that it generates large shuffles as it emits a
   * pair for each co-occurring call on a variant.
   *
   * Use this method only if the partial matrix won't fit in memory, roughly
   * a data set with 50K samples would fit on ~20GB of memory.
   *
   * @param calls an RDD of call ids, one variant per record.
   * @return an RDD of tuples with the matrix entry indexes and its similarity.
   */
  def getSimilarityMatrixStream(calls: RDD[Seq[Int]]):
    RDD[((Int, Int), Int)] = {
    // Keep track of how many calls share the same variant
    calls.flatMap(callset =>
      // Emit only half of the counts
      for (c1 <- callset.iterator; c2 <- callset.iterator if c1 <= c2)
        yield ((c1, c2), 1))
    // Aggregate the similar pairs, partially done in memory.
    .reduceByKey(_ + _, conf.numReducePartitions())
    // Rebuild the symmetric matrix from the aggregated pairs.
    .flatMap(item => {
      if (item._1._1 < item._1._2) {
        Seq(item, ((item._1._2, item._1._1), item._2))
      } else {
        Seq(item)
      }
    })
  }

  def reportIoStats = {
    this.ioStats match {
      case Some(stats) => println(stats.toString)
      case _ => {}
    }
  }

  def stop {
    sc.stop
  }

  def createIoStats = if (conf.inputPath.isDefined)
      None
    else
      Option(new VariantsRddStats(sc))
}
