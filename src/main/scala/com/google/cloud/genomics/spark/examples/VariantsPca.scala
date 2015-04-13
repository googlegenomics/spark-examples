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
import org.apache.spark.broadcast.Broadcast
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
import org.apache.spark.rdd.UnionRDD
import com.google.common.hash.Hashing
import com.google.common.base.Charsets
import org.apache.spark.Partitioner

object VariantsPcaDriver {

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new PcaConf(args)
    val driver = VariantsPcaDriver(conf)
    val data = driver.getData
    val filtered = data.map(driver.filterDataset)
    val callsRdd = driver.getCallsRdd(filtered)
    val simMatrix = driver.getSimilarityMatrix(callsRdd)
    val result = driver.computePca(simMatrix)
    driver.emitResult(result)
    driver.reportIoStats
    driver.stop
  }

  def apply(conf: PcaConf) = new VariantsPcaDriver(conf)

  // The following functions are defined on the companion object so they can be
  // serialized and used on the RDD functions.
  def extractCallInfo(variant: Variant, mapping: Map[String, Int]) = {
    variant.calls.getOrElse(Seq()).map(
        call => CallData(call.genotype.foldLeft(false)(_ || _ > 0),
            mapping(call.callsetId)))
 }

  def getVariantKey(variant: Variant) = {
    Hashing.murmur3_128().newHasher()
       .putString(variant.contig, Charsets.UTF_8)
       .putLong(variant.start)
       .putLong(variant.end)
       .putString(variant.referenceBases, Charsets.UTF_8)
       .putString(
           variant.alternateBases.map(
               altBases => altBases.mkString("")).getOrElse(""),
           Charsets.UTF_8)
      .hash().toString()
  }
}

class VariantsPcaDriver(conf: PcaConf) {

  private val applicationName = this.getClass.getName
  private val sc = conf.newSparkContext(this.getClass.getName)
  private val auth = Authentication.getAccessToken(conf.clientSecrets())
  private val ioStats = createIoStats

  val (indexes, names) = {
    val client = Client(auth).genomics
    val searchCallsets = Paginator.Callsets.create(client)
    val req = new SearchCallSetsRequest()
        .setVariantSetIds(conf.variantSetId())
    val callsets = searchCallsets.search(req).iterator().toSeq
    val indexes = callsets.map(
        callset => callset.getId()).toSeq.zipWithIndex.toMap
    val names = callsets.map(
        callset => (callset.getId(), callset.getName())).toMap
    println(s"Matrix size: ${indexes.size}.")
    (indexes, names)
  }

  private def getData = {
    if (conf.inputPath.isDefined) {
      List(sc.objectFile[(VariantKey, Variant)](conf.inputPath()).map(_._2))
    } else {
      val client = Client(auth).genomics
      val contigs = conf.getReferences(client, conf.variantSetId()).toArray
      val variantSets = conf.variantSetId()
      conf.variantSetId().map { variantSetId =>
        new VariantsRDD(sc, this.getClass.getName, auth,
          variantSetId,
          new VariantsPartitioner(contigs, conf.basesPerPartition()),
          stats=ioStats).map(_._2)
      }
    }
  }

  /**
   * Filter datasets according to the specified flags.
   *
   * Possible flags include:
   *   --min-allele-frequency
   */
  def filterDataset(data: RDD[Variant]) = {
    if (conf.minAlleleFrequency.isDefined) {
      val minAlleleFrequency = conf.minAlleleFrequency()
      println(s"Min allele frequency ${minAlleleFrequency}.")
      data.filter(variant => {
        val alleleFrequency = variant.info.get("AF")
        alleleFrequency.map(_.get(0).toFloat > minAlleleFrequency)
          .getOrElse(false)
      })
    } else {
      data
    }
  }
  /**
   * Returns an RDD of calls joined by their variant matching key.
   *
   * The key is composed by the reference name its start and end positions,
   * as well as the reference and alternate bases.
   */
  def joinDatasets(datasets: List[RDD[Variant]],
      broadcastIndexes: Broadcast[Map[String, Int]]): RDD[Seq[CallData]] = {
    val broadcastIndexes = sc.broadcast(indexes)
    val callsets = datasets.map(_.map(variant =>
      (VariantsPcaDriver.getVariantKey(variant), variant))
      .mapValues(
          VariantsPcaDriver.extractCallInfo(_, broadcastIndexes.value)))
    val callset1 = callsets(0)
    val callset2 = callsets(1)
    callset1.join(callset2, conf.numReducePartitions())
      .values
      .map(related => related._1 ++ related._2)
  }

  /**
   * Returns an RDD of calls merged by their variant matching key.
   *
   * The key is composed by the reference name its start and end positions,
   * as well as the reference and alternate bases.
   */
  def mergeDatasets(data: List[RDD[Variant]], variantSetCount: Int,
      broadcastIndexes: Broadcast[Map[String, Int]]): RDD[Seq[CallData]]= {
    val callsets = new UnionRDD(sc, data)
    val broadcastIndexes = sc.broadcast(indexes)
    callsets.map(variant =>
      (VariantsPcaDriver.getVariantKey(variant), variant))
      .mapValues(
          VariantsPcaDriver.extractCallInfo(_, broadcastIndexes.value))
      .groupByKey(conf.numReducePartitions())
      .values
      .filter(_.size() == variantSetCount)
      .map(related => related.flatMap(calls => calls).toSeq)
  }

  /**
   * Returns an RDD of variant callsets with each call mapped to a position.
   */
  def getCallsRdd(data: List[RDD[Variant]]): RDD[Seq[Int]] = {
    val variantSetCount = conf.variantSetId().size
    val broadcastIndexes = sc.broadcast(indexes)
    val callsets = if (variantSetCount == 1) {
      data.head.map(VariantsPcaDriver.extractCallInfo(_, broadcastIndexes.value))
    } else if (variantSetCount == 2) {
      joinDatasets(data, broadcastIndexes)
    } else {
      mergeDatasets(data, variantSetCount, broadcastIndexes)
    }
    return callsets
      .map(calls => calls.filter(_.hasVariation))
       // Return only those sets that have at least one call with variation.
      .filter(_.size > 0)
      .map(_.map(_.callsetId))
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
    val nonZeroRows = rowSums.filter(_ > 0).size
    println(s"Non zero rows in matrix: ${nonZeroRows} / ${indexes.size}.")
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
    val resultWithNames = result.map { tuple =>
      val dataset = tuple._1.split("-").head
      (names(tuple._1), tuple._2, tuple._3, dataset)
    }
    resultWithNames.sortBy(_._1).foreach(tuple =>
      println(s"${tuple._1}\t${tuple._4}\t${tuple._2}\t${tuple._3}"))

    if(conf.outputPath.isDefined) {
      val resultRdd = sc.parallelize(resultWithNames)
      resultRdd.map(tuple => s"${tuple._1}\t${tuple._2}\t${tuple._3}\t${tuple._4}")
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

case class CallData(hasVariation: Boolean, callsetId: Int) extends Serializable
