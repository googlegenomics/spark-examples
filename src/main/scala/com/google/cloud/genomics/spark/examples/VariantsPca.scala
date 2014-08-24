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

object VariantsSource {
  def main(args: Array[String]) = {
    val conf = new GenomicsConf(args)
    val sc = conf.newSparkContext(this.getClass.getName,
      Some("com.google.cloud.genomics.spark.examples.VariantsRegistrator"))
    Logger.getLogger("org").setLevel(Level.WARN)
    val contigs = Map(
      ("4" -> (619373L, 664571L)),
      ("6" -> (133098746L, 133108745L)),
      ("7" -> (114055378L, 114330012L)) //        ("13" -> (32315479L, 33728139L)),
      //        ("17" -> (41196312L, 41277500L))
      //        ("13" -> (33628138L, 33628150L))
      )
    val data = new VariantCallsRDD(sc,
      this.getClass.getName,
      conf.clientSecrets(),
      VariantDatasets.Google_1000_genomes_phase_1,
      new VariantsPartitioner(contigs, FixedContigSplits(1)))
      .saveAsObjectFile(conf.outputPath())
  }
}

object VariantsPcaDriver {
  def main(args: Array[String]) = {
    val conf = new PcaConf(args)
    val sc = conf.newSparkContext(this.getClass.getName)
      //,Some("com.google.cloud.genomics.spark.examples.VariantsRegistrator"))
    Logger.getLogger("org").setLevel(Level.WARN)
    val out = conf.outputPath()
    if (!conf.nocomputeSimilarity()) {
      val data = if (conf.inputPath.isDefined) {
        println(conf.inputPath())
        sc.objectFile[(VariantKey, VariantCalls)](conf.inputPath())
      } else {
        val contigs = Map(
          ("13" -> (33628138L, 33628150L)))
        new VariantCallsRDD(sc,
          this.getClass.getName,
          conf.clientSecrets(),
          VariantDatasets.Google_PGP_gVCF_Variants,
          new VariantsPartitioner(contigs, FixedContigSplits(1)))
      }
      
      //println(s"Variants ${data.count}") // Matrix size
      val logger = Logger.getLogger(this.getClass.getName)
      val variants = data.map(kv => kv._2)
        .map(_.calls.getOrElse(Seq()))
        .map(calls => {
          //print(s"<<${calls.size}: ")
          val k = calls.filter(call => {
            //print(s"${call.callsetName} @ ${call.genotype}, ")
            call.genotype.map(_ > 0).reduce(_ || _)
          }) // If any of the bases has at least 1 variation
          // Keep only the variants that have more than 1 call
          //println(s" :${k.size}>>")
          k
        }).filter(_.size > 1).cache
        
      //println(s"Variants with calls${variants.count}") // Matrix size
      // Per variant calls
      val callsets = variants.map(_.map(_.callsetName))
      //callsets.foreach(println)
      //val x = callsets.flatMap(item => item).distinct
      //x.collect.sorted.foreach(println)
      //println(s"C calls ${x.count}") // Matrix size
      val counts = callsets.flatMap(callset => callset.map((_, 1)))
        .reduceByKey(_ + _, 16).cache()
      counts.saveAsTextFile(s"${out}-counts.txt")
      val indexes = counts.map(_._1)
        .zipWithIndex()
      indexes.saveAsTextFile(s"${out}-indexes.txt")
      val names = indexes.collectAsMap
      val broadcastNames = sc.broadcast(names)
      println(s"Distinct calls ${names.size}") // Matrix size
      val toIds = callsets.map(callset => {
        val mapping = broadcastNames.value
        callset.map(mapping(_).toInt)
      }).cache()
      computeSimilarity(toIds, out)
    }
    doPca(sc, out)
  }

  def computeSimilarity(toIds: RDD[Seq[Int]], out: String) {
    val similar = toIds.flatMap(callset => {
      //println(s"CSet -> ${callset.size}")
      // Keep track of when two calls have a variant on the same position.
      for (c1 <- callset.iterator; c2 <- callset.iterator if c1 != c2)
        yield ((c1, c2), 1)
    }).reduceByKey(_ + _, 16).cache()
    similar.saveAsObjectFile(s"${out}-similar.dat")
    //println(s"We have ${data.count()} variants in the search regions.")
    //println(s"${variants.count()} have more than 1 call.")
  }

  def doPca(sc: SparkContext, out: String) {
    val similarFromFile =
      sc.objectFile[((Int, Int), Int)](s"${out}-similar.dat")
      
    //println(similarFromFile.count())
    // Group calls by their row id 
    val indexedRows =
      similarFromFile.map(item => (item._1._1, item._1._2, item._2.toDouble))
        .map(item => {
          //          if (item._1 > item._2)
          //            (item._2, (item._1, item._3))
          //          else
          //println((item._1, (item._2, item._3)))
          (item._1, (item._2, item._3))
         })
        .groupByKey()
        .sortByKey(true)
        .map(row => {
          //println(s"${row._1}, ${row._2.size}")
          row._2.map(_._2).toArray})
    println(s"RC: ${indexedRows.count()}")
    val rows = indexedRows.map(row => Vectors.dense({
      //println(row.toList)
      row}))
    val matrix = new RowMatrix(rows)
    val pca = matrix.computePrincipalComponents(2)
    //    println(s"RC: ${pca.numCols}:${pca.numRows}")
    //    println(pca)
    val array = pca.toArray
    for (i <- 0 until pca.numRows) {
      println(s"${array(i)}\t${array(i + pca.numRows)}")
    }
    println(matrix.computeColumnSummaryStatistics.mean)
    //indexedRows.collect.foreach(item => println(s"${item._1} => ${item._2.size}"))
  }
}