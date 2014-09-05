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
import com.google.api.services.genomics.model.SearchReadsetsRequest
import com.google.cloud.genomics.Client
import com.google.cloud.genomics.spark.examples.rdd.{ ReadsRDD, ReadsPartitioner, FixedSplits, TargetSizeSplits }
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Examples {

  final val Google_1KG_HG00096_Readset = "CJDmkYn8ChCcnc7i4KaWqmQ"
  // From http://google-genomics.readthedocs.org/en/latest/constants.html
  final val Google_Example_Readset = "CJDmkYn8ChCh4IH4hOf4gacB"
  // Sage Bio DREAM Contest - Synthetic Set #3
  val Google_DREAM_Set3_Normal = "CPHG3MzoCRDRkqXzk7b6l_kB"
  val Google_DREAM_Set3_Tumor = "CPHG3MzoCRCO1rDx8pOY6yo"

  // SNP @ 6889648 - cilantro/soap variant near OR10A2
  final val Cilantro = 6889648L

  final val HumanChromosomes = Map[String, Long](
    ("1", 249250621),
    ("2", 243199373),
    ("3", 198022430),
    ("4", 191154276),
    ("5", 180915260),
    ("6", 171115067),
    ("7", 159138663),
    ("8", 146364022),
    ("9", 141213431),
    ("10", 135534747),
    ("11", 135006516),
    ("12", 133851895),
    ("13", 115169878),
    ("14", 107349540),
    ("15", 102531392),
    ("16", 90354753),
    ("17", 81195210),
    ("18", 78077248),
    ("19", 59128983),
    ("20", 63025520),
    ("21", 48129895),
    ("22", 51304566),
    ("X", 155270560),
    ("Y", 59373566))
}

/**
 * This example searches for all reads covering the cilantro/soap SNP near OR10A2
 * on chromosome 11 and prints out a pileup. The quality score of each read at
 * the SNP location is also printed inline. This can be visualized in the Genomics API Browser:
 * http://gabrowse.appspot.com/#backend=GOOGLE&readsetId=CJDmkYn8ChCh4IH4hOf4gacB&location=11%3A6889648
 * Note that the reads may be displayed in different order.
 */
object SearchReadsExample1 {
  def main(args: Array[String]) = {
    val conf = new GenomicsConf(args)
    val sc = conf.newSparkContext(this.getClass.getName)
    Logger.getLogger("org").setLevel(Level.WARN)
    val region = Map(("11" -> (Examples.Cilantro - 1000, Examples.Cilantro + 1000)))
    val data = new ReadsRDD(sc, this.getClass.getName, conf.clientSecrets(),
      List(Examples.Google_Example_Readset),
      new ReadsPartitioner(region, FixedSplits(1)))
      .filter { rk =>
        val (_, read) = rk
        read.position <= Examples.Cilantro && read.position + read.alignedBases.length >= Examples.Cilantro
      }.cache()
    val first = data.collect.foldLeft(999999999L) { (a, b) =>
      val (_, read) = b
      val p = read.position
      if (p < a) { p.toLong } else { a }
    }
    println(List.fill((Examples.Cilantro - first).toInt)(" ").foldLeft("")(_ + _) + "v")
    val out = data.map { rk =>
      val (_, read) = rk
      val i = (Examples.Cilantro - read.position).toInt
      val bases = read.alignedBases.splitAt(i + 1)
      val q = "%02d".format(read.baseQuality(i) - 33)
      List.fill((read.position - first).toInt)(" ").foldLeft("")(_ + _) + bases._1 + "(" + q + ") " + bases._2
    }
    // Collect the results so they are printed on the local console.
    out.collect.foreach(println(_))
    
    println(List.fill((Examples.Cilantro - first).toInt)(" ").foldLeft("")(_ + _) + "^")
  }
}

/**
 * This example computes the average read coverage for a genomic range.
 */
object SearchReadsExample2 {
  def main(args: Array[String]) = {
    val conf = new GenomicsConf(args)
    val sc = conf.newSparkContext(this.getClass.getName)
    val chr = "21"
    val len = Examples.HumanChromosomes(chr)
    val region = Map((chr -> (1L, len)))
    val data = new ReadsRDD(sc, this.getClass.getName, conf.clientSecrets(),
      List(Examples.Google_Example_Readset),
      new ReadsPartitioner(region, TargetSizeSplits(100, 5, 1024, 16 * 1024 * 1024)))
    val coverage = data.map(_._2.alignedBases.length.toLong)
      .reduce(_ + _).toDouble / len.toDouble
    println("Coverage of chromosome " + chr + " = " + coverage)
  }
}

/**
 * This example computes the per-base read depth for a genomic range.
 */
object SearchReadsExample3 {
  def main(args: Array[String]) = {
    val conf = new GenomicsConf(args)
    val outPath = conf.outputPath()
    val sc = conf.newSparkContext(this.getClass.getName)
    val chr = "21"
    val region = Map((chr -> (1L, Examples.HumanChromosomes(chr))))
    val data = new ReadsRDD(sc, this.getClass.getName, conf.clientSecrets(),
      List(Examples.Google_Example_Readset),
      new ReadsPartitioner(region, TargetSizeSplits(100, 5, 1024, 16 * 1024 * 1024)))
    data.flatMap { rk =>
      val (_, read) = rk
      val cover = MutableMap[Int, Int]()
      for (i <- 0 until read.alignedBases.length) {
        cover(read.position + i) = 1
      }
      cover
    }
      .reduceByKey(_ + _)
      .sortByKey(true) // optional, obviously
      .saveAsTextFile(outPath + "/coverage_" + chr)
  }
}

/**
 * This example illustrates one way to work with multiple RDDs by aggregating and
 * comparing bases at the same position in different readsets. It uses synthetic
 * tumor-normal data from the ICGC-TCGA DREAM Contest (https://www.synapse.org/#!Synapse:syn312572).
 */
object SearchReadsExample4 {
  def main(args: Array[String]) = {
    val conf = new GenomicsConf(args)
    val outPath = conf.outputPath()
    val sc = conf.newSparkContext(this.getClass.getName)
    val chr = "1"
    val region = Map((chr -> (100000000L, 101000000L)))
    val minMappingQual = 30
    val minBaseQual = 30
    val minFreq = 0.25

    // Generates an RDD that maps genomic position to a base read frequencies.
    // Reads with a mapping quality less than minMappingQual are discarded
    // as are individual bases with base quality scores less than minBaseQual.
    //
    // For example, a snippet of the text dump of the RDD for chromosome 1
    // (synthetic set #3 normal) looks like:
    //    (100091811,Map(A -> 1.0))
    //    (100091812,Map(G -> 0.5428571428571428, A -> 0.45714285714285713))
    //    (100091813,Map(G -> 0.08333333333333333, A -> 0.3611111111111111, C -> 0.5555555555555556))
    //    (100091814,Map(G -> 0.30303030303030304, A -> 0.6060606060606061, C -> 0.09090909090909091))
    //    (100091815,Map(G -> 0.03125, A -> 0.6875, C -> 0.28125))
    //    (100091816,Map(A -> 0.375, C -> 0.03125, T -> 0.59375))
    //    (100091817,Map(A -> 0.90625, T -> 0.09375))
    //    (100091818,Map(A -> 0.125, T -> 0.875))
    //    (100091819,Map(A -> 0.28125, T -> 0.71875))
    //    (100091820,Map(G -> 0.6176470588235294, A -> 0.029411764705882353, T -> 0.35294117647058826))
    //    (100091821,Map(G -> 0.08823529411764706, C -> 0.6470588235294118, T -> 0.2647058823529412))
    //    (100091822,Map(G -> 0.23529411764705882, C -> 0.7352941176470589, T -> 0.029411764705882353))
    //    (100091823,Map(G -> 0.029411764705882353, A -> 0.6470588235294118, C -> 0.3235294117647059))
    //    (100091824,Map(A -> 0.08823529411764706, C -> 0.2647058823529412, T -> 0.6470588235294118))
    //    (100091825,Map(A -> 0.23529411764705882, C -> 0.6764705882352942, T -> 0.08823529411764706))
    //    (100091826,Map(A -> 0.6764705882352942, C -> 0.08823529411764706, T -> 0.23529411764705882))
    //    (100091827,Map(A -> 0.75, C -> 0.2222222222222222, T -> 0.027777777777777776))
    //    (100091828,Map(G -> 0.6571428571428571, A -> 0.3142857142857143, C -> 0.02857142857142857))
    //    (100091829,Map(G -> 0.11428571428571428, A -> 0.2571428571428571, T -> 0.6285714285714286))
    //    (100091830,Map(G -> 0.9142857142857143, A -> 0.02857142857142857, T -> 0.05714285714285714))
    //    (100091831,Map(G -> 0.1111111111111111, A -> 0.6666666666666666, T -> 0.2222222222222222))
    //    (100091832,Map(G -> 0.8888888888888888, A -> 0.08333333333333333, T -> 0.027777777777777776))
    //    (100091833,Map(G -> 0.11428571428571428, A -> 0.8571428571428571, T -> 0.02857142857142857))
    //    (100091834,Map(G -> 0.2, A -> 0.11428571428571428, T -> 0.6857142857142857))
    //    (100091835,Map(G -> 0.7142857142857143, A -> 0.2, T -> 0.08571428571428572))
    //    (100091836,Map(G -> 0.08333333333333333, A -> 0.7222222222222222, T -> 0.19444444444444445))
    def freqRDD(readsets: List[String], partitioner: ReadsPartitioner) = {
      new ReadsRDD(sc, this.getClass.getName, conf.clientSecrets(), readsets, partitioner)
        .filter(rk => rk._2.mappingQuality >= minMappingQual)
        .flatMap { rk =>
          val (_, read) = rk
          var bases = List[(Int, Char)]()
          for (i <- 0 until read.alignedBases.length) {
            if (i < read.baseQuality.length && read.baseQuality(i) >= minBaseQual) {
              bases ::= (read.position + i, read.alignedBases(i))
            }
          }
          bases
        }
        .groupByKey()
        .mapValues { v =>
          val vSeq = v.toSeq
          val total = vSeq.length.toDouble
          vSeq.groupBy(c => c)
            .map(p => (p._1, p._2.length))
            .map(p => (p._1, p._2.toDouble / total))
        }
        .groupByKey()
        .map(p => (p._1, p._2.head))
    }

    val part16M = new ReadsPartitioner(region, TargetSizeSplits(100, 30, 1024, 16 * 1024 * 1024))
    val normal = freqRDD(List(Examples.Google_DREAM_Set3_Normal), part16M)
    val tumor = freqRDD(List(Examples.Google_DREAM_Set3_Tumor), part16M)

    // Generate a new RDD that maps position to a pair of sorted base strings where
    // the first item is the normal and the second is the tumor.
    // Any base occurring with frequency less than minFreq is filtered out.
    // Example:
    //    (100091811,(A,A))
    //    (100091812,(AG,AG))
    //    (100091813,(AC,AC))
    //    (100091814,(AG,AG))
    //    (100091815,(AC,AC))
    //    (100091816,(AT,AT))
    //    (100091817,(A,A))
    //    (100091818,(T,T))
    //    (100091819,(AT,AT))
    //    (100091820,(GT,GT))
    //    (100091821,(CT,CT))
    //    (100091822,(C,CG))
    //    (100091823,(AC,AC))
    //    (100091824,(CT,CT))
    //    (100091825,(C,AC))
    //    (100091826,(A,AT))
    //    (100091827,(A,AC))
    //    (100091828,(AG,AG))
    //    (100091829,(AT,AT))
    //    (100091830,(G,G))
    //    (100091831,(A,AT))
    //    (100091832,(G,G))
    //    (100091833,(A,A))
    //    (100091834,(T,GT))
    //    (100091835,(G,AG))
    //    (100091836,(A,AT))
    val paired = normal.join(tumor).groupByKey()
      .map(p => (p._1, p._2.head))
      .map { p =>
        def f(m: Map[Char, Double]): String = {
          var s = ""
          m.foreach { kv =>
            if (kv._2 >= minFreq) { s += kv._1 }
          }
          s.sorted
        }
        (p._1, (f(p._2._1), f(p._2._2)))
      }

    // This RDD can be further filtered to eliminate any positions with matching bases.
    // Example:
    //    (100091822,(C,CG))
    //    (100091825,(C,AC))
    //    (100091826,(A,AT))
    //    (100091827,(A,AC))
    //    (100091831,(A,AT))
    //    (100091834,(T,GT))
    //    (100091835,(G,AG))
    //    (100091836,(A,AT))
    val diff = paired.filter(p => p._2._1 != p._2._2)
    diff.sortByKey().saveAsTextFile(outPath + "/diff_" + chr)
  }
}
