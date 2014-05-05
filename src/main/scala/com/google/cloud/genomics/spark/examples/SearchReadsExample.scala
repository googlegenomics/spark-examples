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
  final val secretsFile = "PATH_TO/client_secrets.json"
  final val sparkPath = "SPARK_HOME_DIR"
  final val sparkMaster = "local"
  final val outputPath = "."
  final val jarPath = "PATH_TO/googlegenomics-spark-examples-assembly-1.0.jar"

  final val Google_1KG_HG00096_Readset = "CJDmkYn8ChCcnc7i4KaWqmQ"
  // From http://google-genomics.readthedocs.org/en/latest/constants.html
  final val Google_Example_Readset = "CJDmkYn8ChCh4IH4hOf4gacB"

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
    val sc = new SparkContext(Examples.sparkMaster, this.getClass.getName, Examples.sparkPath, List(Examples.jarPath))
    Logger.getLogger("org").setLevel(Level.WARN)
    val region = Map(("11" -> (Examples.Cilantro - 1000, Examples.Cilantro + 1000)))
    val data = new ReadsRDD(sc, this.getClass.getName, Examples.secretsFile,
      List(Examples.Google_Example_Readset),
      new ReadsPartitioner(region, FixedSplits(1)))
      .filter { rk =>
        val (_, read) = rk
        read.position <= Examples.Cilantro && read.position + read.alignedBases.length >= Examples.Cilantro
      }.cache()
    var first = data.collect.foldLeft(999999999L) { (a, b) =>
      val (_, read) = b
      val p = read.position
      if (p < a) { p.toLong } else { a }
    }
    println(List.fill((Examples.Cilantro - first).toInt)(" ").foldLeft("")(_ + _) + "v")
    data.foreach { rk =>
      val (_, read) = rk
      val i = (Examples.Cilantro - read.position).toInt
      val bases = read.alignedBases.splitAt(i + 1)
      val q = "%02d".format(read.baseQuality(i) - 33)
      println(List.fill((read.position - first).toInt)(" ").foldLeft("")(_ + _) + bases._1 + "(" + q + ") " + bases._2)
    }
    println(List.fill((Examples.Cilantro - first).toInt)(" ").foldLeft("")(_ + _) + "^")
  }
}

/**
 * This example computes the average read coverage for a genomic range.
 */
object SearchReadsExample2 {
  def main(args: Array[String]) = {
    val sc = new SparkContext(Examples.sparkMaster, this.getClass.getName, Examples.sparkPath, List(Examples.jarPath))
    val chr = "21"
    val len = Examples.HumanChromosomes(chr)
    val region = Map((chr -> (1L, len)))
    val data = new ReadsRDD(sc, this.getClass.getName, Examples.secretsFile,
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
    val sc = new SparkContext(Examples.sparkMaster, this.getClass.getName, Examples.sparkPath, List(Examples.jarPath))
    val chr = "21"
    val region = Map((chr -> (1L, Examples.HumanChromosomes(chr))))
    val data = new ReadsRDD(sc, this.getClass.getName, Examples.secretsFile,
      List(Examples.Google_Example_Readset),
      new ReadsPartitioner(region, TargetSizeSplits(100, 5, 1024, 16 * 1024 * 1024)))
    data.flatMap { rk =>
      val (_, read) = rk
      val cover = MutableMap[Int, Int]()
      for (i <- 0 until read.alignedBases.length) {
        val n = cover.getOrElse(i, 0)
        cover(read.position + i) = n + 1
      }
      cover
    }
      .reduceByKey(_ + _)
      .sortByKey(true) // optional, obviously
      .saveAsTextFile(Examples.outputPath + "/coverage_" + chr)
  }
}
