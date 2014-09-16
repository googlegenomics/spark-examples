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
package com.google.cloud.genomics.spark.benchmark

import collection.JavaConversions._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import com.google.cloud.genomics.spark.examples.rdd.FixedContigSplits
import com.google.cloud.genomics.spark.examples.rdd.VariantCalls
import com.google.cloud.genomics.spark.examples.rdd.VariantCallsRDD
import com.google.cloud.genomics.spark.examples.rdd.VariantKey
import com.google.cloud.genomics.spark.examples.rdd.VariantsPartitioner
import com.google.cloud.genomics.spark.examples.GenomicsConf
import com.google.cloud.genomics.spark.examples.VariantDatasets
import com.google.cloud.genomics.spark.examples.rdd.VariantsRDD
import com.google.cloud.genomics.spark.examples.rdd.Variant

object SearchVariantsBenchmark {
  def main(args: Array[String]) = {
    val conf = new GenomicsConf(args)
    val sc = conf.newSparkContext(this.getClass.getName)
    Logger.getLogger("org").setLevel(Level.WARN)
    val a = System.currentTimeMillis()
    val data = if (conf.inputPath.isDefined) {
      sc.objectFile[(VariantKey, Variant)](conf.inputPath())
    } else {
      val contigs = conf.getContigs
      new VariantCallsRDD(sc,
        this.getClass.getName,
        conf.clientSecrets(),
        VariantDatasets.Google_1000_genomes_phase_1,
        new VariantsPartitioner(contigs, 
            FixedContigSplits(conf.partitionsPerContig())),
        conf.maxResults())
    }
    val count = data.count()
    val b = System.currentTimeMillis()
    println(s"Read ${count} variants in ${(b - a) / 1000} secs")
    println(s"@ ${count / ((b - a) / 1000).toFloat} variants per sec")
  }
}

object StoreSearchVariants {
  def main(args: Array[String]) = {
    val conf = new GenomicsConf(args)
    val sc = conf.newSparkContext(this.getClass.getName)
    Logger.getLogger("org").setLevel(Level.WARN)
    val contigs = conf.getContigs
    val data = new VariantsRDD(sc,
      this.getClass.getName,
      conf.clientSecrets(),
      VariantDatasets.Google_1000_genomes_phase_1,
      new VariantsPartitioner(contigs, 
          FixedContigSplits(conf.partitionsPerContig())))
      .saveAsObjectFile(conf.outputPath())
  }
}