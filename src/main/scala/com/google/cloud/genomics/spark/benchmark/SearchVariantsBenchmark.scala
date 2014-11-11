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

import scala.collection.JavaConversions._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.google.cloud.genomics.Authentication
import com.google.cloud.genomics.spark.examples.GenomicsConf
import com.google.cloud.genomics.spark.examples.rdd.FixedBasesPerReference
import com.google.cloud.genomics.spark.examples.rdd.Variant
import com.google.cloud.genomics.spark.examples.rdd.VariantKey
import com.google.cloud.genomics.spark.examples.rdd.VariantsPartitioner
import com.google.cloud.genomics.spark.examples.rdd.VariantsRDD

object SearchVariantsBenchmark {
  def main(args: Array[String]) = {
    val conf = new GenomicsConf(args)
    val sc = conf.newSparkContext(this.getClass.getName)
    Logger.getLogger("org").setLevel(Level.WARN)
    val a = System.currentTimeMillis()
    val data = if (conf.inputPath.isDefined) {
      sc.objectFile[(VariantKey, Variant)](conf.inputPath())
    } else {
      val contigs = conf.getReferences
      val applicationName = this.getClass.getName
      val accessToken = Authentication.getAccessToken(applicationName,
        conf.clientSecrets())
      new VariantsRDD(sc,
        applicationName,
        accessToken,
        conf.variantSetId(),
        new VariantsPartitioner(contigs,
              FixedBasesPerReference(conf.basesPerPartition())),
        numThreads=conf.numThreads())
    }
    val count = data.count()
    val b = System.currentTimeMillis()
    println(s"Read ${count} variants in ${(b - a) / 1000} secs")
    println(s"@ ${count / ((b - a) / 1000).toFloat} variants per sec")
    sc.stop
  }
}

object StoreSearchVariants {
  def main(args: Array[String]) = {
    val conf = new GenomicsConf(args)
    val sc = conf.newSparkContext(this.getClass.getName)
    Logger.getLogger("org").setLevel(Level.WARN)
    val contigs = conf.getReferences
    val applicationName = this.getClass.getName
    val accessToken = Authentication.getAccessToken(applicationName,
      conf.clientSecrets())
    val data = new VariantsRDD(sc,
        applicationName,
        accessToken,
        conf.variantSetId(),
        new VariantsPartitioner(contigs,
              FixedBasesPerReference(conf.basesPerPartition())))
      .saveAsObjectFile(conf.outputPath())
    sc.stop
  }
}