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

import org.apache.spark.SparkContext
import org.rogach.scallop.ScallopConf
import org.apache.spark.SparkConf

class GenomicsConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val sparkMaster = opt[String](default = Some("local[2]"))
  val sparkPath = opt[String](default = Some(""))
  val outputPath = opt[String](default = Some("."))
  val inputPath = opt[String]()
  val jarPath = opt[String]()
  val clientSecrets = opt[String](default = Some("client_secrets.json"))

  def newSparkContext(className: String, registrator:Option[String]=None) = {
    val jarPath = if (this.jarPath.isDefined)
      Seq(this.jarPath())
    else
      Seq[String]()
    val conf = new SparkConf()
      .setMaster(this.sparkMaster())
      .setAppName(className)
      .setSparkHome(this.sparkPath())
      .setJars(jarPath)
    if (registrator.isDefined) {
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.kryo.registrator", registrator.get)
    }
    new SparkContext(conf)
  }
}

class PcaConf(arguments: Seq[String]) extends GenomicsConf(arguments) {
  val nocomputeSimilarity = opt[Boolean](default=Option(false))
}