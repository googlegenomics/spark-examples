/*
Copyright 2015 Google Inc. All rights reserved.

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
import org.apache.spark.SparkContext
import com.google.api.services.genomics.model.SearchCallSetsRequest
import com.google.genomics.v1.{Variant => VariantModel}
import com.google.cloud.genomics.Authentication
import com.google.cloud.genomics.Client
import com.google.cloud.genomics.spark.examples.rdd.Variant
import com.google.cloud.genomics.spark.examples.rdd.VariantKey
import com.google.cloud.genomics.spark.examples.rdd.VariantsPartitioner
import com.google.cloud.genomics.spark.examples.rdd.VariantsRDD
import com.google.cloud.genomics.spark.examples.rdd.VariantsRddStats
import com.google.cloud.genomics.utils.Paginator

import org.apache.spark.rdd.RDD

class VariantsCommon(conf: PcaConf, sc: SparkContext) {

  private val auth = Authentication.getAccessToken(conf.clientSecrets.get)
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

  val data = {
    if (conf.inputPath.isDefined) {
      List(sc.objectFile[(VariantKey, Variant)](conf.inputPath()).map(_._2))
    } else {
      val variantSets = conf.variantSetId()
      println(s"Running PCA on ${variantSets.length} datasets.")
      conf.variantSetId().zipWithIndex.map {
        case (variantSetId, variantSetIndex) =>
        new VariantsRDD(sc, this.getClass.getName, auth,
          variantSetId,
          conf.getPartitioner(auth, variantSetId, variantSetIndex),
          stats=ioStats).map(_._2)
      }
    }
  }

  def reportIoStats = {
    this.ioStats match {
      case Some(stats) => println(stats.toString)
      case _ => {}
    }
  }

  // For now assume a single dataset when invoking from python.
  def getJavaData: RDD[VariantModel] = data.head.map(_.toJavaVariant)

  def createIoStats = if (conf.inputPath.isDefined) None
      else Option(new VariantsRddStats(sc))

}
