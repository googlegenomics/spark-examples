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
package com.google.cloud.genomics.spark.examples.rdd

import java.lang.{Double => JDouble}
import java.util.{List => JList}
import scala.collection.JavaConversions._
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import com.google.api.services.genomics.model.{Call => CallModel}
import com.google.api.services.genomics.model.SearchVariantsRequest
import com.google.api.services.genomics.model.{Variant => VariantModel}
import com.google.cloud.genomics.Auth
import com.google.cloud.genomics.Client
import com.google.cloud.genomics.utils.Paginator

import org.apache.spark.Accumulator

/**
 * A serializable version of the Variant.
 * Currently Java Client model objects are not serializable, see
 * https://code.google.com/p/google-api-java-client/issues/detail?id=390
 * for more information.
 */

case class Call(callsetId: String, callsetName: String, genotype: List[Integer],
    genotypeLikelihood: Option[List[JDouble]], phaseset: String,
    info: Map[String, JList[String]]) extends Serializable


case class Variant(contig: String, id: String, names: Option[List[String]],
    position: Long, end: Option[Long], referenceBases: String,
    alternateBases: Option[List[String]], info: Map[String, JList[String]],
    created: Long, variantSetId: String, calls: Option[Seq[Call]]) extends Serializable {

  def toJavaVariant() = {
    val variant = new VariantModel()
    .setReferenceName(this.contig)
    .setCreated(this.created)
    .setVariantSetId(this.variantSetId)
    .setId(this.id)
    .setInfo(this.info)
    .setStart(this.position)
    .setReferenceBases(this.referenceBases)

    if (this.alternateBases isDefined) variant.setAlternateBases(this.alternateBases.get)
    if (this.end isDefined) variant.setEnd(this.end.get.toLong)
    if (this.names isDefined) variant.setNames(this.names.get)
    if (this.calls isDefined) {
      val calls = this.calls.get.map
      { c =>
        val call = new CallModel()
        .setCallSetId(c.callsetId)
        .setCallSetName(c.callsetName)
        .setGenotype(c.genotype)
        .setInfo(c.info)
        .setPhaseset(c.phaseset)
        if (c.genotypeLikelihood isDefined) call.setGenotypeLikelihood(c.genotypeLikelihood.get)

        call
      }
      variant.setCalls(calls)
    }

    variant
  }
}


object VariantsBuilder {

  def build(r: VariantModel) = {
    val variantKey = VariantKey(r.getReferenceName, r.getStart)

    val calls = if (r.containsKey("calls"))
        Some(r.getCalls().map(
            c => Call(
                c.getCallSetId,
                c.getCallSetName,
                c.getGenotype.toList,
                if (c.containsKey("genotypeLikelihood"))
                  Some(c.getGenotypeLikelihood.toList)
                else
                  None,
                c.getPhaseset,
                if (c.containsKey("info"))
                  c.getInfo.toMap
                else
                  Map[String,java.util.List[String]]())))
      else
        None

    val variant = Variant(
        r.getReferenceName,
        r.getId,
        if (r.containsKey("names"))
          Some(r.getNames.toList)
        else
          None,
        r.getStart,
        if (r.containsKey("end"))
          Some(r.getEnd)
        else
          None,
        r.getReferenceBases,
        if (r.containsKey("alternateBases"))
          Some(r.getAlternateBases.toList)
        else
          None,
        if (r.containsKey("info"))
          r.getInfo.toMap
        else
          Map[String,java.util.List[String]](),
        if (r.containsKey("created"))
          r.getCreated
        else
          0L,
        r.getVariantSetId,
        calls)
    (variantKey, variant)
  }
}

class VariantsRddStats(sc: SparkContext) extends Serializable {
    val partitionsAccum = sc.accumulator(0, "Partitions count")
    val referenceBasesAccum = sc.accumulator(0L, "Reference bases count")
    val requestsAccum = sc.accumulator(0, "Request count")
    val variantsAccum = sc.accumulator(0, "Variant count")

    override def toString ={
      val buf = new StringBuilder
      buf ++= "Variants API stats:\n"
      buf ++= "-------------------------------\n"
      buf ++= s"# of partitions: ${this.partitionsAccum}\n"
      buf ++= s"# of API requests: ${this.requestsAccum}\n"
      buf ++= s"# of bases requested: ${this.referenceBasesAccum}\n"
      buf ++= s"# of variants read: ${this.variantsAccum}\n"
      buf.toString
    }
}

/**
 * A simple Spark RDD backed by Google Genomics VariantStore and
 * populated via the SearchVariants API call
 * (https://developers.google.com/genomics/v1beta/reference/variants/search).
 */
class VariantsRDD(sc: SparkContext,
    applicationName: String,
    auth: Auth,
    variantSetId: String,
    variantsPartitioner: VariantsPartitioner,
    stats:Option[VariantsRddStats] = None)
     extends RDD[(VariantKey, Variant)](sc, Nil) {

  override val partitioner = Some(variantsPartitioner)

  override def getPartitions: Array[Partition] = {
    variantsPartitioner.getPartitions(variantSetId)
  }

  override def compute(part: Partition, ctx: TaskContext):
    Iterator[(VariantKey, Variant)] = {
    val client = Client(auth)
    val reads = Paginator.Variants.create(client.genomics)
    val partition = part.asInstanceOf[VariantsPartition]
    val req = new SearchVariantsRequest()
      .setVariantSetIds(List(partition.variantSetId))
      .setReferenceName(partition.contig)
      .setStart(java.lang.Long.valueOf(partition.start))
      .setEnd(java.lang.Long.valueOf(partition.end))
    val iterator = reads.search(req).iterator().map(variant => {
      stats map { _.variantsAccum += 1 }
      VariantsBuilder.build(variant)
    })
    stats map { stat =>
        stat.partitionsAccum += 1
        stat.referenceBasesAccum += (partition.end - partition.start)
    }
    // Wrap the iterator to read the number of initialized requests once
    // it is fully traversed.
    new Iterator[(VariantKey, Variant)]() {

      def hasNext = {
        val hasNext = iterator.hasNext
        if (!hasNext) {
          stats map { _.requestsAccum += client.initializedRequestsCount }
        }
        hasNext
      }

      def next = iterator.next
    }
  }
}

/**
 * Defines a search range over a contig.
 */
case class VariantsPartition(override val index: Int,
                          val variantSetId: String,
                          val contig: String,
                          val start: Long,
                          val end: Long) extends Partition

/**
 * Indexes a variant to its partition.
 */
case class VariantKey(contig: String, position: Long)
