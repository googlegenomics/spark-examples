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

import com.google.cloud.genomics.Client
import com.google.cloud.genomics.utils.OfflineAuth
import com.google.cloud.genomics.utils.ShardBoundary
import com.google.cloud.genomics.utils.ShardUtils
import com.google.cloud.genomics.utils.ShardUtils.SexChromosomeFilter
import com.google.cloud.genomics.utils.grpc.VariantStreamIterator
import com.google.genomics.v1.StreamVariantsRequest
import com.google.genomics.v1.{Variant => VariantModel}
import com.google.genomics.v1.{VariantCall => CallModel}
import com.google.protobuf.ByteString
import com.google.protobuf.ListValue
import com.google.protobuf.Value

import org.apache.spark.Accumulator
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

/**
 * A serializable version of the Variant.
 * https://github.com/googlegenomics/spark-examples/issues/84
 */
case class Call(callsetId: String, callsetName: String, genotype: List[Integer],
    genotypeLikelihood: Option[List[JDouble]], phaseset: String,
    info: Map[String, List[String]]) extends Serializable


case class Variant(contig: String, id: String, names: Option[List[String]],
    start: Long, end: Long, referenceBases: String,
    alternateBases: Option[List[String]], info: Map[String, List[String]],
    created: Long, variantSetId: String, calls: Option[Seq[Call]]) extends Serializable {

  def toListValue(values: List[String]) = {
    val listValue = ListValue.newBuilder()
    listValue.addAllValues(
      values.map(Value.newBuilder().setStringValue(_).build))
    listValue.build
  }

  def toJavaVariant() = {
    val variant = VariantModel.newBuilder()
    .setReferenceName(this.contig)
    .setCreated(this.created)
    .setVariantSetId(this.variantSetId)
    .setId(this.id)
    .setStart(this.start)
    .setEnd(this.end)
    .setReferenceBases(this.referenceBases)

    variant.putAllInfo(this.info.mapValues(toListValue))

    if (this.alternateBases isDefined)
      variant.addAllAlternateBases(this.alternateBases.get)
    if (this.names isDefined)
      variant.addAllNames(this.names.get)
    if (this.calls isDefined) {
      val calls = this.calls.get.map
      { c =>
        val call = CallModel.newBuilder()
        .setCallSetId(c.callsetId)
        .setCallSetName(c.callsetName)

        call.addAllGenotype(c.genotype)
        call.setPhaseset(c.phaseset)

        call.putAllInfo(c.info.mapValues(toListValue))
        if (c.genotypeLikelihood isDefined)
          call.addAllGenotypeLikelihood(c.genotypeLikelihood.get)
        call.build
      }
      variant.addAllCalls(calls)
    }
    variant.build
  }
}


object VariantsBuilder {

  val refNameRegex = """([a-z]*)?([0-9]*)""".r

  def normalize(referenceName: String) = {
    referenceName match {
      case refNameRegex(ref, id) => Some(id)
      case _ => None
    }
  }

  def toStringList(values: ListValue) =
    values.getValuesList.map(_.getStringValue()).toList

  def build(r: VariantModel) = {
    val variantKey = VariantKey(r.getReferenceName, r.getStart)
    val calls = if (r.getCallsCount > 0)
        Some(r.getCallsList.map(
            c => Call(
                c.getCallSetId,
                c.getCallSetName,
                c.getGenotypeList.toList,
                if (c.getGenotypeLikelihoodCount > 0)
                  Some(c.getGenotypeLikelihoodList.toList)
                else
                  None,
                c.getPhaseset,
                c.getInfo.mapValues(toStringList).toMap)))
      else
        None

    val referenceName = normalize(r.getReferenceName)

    if (referenceName.isEmpty) {
      None;
    } else {
      val variant = Variant(
          referenceName.get,
          r.getId,
          if (r.getNamesCount() > 0)
            Some(r.getNamesList.toList)
          else
            None,
          r.getStart,
          r.getEnd,
          r.getReferenceBases,
          if (r.getAlternateBasesCount() > 0)
            Some(r.getAlternateBasesList.toList)
          else
            None,
          r.getInfo.mapValues(toStringList).toMap,
          r.getCreated,
          r.getVariantSetId,
          calls)
      Some((variantKey, variant))
    }
  }
}

class VariantsRddStats(sc: SparkContext) extends Serializable {
    val partitionsAccum = sc.accumulator(0, "Partitions count")
    val referenceBasesAccum = sc.accumulator(0L, "Reference bases count")
    val requestsAccum = sc.accumulator(0, "Request count")
    val unsuccessfulResponsesAccum = sc.accumulator(0, "Unsuccessful count")
    val ioExceptionsAccum = sc.accumulator(0, "IO exceptions count")
    val variantsAccum = sc.accumulator(0, "Variant count")

    override def toString ={
      val buf = new StringBuilder
      buf ++= "Variants API stats:\n"
      buf ++= "-------------------------------\n"
      buf ++= s"# of partitions: ${this.partitionsAccum}\n"
      buf ++= s"# of bases requested: ${this.referenceBasesAccum}\n"
      buf ++= s"# of variants read: ${this.variantsAccum}\n"
      buf ++= s"# of API requests: ${this.requestsAccum}\n"
      buf ++= s"# of unsuccessful responses: ${this.unsuccessfulResponsesAccum}\n"
      buf ++= s"# of IO exceptions: ${this.ioExceptionsAccum}\n"
      buf.toString
    }
}

/**
 * A simple Spark RDD backed by Google Genomics VariantStore and
 * populated via the StreamVariants API call
 * (https://cloud.google.com/genomics/reference/rpc/google.genomics.v1#streamingvariantservice).
 */
class VariantsRDD(sc: SparkContext,
    applicationName: String,
    auth: OfflineAuth,
    variantSetId: String,
    variantsPartitioner: VariantsPartitioner,
    stats:Option[VariantsRddStats] = None)
     extends RDD[(VariantKey, Variant)](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    variantsPartitioner.getPartitions(variantSetId)
  }

  def reportStats(client: Client) = stats map { stat =>
    stat.requestsAccum += client.initializedRequestsCount
    stat.unsuccessfulResponsesAccum += client.unsuccessfulResponsesCount
    stat.ioExceptionsAccum += client.ioExceptionsCount
  }

  override def compute(part: Partition, ctx: TaskContext):
    Iterator[(VariantKey, Variant)] = {
    val client = Client(auth)
    val partition = part.asInstanceOf[VariantsPartition]
    val request = partition.getVariantsRequest
    val responses = VariantStreamIterator.enforceShardBoundary(
        auth, request, ShardBoundary.Requirement.STRICT, null);
    val iterator = responses.flatMap(variantResponse => {
      variantResponse.getVariantsList().map(variant => {
          stats map { _.variantsAccum += 1 }
          VariantsBuilder.build(variant)
        })
      }).filter(_.isDefined).map(_.get)
    stats map { stat =>
        stat.partitionsAccum += 1
        stat.referenceBasesAccum += (partition.range)
    }
    // Wrap the iterator to read the number of initialized requests once
    // it is fully traversed.
    new Iterator[(VariantKey, Variant)]() {
      def hasNext = {
        val hasNext = iterator.hasNext
        if (!hasNext) {
          reportStats(client)
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
case class VariantsPartition(
    override val index: Int, serializedRequest: ByteString)
    extends Partition {

  def getVariantsRequest = StreamVariantsRequest.parseFrom(serializedRequest)

  def range = {
    val request = getVariantsRequest
    request.getEnd() - request.getStart()
  }
}


/**
 * Indexes a variant to its partition.
 */
case class VariantKey(contig: String, position: Long)

trait VariantsPartitioner extends Serializable {
  def getPartitions(variantSetId: String): Array[Partition]
}


/**
 * Describes partitions for a set of contigs and their ranges.
 */
class AllReferencesVariantsPartitioner(numberOfBasesPerShard: Long,
    auth: OfflineAuth) extends VariantsPartitioner {

  // Generates all partitions for all mapped variants in the contig space.
  def getPartitions(variantSetId: String): Array[Partition] = {
    println(s"Variantset: ${variantSetId}; All refs, exclude XY")
    ShardUtils.getVariantRequests(
        variantSetId, SexChromosomeFilter.EXCLUDE_XY,
        numberOfBasesPerShard, auth).zipWithIndex.map {
      case(request, index) => VariantsPartition(index, request.toByteString())
    }.toArray
  }
}

class ReferencesVariantsPartitioner(references: String,
    numberOfBasesPerShard: Long) extends VariantsPartitioner {
  // Generates all partitions for all mapped variants in the contig space.
  def getPartitions(variantSetId: String): Array[Partition] = {
    println(s"Variantset: ${variantSetId}; Refs: ${references}")
    ShardUtils.getVariantRequests(
        variantSetId, references, numberOfBasesPerShard).zipWithIndex.map {
      case(request, index) => VariantsPartition(index, request.toByteString)
    }.toArray
  }
}
