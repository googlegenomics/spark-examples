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

import java.util.{List => JList}

import com.google.cloud.genomics.Client
import com.google.cloud.genomics.utils.OfflineAuth
import com.google.cloud.genomics.utils.ShardBoundary
import com.google.cloud.genomics.utils.ShardUtils
import com.google.cloud.genomics.utils.ShardUtils.SexChromosomeFilter
import com.google.cloud.genomics.utils.grpc.ReadStreamIterator
import com.google.genomics.v1.StreamReadsRequest
import com.google.genomics.v1.{Read => ReadModel}
import com.google.protobuf.ByteString
import com.google.protobuf.ListValue
import com.google.protobuf.Value

import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * A serializable version of the Read.
 * https://github.com/googlegenomics/spark-examples/issues/84
 */
case class Read(alignedQuality: JList[Integer], cigar: String,
    id: String, mappingQuality: Int, matePosition: Option[Long],
    mateReferenceName: Option[String], fragmentName: String, alignedSequence: String,
    position: Long, readGroupSetId: String, referenceName: String,
    info: Map[String, JList[String]], fragmentLength: Int) extends Serializable

object ReadBuilder {

  val CIGAR_MATCH = Map(
      "ALIGNMENT_MATCH" -> "M",
      "CLIP_HARD" -> "H",
      "CLIP_SOFT" -> "S",
      "DELETE" -> "D",
      "INSERT" -> "I",
      "PAD" -> "P",
      "SEQUENCE_MATCH" -> "=",
      "SEQUENCE_MISMATCH" -> "X",
      "SKIP" -> "N")

  def fromJavaRead(r: ReadModel) = {
    val readKey = ReadKey(r.getAlignment.getPosition.getReferenceName,
        r.getAlignment.getPosition.getPosition)

   val cigar =  r.getAlignment.getCigarList.map(cigarUnit =>
     cigarUnit.getOperationLength() +
     CIGAR_MATCH(cigarUnit.getOperation().name())).mkString("")

    val read = Read(
        r.getAlignedQualityList,
        cigar,
        r.getId,
        r.getAlignment.getMappingQuality,
        Some(r.getNextMatePosition.getPosition),
        Some(r.getNextMatePosition.getReferenceName),
        r.getFragmentName,
        r.getAlignedSequence,
        r.getAlignment.getPosition.getPosition,
        r.getReadGroupSetId,
        r.getAlignment.getPosition.getReferenceName,
        r.getInfo.mapValues(_.getValuesList.map(_.getStringValue()).toList.asJava).toMap,
        r.getFragmentLength)
        (readKey, read)
  }
}

/**
 * A simple Spark RDD backed by Google Genomics Readstore and populated
 * via the StreamReads API call (https://cloud.google.com/genomics/reference/rpc/google.genomics.v1#streamingreadservice).
 */
class ReadsRDD(sc: SparkContext,
               applicationName: String,
               auth: OfflineAuth,
               readGroupSetId: String,
               readsPartitioner: ReadsPartitioner)
               extends RDD[(ReadKey, Read)](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    readsPartitioner.getPartitions(readGroupSetId)
  }

  override def compute(part: Partition, ctx: TaskContext):
    Iterator[(ReadKey, Read)] = {
    val client = Client(auth).genomics
    val partition = part.asInstanceOf[ReadsPartition]
    val request = partition.getReadsRequest
    val responses = ReadStreamIterator.enforceShardBoundary(
        auth, request, ShardBoundary.Requirement.STRICT, null);
    val iterator = responses.flatMap(readResponse => {
      readResponse.getAlignmentsList().map(read => {
          ReadBuilder.fromJavaRead(read)
        })
      })
    // Wrap the iterator to read the number of initialized requests once
    // it is fully traversed.
    new Iterator[(ReadKey, Read)]() {
      def hasNext = {
        val hasNext = iterator.hasNext
        hasNext
      }

      def next = iterator.next
    }
  }
}

/**
 * Defines a search range over a contig.
 */
case class ReadsPartition(
    override val index: Int, serializedRequest: ByteString)
    extends Partition {

  def getReadsRequest = StreamReadsRequest.parseFrom(serializedRequest)

  def range = {
    val request = getReadsRequest
    request.getEnd() - request.getStart()
  }
}


/**
 * Indexes a Read to its partition.
 */
case class ReadKey(contig: String, position: Long)

trait ReadsPartitioner extends Serializable {
  def getPartitions(readGroupSetId: String): Array[Partition]
}


/**
 * Describes partitions for a set of contigs and their ranges.
 */
class AllReferencesReadsPartitioner(numberOfBasesPerShard: Long,
    auth: OfflineAuth) extends ReadsPartitioner {

  // Generates all partitions for all mapped Reads in the contig space.
  def getPartitions(readGroupSetId: String): Array[Partition] = {
    println(s"ReadGroupSetId: ${readGroupSetId}; All refs, exclude XY")
    ShardUtils.getReadRequests(
        readGroupSetId, SexChromosomeFilter.INCLUDE_XY,
        numberOfBasesPerShard, auth).zipWithIndex.map {
      case(request, index) => ReadsPartition(index, request.toByteString())
    }.toArray
  }
}

class ReferencesReadsPartitioner(references: String,
    numberOfBasesPerShard: Long) extends ReadsPartitioner {
  // Generates all partitions for all mapped Reads in the contig space.
  def getPartitions(readGroupSetId: String): Array[Partition] = {
    println(s"ReadGroupSetId: ${readGroupSetId}; Refs: ${references}")
    ShardUtils.getReadRequests(
        List(readGroupSetId), references, numberOfBasesPerShard).zipWithIndex.map {
      case(request, index) => ReadsPartition(index, request.toByteString)
    }.toArray
  }
}
